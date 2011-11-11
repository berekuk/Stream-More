package Stream::RoundRobin;

# ABSTRACT: local RoundRobin-style string storage

use namespace::autoclean;

use Moose;
use MooseX::Params::Validate;
use Moose::Util::TypeConstraints;

use autodie qw(:all);
use IPC::System::Simple;
use Fcntl qw( SEEK_SET SEEK_CUR SEEK_END );
use IO::Handle;
use List::Util qw(sum);
use Format::Human::Bytes;

use Yandex::Lockf;
use Yandex::Logger;
use Yandex::Persistent;

use Stream::RoundRobin::Types qw(:all);
use Stream::RoundRobin::Util qw( check_cross );;
use Stream::RoundRobin::In;

use Stream::In::DiskBuffer;

sub isa {
    return 1 if $_[1] eq __PACKAGE__;
    $_[0]->next::method if $_[0]->next::can;
} # ugly hack

=head1 ATTRIBUTES

=over

=item B< dir >

Dir to store data. Required.

=cut
has 'dir' => (
    is => 'ro',
    isa => 'Str',
    required => 1,
);

=item B< data_size >

Data size.

RoundRobin storage always occupy this amount of space on disk (more or less).

Default is 1GB.

=cut
has 'data_size' => (
    is => 'ro',
    isa => 'Int',
    default => 1024 * 1024 * 1024, # 1GB
);

has '_buffer' => (
    is => 'rw',
    isa => 'ArrayRef[Str]',
    default => sub { [] },
);

=back

=head1 METHODS

=over

=cut
sub BUILD {
    my $self = shift;
    my $dir = $self->dir;
    unless (-d $dir) {
        mkdir $dir;
    }
    my $lock = $self->_lock;
    unless (-d "$dir/clients") {
        mkdir "$dir/clients";
    }
    unless (-e "$dir/data") {
        open my $fh, '>', "$dir/data";
        my $count = $self->data_size;

        while ($count >= 1024) {
            print {$fh} "\n" x 1024;
            $count -= 1024;
        }
        print {$fh} "\n" while $count-- > 0;
        close $fh;
    }
    if (int(-s "$dir/data") != $self->data_size) {
        die "Invalid data_size ".$self->data_size.", file has size ".(-s "$dir/data");
    }
}

sub _lock {
    my $self = shift;
    return lockf($self->dir.'/lock', { blocking => 1 });
}

sub write_chunk {
    my ($self, $chunk) = (shift, shift);
    push @{ $self->_buffer }, @$chunk;
}

=item B< position >

Get current write position - offset in bytes in data file.

=cut
sub position {
    my $self = shift;
    my $state = Yandex::Persistent->new($self->dir.'/state', { read_only => 1 });
    return $state->{position} || 0;
}

=item B< set_position($position) >

Set current write position. (Don't call this method outside of C<Stream::RoundRobin> code).

=cut
sub set_position {
    my $self = shift;
    my $position = shift;
    $self->check_read_only;
    my $state = Yandex::Persistent->new($self->dir.'/state', { auto_commit => 0 });
    $state->{position} = $position;
    $state->commit;
}

sub commit {
    my $self = shift;

    my $buffer = $self->_buffer;
    return unless @$buffer;

    # if there will be an exception (because of cross_check, for example), storage will still stay usable
    # (but please don't rely on what I say here, it's mostly for tests)
    $self->_buffer([]);

    my $lock = $self->_lock;
    open my $fh, '+<', $self->dir.'/data';

    my $old_position = $self->position;
    sysseek($fh, $old_position, SEEK_SET);

    my $write = sub {
        my $line = shift;
        my $left = length $line;
        my $offset = 0;
        while ($left) {
            my $bytes = $fh->syswrite($line, $left, $offset);
            if (not defined $bytes) {
                die "syswrite failed: $!";
            } elsif ($bytes == 0) {
                die "syswrite no progress";
            } else {
                $offset += $bytes;
                $left -= $bytes;
            }
        }
    };

    {
        # let's check that new data will fit in the storage
        my $buffer_length = sum(map { length $_ } @$buffer);
        if ($buffer_length >= $self->data_size) {
            confess "buffer is too large ($buffer_length bytes, ".scalar @$buffer." lines)"
        }
        my $left = int(sysseek($fh, 0, SEEK_CUR));
        my $right = $left + $buffer_length;
        $right -= $self->data_size if $right > $self->data_size;
        check_cross(
            left => $left,
            right => $right,
            size => $self->data_size,
            positions => {
                "storage's own" => $old_position,
                map { $_ => $self->in($_)->in->position } $self->client_names,
            },
        );
    }

    for my $line (@$buffer) {
        my $pos = sysseek($fh, 0, SEEK_CUR); # TODO - calculate from previous writes to avoid syscall?
        if (length($line) + $pos < $self->data_size) {
            $write->($line);
        }
        else {
            $write->(substr($line, 0, $self->data_size - $pos));
            sysseek($fh, 0, SEEK_SET);
            $write->(substr($line, $self->data_size - $pos, length($line) + $pos - $self->data_size));
        }
    }
    my $new_position = sysseek($fh, 0, SEEK_CUR);
    close $fh;
    # TODO - fsync?
    $self->set_position($new_position);
}

sub client_names {
    my $self = shift;
    my @client_names = map { File::Spec->abs2rel( $_, $self->dir."/clients" ) } grep { -d $_ } glob $self->dir."/clients/*";
    return @client_names;
}

sub register_client {
    my $self = shift;
    my ($name) = pos_validated_list(\@_, { isa => ClientName });
    $self->check_read_only();

    if ($self->has_client($name)) {
        return; # already registered
    }

    my $lock = $self->_lock; # in case initial 'data' generation happens right now

    INFO "Registering $name at ".$self->dir;
    mkdir($self->dir."/clients/$name");
    $self->in($name)->in->commit; # create client state
}

sub unregister_client {
    my $self = shift;
    my ($client) = pos_validated_list(\@_, { isa => ClientName });
    $self->check_read_only();
    unless ($self->has_client($client)) {
        WARN "No such client '$client', can't unregister";
        return;
    }
    INFO "Unregistering $client at ".$self->dir;
    system('rm', '-rf', '--', $self->dir."/clients/$client");
}

sub in {
    my $self = shift;
    my ($client) = pos_validated_list(\@_, { isa => ClientName });
    return Stream::In::DiskBuffer->new(
        Stream::RoundRobin::In->new(storage => $self, name => $client) => $self->dir."/clients/$client/buffer",
        {
            read_only => $self->read_only,
            format => 'plain',
        }
    );
}

sub description {
    my $self = shift;

    return
        "dir: ".$self->dir."\n"
        ."size: "..Format::Human::Bytes->new->base2($self->data_size)
    ;
}

=back

=head1 ROLES

See the sources for the list of roles this module implements.

=cut
with
    'Stream::Moose::Storage',
    'Stream::Moose::Storage::ClientList', # register_client/unregister_client/client_names methods
    'Stream::Moose::Storage::AutoregisterClients',
    'Stream::Moose::Out::Chunked', # provides 'write' implementation
    'Stream::Moose::Out::ReadOnly', # provides check_read_only and calls it before write/write_chunk/commit
    'Stream::Moose::Role::AutoOwned' => { file_method => 'dir' }, # provides owner/owner_uid
    'Stream::Moose::Role::Description',
;

__PACKAGE__->meta->make_immutable;
