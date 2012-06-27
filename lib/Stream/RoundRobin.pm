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

Data size in bytes.

RoundRobin storage always occupy this amount of space on disk (more or less).

Default is 1GB.

=cut
has 'data_size' => (
    is => 'ro',
    isa => 'Int',
    default => 1024 * 1024 * 1024, # 1GB
);

has '_buffer_ref' => (
    is => 'rw',
    lazy => 1,
    clearer => '_clear_buffer',
    default => sub { my $x; \$x },
);

has '_buffer_lines' => (
    is => 'rw',
    default => 0,
);

after _clear_buffer => sub {
    my $self = shift;
    $self->_buffer_lines(0);
};

=item B< buffer_size >

Buffer size in bytes.

Commit is forced anytime C< buffer_size > of buffered data is reached. Set to 0 to disable the feature.

Default is 5% of C< data_size > or 10MB whichever is less.

=cut

has 'buffer_size' => (
    is => 'ro',
    isa => 'Int',
    lazy => 1, # depends on $self->data_size
    default => sub {
        my $self = shift;
        my $buffer_size = int($self->data_size * 0.05) + 1;
        $buffer_size = 10 * 1024 * 1024 if $buffer_size > 10 * 1024 * 1024;
        return $buffer_size;
    },
);

sub _mkdir_unless_exists {
    my $self = shift;
    my ($dir) = @_;
    return if -d $dir;
    {
        no autodie;
        unless (mkdir $dir) {
            die "mkdir failed: $!" unless -d $dir;
        }
    }
}

sub _init_data {
    my $self = shift;

    my $dir = $self->dir;
    return if -e "$dir/data";
    my $lock = $self->_lock;
    return if -e "$dir/data";

    open my $fh, '>', "$dir/data.new";
    my $count = $self->data_size;

    while ($count >= 1024) {
        print {$fh} "\n" x 1024;
        $count -= 1024;
    }
    print {$fh} "\n" while $count-- > 0;
    close $fh;
    rename "$dir/data.new" => "$dir/data";
}

=back

=head1 METHODS

Note that C<Stream::RoundRobin> implements all methods from L<Stream::Storage> and L<Stream::Moose::Storage::ClientList>.

=over

=cut
sub BUILD {
    my $self = shift;
    my $dir = $self->dir;

    $self->_mkdir_unless_exists($dir);
    $self->_mkdir_unless_exists("$dir/clients");
    $self->_init_data;

    if (int(-s "$dir/data") != $self->data_size) {
        die "Invalid data_size ".$self->data_size.", file has size ".(-s "$dir/data");
    }
}

sub _lock {
    my $self = shift;
    return lockf($self->dir.'/lock', { blocking => 1 });
}

sub write_chunk {
    my $self = shift;
    my ($chunk) = @_;

    ${$self->_buffer_ref} .= join "", @$chunk;
    $self->_buffer_lines($self->_buffer_lines + @$chunk);
    if ($self->buffer_size and length ${$self->_buffer_ref} > $self->buffer_size) {
        $self->commit;
    }
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

    my $buffer_ref = $self->_buffer_ref;
    return unless $$buffer_ref;

    # if there will be an exception (because of cross_check, for example), storage will still stay usable
    # (but please don't rely on what I say here, it's mostly for tests)
    $self->_clear_buffer();

    my $lock = $self->_lock;
    open my $fh, '+<', $self->dir.'/data';

    my $old_position = $self->position;
    sysseek($fh, $old_position, SEEK_SET);

    my $write = sub {
        # substr passed by reference
        my $left = length $_[0];
        my $offset = 0;
        while ($left) {
            my $bytes = $fh->syswrite($_[0], $left, $offset);
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

    # let's check that new data will fit in the storage
    my $buffer_length = length $$buffer_ref;
    if ($buffer_length >= $self->data_size) {
        confess "buffer is too large ($buffer_length bytes, ".$self->_buffer_lines." lines)"
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

    my $pos = sysseek($fh, 0, SEEK_CUR); # TODO - calculate from previous writes to avoid syscall?
    if ($buffer_length + $pos < $self->data_size) {
        $write->($$buffer_ref);
    }
    else {
        $write->(substr($$buffer_ref, 0, $self->data_size - $pos));
        sysseek($fh, 0, SEEK_SET);
        $write->(substr($$buffer_ref, $self->data_size - $pos, $buffer_length + $pos - $self->data_size));
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

sub has_client {
    my $self = shift;
    my ($name) = @_;
    return -d $self->dir."/clients/$name";
}

sub register_client {
    my $self = shift;
    my ($name) = pos_validated_list(\@_, { isa => ClientName });
    $self->check_read_only();

    # check if already registered
    # we check first without locking the whole storage because auto-registering may be enabled, and lock can be undesirable
    # (or maybe this is a premature optimization)
    return if $self->has_client($name);

    my $lock = $self->_lock; # in case initial 'data' generation happens right now

    INFO "Registering $name at ".$self->dir;
    $self->_mkdir_unless_exists($self->dir."/clients/$name");
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

=item B< in($client) >

=item B< in($client, $options) >

Get input stream by a client name.

Input streams are wrapped in L<Stream::In::DuskBuffer> for convinience. You can pass C<< buffer => 0 >> as a second parameter to avoid this and get a raw L<Stream::In::DiskBuffer> object.
(This can be useful, for example, if you process data in a single process and need more performance.)

=cut
sub in {
    my $self = shift;
    my ($client, $other) = pos_validated_list(\@_,
        { isa => ClientName },
        { isa => 'HashRef', default => {} },
    );
    my ($use_buffer) = validated_list([ %$other ],
        buffer => { isa => 'Bool', default => 1 },
        MX_PARAMS_VALIDATE_CACHE_KEY => 'roundrobin-in-additional-options-validation', # sorry; see MooseX::Params::Validate docs
    );

    my $in = Stream::RoundRobin::In->new(storage => $self, name => $client);

    if ($use_buffer) {
        return Stream::In::DiskBuffer->new(
            $in  => $self->dir."/clients/$client/buffer",
            {
                read_only => $self->read_only,
                format => 'plain',
                read_lock => 0, # RoundRobin::In locks itself
            }
        );
    }

    return $in;
}

sub description {
    my $self = shift;

    return
        "dir: ".$self->dir."\n"
        ."size: ".Format::Human::Bytes->new->base2($self->data_size);
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
