package Stream::RoundRobin;

use namespace::autoclean;

use Moose;
use MooseX::Params::Validate;

use autodie;
use Yandex::Lockf;
use Yandex::Persistent;
use Fcntl qw(SEEK_SET SEEK_CUR SEEK_END);
use IO::Handle;
use List::Util qw(sum);

# FIXME - replace these with moose roles
use parent qw(
    Stream::Storage::Role::ClientList
);

sub isa {
    return 1 if $_[1] eq __PACKAGE__;
    $_[0]->next::method if $_[0]->next::can;
} # ugly hack

has 'dir' => (
    is => 'ro',
    isa => 'Str',
    required => 1,
);

has 'data_size' => (
    is => 'ro',
    isa => 'Int',
    default => 1024 * 1024 * 1024, # 1GB
);

sub BUILD {
    my $self = shift;
    my $dir = $self->dir;
    unless (-d $dir) {
        mkdir $dir;
    }
    unless (-d "$dir/clients") {
        mkdir "$dir/clients";
    }
    unless (-e "$dir/data") {
        open my $fh, '>', "$dir/data"; # TODO - lock!
        sysseek $fh, $self->data_size - 1, 0;
        print {$fh} "\0";
        close $fh;
    }
}

has 'buffer' => (
    is => 'rw',
    isa => 'ArrayRef[Str]',
    default => sub { [] },
);

sub lock {
    my $self = shift;
    return lockf($self->dir.'/lock', { blocking => 1 });
}

sub write_chunk {
    my ($self, $chunk) = (shift, shift);
    push @{ $self->buffer }, @$chunk;
}

sub position {
    my $self = shift;
    my $state = Yandex::Persistent->new($self->dir.'/state', { read_only => 1 });
    return $state->{position} || 0;
}

sub set_position {
    my $self = shift;
    my $position = shift;
    $self->check_read_only;
    my $state = Yandex::Persistent->new($self->dir.'/state', { auto_commit => 0 });
    $state->{position} = $position;
    $state->commit;
}

sub _check_cross {
    my $self = shift;
    my ($left, $right, $positions) = validated_list(
        \@_,
        left => { isa => 'Int' },
        right => { isa => 'Int' },
        positions => { isa => 'HashRef[Int]' }, # name -> int value
    );
    if ($right > $self->data_size) {
        confess "right side is too big ($right)";
    }
    if ($left > $self->data_size) {
        confess "right side is too big ($left)";
    }
    if ($left < $right) {
        # easy
        for my $name (keys %$positions) {
            my $position = $positions->{$name};
            confess "crossed $name position ($position)" if $position > $left and $position <= $right;
        }
    }
    else {
        $self->_check_cross(left => $left, right => $self->data_size, positions => $positions);
        $self->_check_cross(left => 0, right => $right, positions => $positions);
    }
}

sub commit {
    my $self = shift;

    my $lock = $self->lock;
    my $buffer = $self->buffer;
    open my $fh, '+<', $self->dir.'/data';

    my $old_position = $self->position;
    sysseek($fh, $old_position, SEEK_SET);

    my $write = sub {
        my $line = shift;
        my $left = length $line;
        my $offset = 0;
        while ($left) {
            my $bytes = $fh->syswrite($line, $left, $offset); # FIXME - restart on overflow!
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
        confess "buffer is too large ($buffer_length)" if $buffer_length >= $self->data_size;
        my $left = int(sysseek($fh, 0, SEEK_CUR));
        my $right = $left + $buffer_length;
        $right -= $self->data_size if $right > $self->data_size;
        $self->_check_cross(
            left => $left,
            right => $right,
            positions => {
                "storage's own" => $old_position,
                # TODO - client positions
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
    $self->buffer([]);
}

sub in {
    warn "in (TBI)";
}

with
    'Stream::Moose::Storage',
    'Stream::Moose::Out::Chunked', # provides 'write' implementation
    'Stream::Moose::Out::ReadOnly', # provides check_read_only and calls it before write/write_chunk/commit
    'Stream::Moose::Role::AutoOwned' => { file_method => 'dir' }, # provides owner/owner_uid
;

__PACKAGE__->meta->make_immutable;
