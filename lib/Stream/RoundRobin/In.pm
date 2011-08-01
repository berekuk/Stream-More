package Stream::RoundRobin::In;

use namespace::autoclean;
use Moose;

use autodie qw( open seek ); # can't import everything - read messes with stream's read method
use Fcntl qw( SEEK_SET SEEK_CUR SEEK_END );

use Yandex::Persistent;
use Yandex::Lockf;

use Stream::RoundRobin::Types qw(:all);

sub isa {
    return 1 if $_[1] eq __PACKAGE__;
    $_[0]->next::method if $_[0]->next::can;
} # ugly hack

has 'storage' => (
    is => 'ro',
    isa => 'Stream::RoundRobin',
    required => 1,
);

has 'name' => (
    is => 'ro',
    isa => ClientName,
    required => 1,
);

has 'dir' => (
    is => 'ro',
    isa => 'Str',
    lazy => 1,
    default => sub {
        my $self = shift;
        $self->storage->dir.'/clients/'.$self->name;
    }
);


has 'lock' => (
    is => 'ro',
    lazy_build => 1,
);

sub _build_lock {
    my $self = shift;
    return lockf($self->dir.'/lock', { blocking => 1 });
}

has 'position' => (
    is => 'rw',
    isa => 'Int',
    lazy_build => 1,
);

sub _build_position {
    my $self = shift;
    my $state = Yandex::Persistent->new($self->dir.'/state', { read_only => 1 });

    my $position = $state->{position};
    $position = $self->storage->position unless defined $position;

    return $position;
}

sub commit_position {
    my $self = shift;
    $self->check_read_only;

    my $position = $self->position; # before creating $state to avoid deadlock

    my $state = Yandex::Persistent->new($self->dir.'/state', { auto_commit => 0 });
    $state->{position} = $position;
    $state->commit;

    $self->clear_position;
}

sub read_chunk {
    my $self = shift;
    my $length = shift;

    $self->lock;

    open my $fh, '+<', $self->storage->dir.'/data';

    my $cur = $self->position;
    seek($fh, $cur, SEEK_SET);

    my $write_position = $self->storage->position;

    my $read_until_wrap;
    if ($cur > $write_position) {
        $read_until_wrap = 1;
    }

    my $data_size = $self->storage->data_size;
    my $incomplete;
    my @buffer;

    while (@buffer < $length) {
        if ($cur == $write_position) {
            confess "Unexpected incomplete line $incomplete" if defined $incomplete;
            # ok, nothing more to read
            last;
        }
        my $line = <$fh>;
        $cur += length $line;
        if ($line =~ /\n$/) {
            if (defined $incomplete) {
                $line = $incomplete.$line;
                undef $incomplete;
            }
            push @buffer, $line;
        }
        else {
            if ($cur == $data_size) {
                confess "Two incomplete lines in a row" if defined $incomplete;
                $incomplete = $line;
            }
            else {
                confess "Incomplete line"; # this is pretty much impossible if file has the correct size
            }
        }

        if ($read_until_wrap) {
            if ($cur == $data_size) {
                # wrap!
                seek($fh, 0, SEEK_SET);
                $read_until_wrap = 0;
                $cur = 0;
            }
        }
        elsif ($cur > $write_position) {
            confess "Read crossed write position, something's wrong";
        }
    }

    $self->position($cur);

    return unless @buffer;
    return \@buffer;
}

sub commit {
    my $self = shift;

    $self->commit_position;
    $self->clear_lock;
}

with 'Stream::Moose::In::Chunked',
    'Stream::Moose::In::ReadOnly', # provides check_read_only and calls it before write/write_chunk/commit
    'Stream::Moose::Role::AutoOwned' => { file_method => 'dir' }, # provides owner/owner_uid
;

__PACKAGE__->meta->make_immutable;
