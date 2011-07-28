package Stream::RoundRobin;

use namespace::autoclean;

use Moose;
use autodie;

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

has 'size' => (
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
        seek $fh, $self->size - 1, 0;
        print {$fh} "\0";
        close $fh;
    }
}

sub write {
    warn "write (TBI)";
}

sub commit {
    warn "commit (TBI)";
}

sub in {
    warn "in (TBI)";
}

with
    'Stream::Moose::Storage',
    'Stream::Moose::Out::ReadOnly',
    'Stream::Moose::Role::AutoOwned' => { file_method => 'dir' },
;

__PACKAGE__->meta->make_immutable;
