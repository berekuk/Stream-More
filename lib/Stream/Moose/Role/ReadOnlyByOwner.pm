package Stream::Moose::Role::ReadOnlyByOwner;

use Moose::Role;

has 'read_only' => (
    is => 'ro',
    isa => 'Bool',
    lazy_build => 1,
);

sub _build_read_only {
    my $self = shift;
    if ($self->owner ne scalar getpwuid($>)) {
        return 1;
    }
    return;
}

1;
