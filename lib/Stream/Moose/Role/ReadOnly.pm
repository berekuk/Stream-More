package Stream::Moose::Role::ReadOnly;

use Moose::Role;
with 'Stream::Moose::Role::Owned';

has 'read_only' => (
    is => 'ro',
    isa => 'Bool',
    lazy_build => 1,
);

sub _build_read_only {
    my $self = shift;
    if ($self->owner_uid ne $>) {
        return 1;
    }
    return;
}

sub check_read_only {
    my $self = shift;
    confess "Stream $self is read only" if $self->read_only;
}

1;
