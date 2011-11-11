package Stream::Moose::Role::ReadOnly;

# ABSTRACT: role for optionally read-only streams

# There is nothing stream-specific in this role, actually.
# We could refactor it into some overly-abstract Role::ReadOnly::OwnerBased role in the future.

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
