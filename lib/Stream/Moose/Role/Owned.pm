package Stream::Moose::Role::Owned;

# ABSTRACT: role for streams which have owners

# There is nothing stream-specific in this role, actually.
# We could refactor it into some overly-abstract Role::Owned role in the future.
# (see also Stream::Moose::Role::ReadOnly)

use Moo::Role;
use MooX::Types::MooseLike::Base qw( Int Str );

around DOES => sub {
    my ($orig, $self) = (shift, shift);
    my ($class) = @_;

    if ($class eq 'Stream::Role::Owned') {
        return 1;
    }

    unless ($self->can('DOES')) {
        $orig = 'isa';
    }
    return $self->$orig(@_);
};

has 'owner' => (
    is => 'lazy',
    isa => Str,
);

has 'owner_uid' => (
    is => 'lazy',
    isa => Int,
);

1;
