package Stream::Moose::Role::Owned;

# ABSTRACT: role for streams which have owners

# There is nothing stream-specific in this role, actually.
# We could refactor it into some overly-abstract Role::Owned role in the future.
# (see also Stream::Moose::Role::ReadOnly)

use UNIVERSAL::DOES;

use Moo::Role;
use Types::Standard qw( Int Str );
use namespace::clean;

around DOES => sub {
    my ($orig, $self) = (shift, shift);
    my ($class) = @_;

    if ($class eq 'Stream::Role::Owned') {
        return 1;
    }

    return $self->$orig(@_);
};

has 'owner' => (
    is => 'lazy',
# TODO, commented due to issue https://rt.cpan.org/Ticket/Display.html?id=85895
#    isa => Str,
);

has 'owner_uid' => (
    is => 'lazy',
# TODO, commented due to issue https://rt.cpan.org/Ticket/Display.html?id=85895
#    isa => Int,
);

1;
