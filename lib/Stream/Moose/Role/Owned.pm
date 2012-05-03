package Stream::Moose::Role::Owned;

# ABSTRACT: role for streams which have owners

# There is nothing stream-specific in this role, actually.
# We could refactor it into some overly-abstract Role::Owned role in the future.
# (see also Stream::Moose::Role::ReadOnly)

use Moose::Role;

use Class::DOES::Moose;
extra_does 'Stream::Role::Owned';

has 'owner' => (
    is => 'ro',
    isa => 'Str',
    lazy_build => 1,
    required => 1,
);

has 'owner_uid' => (
    is => 'ro',
    isa => 'Int',
    lazy_build => 1,
    required => 1,
);

1;
