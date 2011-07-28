package Stream::Moose::Role::Owned;

use Moose::Role;

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
