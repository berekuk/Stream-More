package Stream::RoundRobin::Types;

# ABSTRACT: moose types collection for Stream::RoundRobin

use strict;
use warnings;

use MooseX::Types
    -declare => [qw( ClientName )];

use MooseX::Types::Moose qw( Str );

subtype ClientName,
    as Str,
    where { /^[\w\.-]+$/ };

1;
