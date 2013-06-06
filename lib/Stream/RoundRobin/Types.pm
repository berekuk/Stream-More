package Stream::RoundRobin::Types;

# ABSTRACT: moo-like types collection for Stream::RoundRobin

use strict;
use warnings;

use Type::Library
    -base,
    -declare => qw( ClientName );
use Type::Utils;
use Types::Standard qw( Str );

declare ClientName,
    as Str,
    where {
        /^[\w\.-]+$/
    },
    message {"Invalid client name '$_'" };

1;
