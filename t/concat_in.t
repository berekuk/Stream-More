#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 2;

use lib 'lib';

use Stream::Simple qw( array_in code_out );
use Streams qw( process );
use Stream::Concat::In;

{
    my $in = Stream::Concat::In->new(
        array_in(['a'..'d']),
        array_in(['e'..'z']),
    );

    my @result;
    process($in => code_out { push @result, @_ });
    is_deeply(['a'..'z'], \@result);
}

{
    my $in = Stream::Concat::In->new(
        array_in([]),
        array_in(['a'..'d']),
        array_in(['e'..'g']),
        array_in(['h'..'y']),
        array_in([]),
        array_in([]),
        array_in(['z']),
        array_in([]),
    );

    my @result;
    process($in => code_out { push @result, @_ });
    is_deeply(['a'..'z'], \@result);
}
