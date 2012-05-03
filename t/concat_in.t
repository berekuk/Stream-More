#!/usr/bin/perl

use strict;
use warnings;

use Test::More 0.95;

use lib 'lib';

use Stream::Simple qw( array_in code_out code_in );
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

{
    my $in = Stream::Concat::In->new(
        array_in([]),
    );
    my @empty_read = $in->read;
    is_deeply \@empty_read, [undef];
}

{
    my @data1 = ('foo');
    my @data2 = ('bar');
    my $in1 = code_in { shift @data1 };
    my $in2 = code_in { shift @data2 };

    my $storage = Stream::Concat::In->new($in1, $in2);

    is $storage->read, 'foo';
    is $storage->read, 'bar';
    push @data1, 'foo2';
    push @data2, 'bar2';
    is $storage->read, 'bar2';
    is $storage->read, undef;

    push @data1, 'foo3';
    push @data2, 'bar3';
    is $storage->read, 'bar3';
    is $storage->read, undef;
}

done_testing;
