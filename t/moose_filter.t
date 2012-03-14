#!/usr/bin/perl

use strict;
use warnings;

use lib 'lib';
use Test::More;

use Scalar::Util qw(blessed);

# core role tests
{
    package X;
    use Moose;
    with 'Stream::Moose::Filter';

    sub write { $_[1] }
    sub write_chunk { $_[1] }
    sub commit {}
}

{
    ok(
        X->new->isa('Stream::Filter'),
        'fake Stream::Filter isa'
    );

    ok(
        blessed(X->new | X->new),
        '| overload works'
    );
}

# Buffered role tests
{
    package Buff;
    use Moose;
    with 'Stream::Moose::Filter::Buffered';

    has '+buffer_size' => ( default => 3 );

    sub flush {
        my $self = shift;
        my ($lines) = @_;
        return [ map { $_ * 2 } @$lines ];
    }
}

{
    my $f = Buff->new;

    ok(
        $f->DOES('Stream::Filter'),
        'buffered filter is still a filter'
    );

    is_deeply [$f->write(1)], [], '1st item is buffered';
    is_deeply [$f->write(2)], [], '2nd item is buffered';
    is_deeply [$f->write(3)], [2,4,6], 'flush on 3rd item';
    is_deeply [$f->write(4)], [], '4th item is buffered again';
    is_deeply [$f->write(5)], [], '5th item is buffered';
    is_deeply [$f->write(6)], [8,10,12], 'flush last 3 items again';

    is_deeply $f->write_chunk([7,8]), [], 'two items are buffered on write_chunk';
    is_deeply $f->write_chunk([9,10]), [14,16,18,20], 'everything flushed on write_chunk with overflow';

    is_deeply $f->write_chunk([11,12]), [], 'two items buffered';
    is_deeply [$f->commit()], [22,24], 'final flush on commit';

    is_deeply $f->write_chunk([13,14,15]), [26,28,30], 'write_chunk after commit - buffer emptied on commit and filled again';
}

done_testing;
