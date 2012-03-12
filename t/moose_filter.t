#!/usr/bin/perl

use strict;
use warnings;

use lib 'lib';
use Test::More;

{
    package X;
    use Moose;
    with 'Stream::Moose::Filter';

    sub write { $_[1] }
    sub write_chunk { $_[1] }
    sub commit {}
}

use Scalar::Util qw(blessed);

ok(
    X->new->isa('Stream::Filter'),
    'fake Stream::Filter isa'
);

ok(
    blessed(X->new | X->new),
    '| overload works'
);

done_testing;

