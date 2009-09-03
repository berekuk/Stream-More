#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 3;

use lib 'lib';

use Stream::Out::Multiplex;
use Stream::Simple qw(array_seq);
use Streams;

my @v1;
my @v2;
my @v3;

process(
    array_seq([ 2, 3, 4 ])
    =>
    Stream::Out::Multiplex->new([
        processor(sub { push @v1, shift() }),
        processor(sub { push @v2, shift() * 2 }),
        processor(sub { push @v3, shift() ** 2 }),
    ])
);

is_deeply(\@v1, [2,3,4], 'first target stream');
is_deeply(\@v2, [4,6,8], 'second target stream');
is_deeply(\@v3, [4,9,16], 'third target stream');

