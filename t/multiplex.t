#!/usr/bin/perl

use strict;
use warnings;

use parent qw(Test::Class);
use Test::More;

use lib 'lib';
use lib 't/lib';
use Stream::Test::Out;

use Stream::Out::Multiplex;
use Stream::Simple qw(array_in);
use Streams;

my @v1;
my @v2;
my @v3;

sub test :Test(3) {
    process(
        array_in([ 2, 3, 4 ])
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
}

my $common_test = Stream::Test::Out->new(sub {
    Stream::Out::Multiplex->new([
        processor(sub { push @v1, shift() }),
        processor(sub { push @v2, shift() x 2 }),
        processor(sub { push @v3, shift() x 3 }),
    ])
});

Test::Class->runtests(
    __PACKAGE__->new, $common_test
);

