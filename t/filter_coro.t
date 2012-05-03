#!/usr/bin/perl

use strict;
use warnings;

use lib 'lib';
use Test::More;

use Stream::Filter::Coro;
use Streams qw(filter process);
use Stream::Simple qw(memory_storage array_in);
use Time::HiRes qw(time);

my $filter = Stream::Filter::Coro->new(
    threads => 5,
    filter => filter {
        my $item = shift;
        use Coro::AnyEvent;
        Coro::AnyEvent::sleep(1);
        return $item;
    },
);

my $start = time;
my $s = memory_storage;
process(
    array_in([ 1 .. 10])
    => $filter | $s
);

my $end = time;

cmp_ok $end - $start, '<', 2.5;
cmp_ok $end - $start, '>', 1.5;

is_deeply
    [1 .. 10],
    [ sort { $a <=> $b } @{ $s->stream('test')->read_chunk(10) } ];

done_testing;
