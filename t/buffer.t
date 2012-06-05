#!/usr/bin/perl

use strict;
use warnings;

use parent qw(Test::Class);
use Test::More;
use Test::Deep;
use Test::Exception;

use lib 'lib';
use PPB::Test::TFiles;

use Stream::Simple qw(array_in code_in);
use Stream::In::Buffer;

sub setup :Test(setup) {
    PPB::Test::TFiles::import;
}

sub simple :Tests {

    my $in = array_in(["a" .. "z"]);
    my $mq = Stream::In::Buffer->new($in, { dir => "tfiles/" });

    my $items = $mq->read_chunk(3);
    cmp_deeply($items, [ [ignore(), "a"], [ignore(), "b"], [ignore(), "c"] ]);

    $mq->commit($items->[0]->[0], $items->[2]->[0]);
    undef $mq;

    $mq = Stream::In::Buffer->new($in, { dir => "tfiles/" });
    $items = $mq->read_chunk(2);
    cmp_deeply($items, [ [ignore(), "b"], [ignore(), "d"] ]);
}

sub id :Tests {
    
    my $in = array_in(["a" .. "z"]);
    my $mq = Stream::In::Buffer->new($in, { dir => "tfiles/" });

    $mq->read();
    my $item = $mq->read();
    is($item->[0], 1, "autoincrement id");

    $mq = Stream::In::Buffer->new($in, { dir => "tfiles/" });
    $mq->read();
    $mq->read();
    $item = $mq->read();
    is($item->[0], 2, "autoincrement id after reset");

    $mq->commit(0,1);
    
    $mq = Stream::In::Buffer->new($in, { dir => "tfiles/" });

    $mq->read();
    $item = $mq->read();
    is($item->[0], 3, "autoincrement id after commit");
}

sub lazy :Tests {
    my $arr = ["a" .. "z"];
    my $in = array_in($arr);
    my $mq = Stream::In::Buffer->new($in, { dir => "tfiles/" });

    $mq->read_chunk(3);
    cmp_ok(scalar(@$arr), ">=", 26-3);
    $mq->read_chunk(5);
    cmp_ok(scalar(@$arr), ">=", 26-8);

    my $arr_size = @$arr;
    undef $mq;
    $mq = Stream::In::Buffer->new($in, { dir => "tfiles/" });
    $mq->read_chunk(8);
    cmp_ok(scalar(@$arr), "==", $arr_size);
}

sub size :Tests {
    
    my $in = array_in(["a" .. "z"]);
    my $mq = Stream::In::Buffer->new($in, { dir => "tfiles/", size => 4 });
    my $items = $mq->read_chunk(3);
    throws_ok(sub { $mq->read_chunk(2) }, qr/exhausted/);
    $mq->commit(map {$_->[0]} @$items);
    lives_ok(sub { $mq->read_chunk(2) });
}

__PACKAGE__->new->runtests;
