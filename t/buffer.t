#!/usr/bin/perl

use strict;
use warnings;

use parent qw(Test::Class);
use Test::More;
use Test::Deep;

use lib 'lib';
use PPB::Test::TFiles;

use Stream::Simple qw(array_in);
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

__PACKAGE__->new->runtests;
