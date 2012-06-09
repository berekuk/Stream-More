#!/usr/bin/perl

use strict;
use warnings;

use parent qw(Test::Class);
use Test::More;
use Test::Deep;
use Test::Fatal;

use lib 'lib';
use PPB::Test::TFiles;

use Yandex::X;

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

    $mq->commit([$items->[0]->[0], $items->[2]->[0]]);
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

    undef $mq;
    $mq = Stream::In::Buffer->new($in, { dir => "tfiles/" });
    $mq->read();
    $mq->read();
    $item = $mq->read();
    is($item->[0], 2, "autoincrement id after reset");

    $mq->commit([0,1]);
    
    undef $mq;
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
    my $mq = Stream::In::Buffer->new($in, { dir => "tfiles/", max_chunk_size => 4 });
    my $items = $mq->read_chunk(3);
    $mq->commit([map {$_->[0]} @$items]);
    $items = $mq->read_chunk(3); # lives
    like(exception { $mq->read_chunk(2) }, qr/size exceeded/, "max_chunk_size check");
}

sub lag :Tests {

    my $in = array_in(["a" .. "z"]);
    my $mq = Stream::In::Buffer->new($in, { dir => "tfiles/" });

    is($mq->lag(), 26);
    my $items = $mq->read_chunk(10);
    cmp_ok($mq->lag(), ">=", 16); #TODO: count uncommited
    $mq->commit([map {$_->[0]} splice @$items, 0, 5]);

    undef $mq; #FIXME: sum lags in all chunks!
    $mq = Stream::In::Buffer->new($in, { dir => "tfiles/", });
    is($mq->lag(), 21);
    $items = $mq->read_chunk(10);
    cmp_ok($mq->lag(), ">=", 11);
}

sub concurrent :Tests {

    my $in = array_in(["a" .. "z"]);

    my $mq1 = Stream::In::Buffer->new($in, { dir => "tfiles/" });
    my $mq2 = Stream::In::Buffer->new($in, { dir => "tfiles/" });

    $mq1->read_chunk(3);
    $mq2->read_chunk(3);

    undef $mq1;
    undef $mq2;

    $mq1 = Stream::In::Buffer->new($in, { dir => "tfiles/" });
    $mq2 = Stream::In::Buffer->new($in, { dir => "tfiles/" });

    my $items;
    push @$items, @{$mq1->read_chunk(4)};
    push @$items, @{$mq2->read_chunk(4)};

    cmp_deeply([map { $_->[1] } @$items], bag("a" .. "h"));

    like(exception { Stream::In::Buffer->new($in, { dir => "tfiles/", max_chunk_count => 2 }) }, qr/limit exceeded/);
}

sub read_chunk :Tests {
    my $in = array_in(["a", "b"]);
    my $mq = Stream::In::Buffer->new($in, { dir => "tfiles/" });

    cmp_deeply($mq->read_chunk(3), [[ ignore(), "a" ], [ ignore(), "b" ]]);
    is($mq->read_chunk(2), undef); # not []!
}

sub buffers :Tests {
    my $in = array_in(["a" .. "z"]);
    
    my @mq = map { Stream::In::Buffer->new($in, { dir => "tfiles/" }) } (1 .. 4);
    $_->read_chunk(2) for @mq;
    undef @mq;

    my $mq1 = Stream::In::Buffer->new($in, { dir => "tfiles/" });
    my $mq2 = Stream::In::Buffer->new($in, { dir => "tfiles/" });

    my ($items1, $items2);

    push @$items1, @{$mq1->read_chunk(6)};
    is({ map { ($_->[1] => 1) } @$items1 }->{"i"}, undef, "switch to another unlocked buffer");

    push @$items2, @{$mq2->read_chunk(4)};
    cmp_deeply([map { $_->[1] } @$items1, @$items2], bag("a" .. "j"), "all pending buffers fetched");

    $mq2->commit([map { $_->[0] } @$items2]);
    undef $mq1;
    undef $mq2;

    is(int(xqx("ls tfiles/ | wc -l")), 1, "buffers are merged and purged");
            
}

__PACKAGE__->new->runtests;
