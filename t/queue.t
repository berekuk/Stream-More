#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 5;

use lib 'lib';

use Test::Exception;
use PPB::Test::Logger;

use Stream::Queue;
use PPB::Test::TFiles;

sub fill_queue {
    my @portions = @_;
    my $queue = Stream::Queue->new({
        dir => 'tfiles',
    });

    my @written;
    for my $portion (@portions) {
        for my $item (@$portion) {
            push @written, $item;
            $queue->write($item);
        }
        $queue->commit;
    }
    return @written;
}

sub read_all {
    my ($in) = @_;
    my @items;
    while (my $item = $in->read) {
        push @items, $item;
    }
    return @items;
}

sub read_n {
    my ($in, $n) = @_;
    my @items;
    for (1..$n) {
        my $item = $in->read();
        unless (defined $item) {
            use Carp;
            confess "Can't read $n-th item";
        }
        push @items, $item;
    }
    return @items;
}

# write
{
    PPB::Test::TFiles::import();
    my @written = fill_queue(
        [ map { { i => "id$_" } } (1..10) ],
        [ map { { i => "id$_" } } (1..20) ],
        [ map { { i => "id$_" } } (1..30) ],
    );
    pass('write works');
}

# read
{
    PPB::Test::TFiles::import();
    my @written = fill_queue(
        [ map { { str => "id$_" } } (1..10) ],
        [ map { { str => "id$_" } } (11..20) ],
    );

    my $queue = Stream::Queue->new({
        dir => 'tfiles',
    });

    my $in = $queue->stream('client');
    is_deeply([ read_all($in) ], \@written, 'reading from queue');
}

# read and commits
{
    PPB::Test::TFiles::import();
    my @written = fill_queue(
        [ map { { id => $_, str => "id$_" } } (1..10) ],
        [ map { { id => $_, str => "id$_" } } (11..20) ],
    );

    my $queue = Stream::Queue->new({
        dir => 'tfiles',
    });

    my $in = $queue->stream('client');
    my @items;

    push @items, read_n($in, 5);
    $in->commit;

    $in = $queue->stream('client');
    read_n($in, 3); # to be discarded

    $in = $queue->stream('client');
    push @items, read_n($in, 15);
    $in->commit;

    is_deeply(\@items, \@written, 'reading from queue in several portions');
}

# clients
{
    PPB::Test::TFiles::import();
    my @written = fill_queue(
        [ map { { id => $_, str => "id$_" } } (1..100) ], # enough data to guarantee parallelism
        [ map { { id => $_, str => "id$_" } } (101..110) ],
        [ map { { id => $_, str => "id$_" } } (111..120) ],
    );

    warn "total: ".scalar(@written);

    my $queue = Stream::Queue->new({
        dir => 'tfiles',
    });

    my $in11 = $queue->stream('client1');
    my $in12 = $queue->stream('client1');
    my $in2 = $queue->stream('client2');
    my @items1;
    my @items2;

    push @items1, read_n($in11, 3);
    push @items1, read_n($in12, 4); # client1 total: 7
    push @items2, read_n($in2, 8); # client2 total: 8
    $in11->commit;
    $in12->commit;
    $in2->commit;

    push @items1, read_n($in11, 2);
    push @items1, read_n($in12, 1); # client1 total: 10
    push @items2, read_n($in2, 8); # client2 total: 16
    $in11->commit;
    $in12->commit;
    $in2->commit;

    read_n($in11, 3);
    read_n($in12, 5); # discarded, client1 total: 10
    push @items2, read_n($in2, 8); # client2 total: 24
    $in2->commit;

    undef $in11;
    undef $in12;
    $in11 = $queue->stream('client1');

    push @items1, read_n($in11, 110);
    push @items2, read_n($in2, 96);
    $in11->commit;
    $in2->commit;

    @items1 = sort { $a->{id} <=> $b->{id} } @items1;
    @items2 = sort { $a->{id} <=> $b->{id} } @items2;

    is_deeply(\@items1, \@written, 'reading from queue with several instances of client');
    is_deeply(\@items2, \@written, 'reading from queue with different client');
}
