#!/usr/bin/env perl

use strict;
use warnings;

use lib 'lib';

use Benchmark;
use Coro;
use PPB::Test::TFiles;
use Stream::Simple qw(array_in);
use Stream::In::Buffer;

my $gen_sub = sub {
    my $class = shift;

    return sub {
        mkdir 'tfiles/buffer';
        my $in = array_in([map {$_ . "\n"} (1 .. 1000)]);
        my $mq = Stream::In::Buffer->new($in, { dir => "tfiles/buffer", buffer_class => $class });

        my @out;
        my %ids;
        # prefetch some items first to fill the buffer
        for my $i (1 .. 100) {
            my $item = $mq->read;
            my ($id, $data) = @$item;
            push @out, $data;
            $ids{$id}++;
        }

        while (my $item = $mq->read) {
            my ($id, $data) = @$item;
            $ids{$id}++;
            push @out, $data;
            my @ids = keys %ids;
            my $commit_id = $ids[rand $#ids];
            delete $ids{$commit_id};
            $mq->commit([$commit_id]);
        }

        for my $id (keys %ids) {
            $mq->commit([$id]);
        }
    };
};

warn "Benchmark.pm doesn't count disk operations, take a look at wall seconds, not at operations-per-second!";
timethese($ENV{TIMES} || 10, {
    map { $_ => $gen_sub->($_) } qw/ Stream::Buffer::Persistent Stream::Buffer::SQLite Stream::Buffer::File /
});

