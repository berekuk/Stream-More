#!/usr/bin/env perl

use strict;
use warnings;

use lib 'lib';

use Benchmark;
use PPB::Test::TFiles;
use Stream::Simple qw(array_in);
use Stream::In::Buffer;

my $gen_sub = sub {
    my $class = shift;

    return sub {
        mkdir 'tfiles/buffer';
        my $in = array_in([1 .. 20]);
        my $mq = Stream::In::Buffer->new($in, { dir => "tfiles/buffer", buffer_class => $class });

        my @out;
        while (my $item = $mq->read) {
            my ($id, $data) = @$item;
            push @out, $data;
            $mq->commit([$id]);
        }
    };
};

warn "Benchmark.pm doesn't count disk operations, take a look at wall seconds, not at operations-per-second!";
timethese(1000, {
    map { $_ => $gen_sub->($_) } qw/ Stream::Buffer::Persistent Stream::Buffer::SQLite /
});

