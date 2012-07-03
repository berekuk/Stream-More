#!/usr/bin/perl

use strict;
use warnings;

use lib 'lib';

use Benchmark;
use Stream::Log;
use Stream::Queue;
use Stream::RoundRobin;
use Yandex::X;

use Streams qw( process catalog );

use PPB::Test::TFiles;

my %bench;

$ENV{STREAM_LOG_POSDIR} = 'tfiles';

sub gen_bench {
    my $storage = shift;
    return sub {
        $storage->register_client('abc');
        for (1 .. 100) {
            for (1 .. 100) {
                $storage->write("line$_\n");
            }
            $storage->commit;
        }
        my $in = $storage->stream('abc');
        process($in => catalog->out('null'));
    };
}

$bench{Log} = gen_bench(
    catalog->format('storable')->wrap(
        Stream::Log->new('tfiles/log')
    )
);
$bench{Queue} = gen_bench(
    Stream::Queue->new(dir => 'tfiles/queue')
);
$bench{RoundRobin} = gen_bench(
    catalog->format('storable')->wrap(
        Stream::RoundRobin->new(dir => 'tfiles/rr')
    )
);

timethese(20, \%bench);
