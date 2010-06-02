#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 3;
use lib 'lib';

use PPB::Test::TFiles;
use Time::HiRes qw(time);
use Stream::Queue;
use Yandex::X;
use Params::Validate qw(:all);
use Yandex::Logger;

Log::Log4perl->get_logger('Stream.Queue')->less_logging(1);
Log::Log4perl->get_logger('Yandex.Lockf')->less_logging(1);

sub bench {
    my $params = validate(@_, {
        forks => { type => SCALAR, default => 1 },
        parallel => { type => SCALAR, default => 1 },
        total => { type => SCALAR },
        code => { type => CODEREF },
        setup => { type => CODEREF, default => sub {} },
    });

    $params->{setup}->();

    my $total = $params->{total};
    my $parallel = $params->{parallel};
    my $process_total = int($total / $parallel);

    my $started = time;
    for my $child_id (1..$params->{forks}) {
        my $count = $process_total;
        if ($child_id == $parallel) {
            $count = $total; # last real process
        }
        $total -= $process_total;
        DEBUG "forking to process $count portions" if $child_id <= $parallel;

        xfork() and next;
        if ($child_id > $parallel) {
            exit; # fake process, just to compensate performance with fake forks
        }
        for (1..$count) {
            $params->{code}->();
        }
        exit;
    }
    1 while wait > -1;
    return time - $started;
}

# write parallelism
{

    my $bench_write = sub {
        my $parallel = shift;
        bench({
            forks => 10,
            parallel => $parallel,
            total => 1000,
            code => sub {
                my $portion = 100;
                my $queue = Stream::Queue->new({ dir => 'tfiles/queue' });
                for (1..$portion) {
                    $queue->write({ id => $_ });
                }
                $queue->commit;
            },
            setup => sub {
                PPB::Test::TFiles::import;
                my $queue = Stream::Queue->new({ dir => 'tfiles/queue' });
                $queue->register_client('client');
                undef $queue;
            },
        });
    };

    my $seq_time = $bench_write->(1);
    my $parallel_time = $bench_write->(10);
    # TODO - check that data is valid in both cases?

    my $write_parallelism = $seq_time / $parallel_time;
    diag("seq_time: $seq_time");
    diag("parallel_time: $parallel_time");
    cmp_ok($write_parallelism, '>', 4, '10 processes are faster than 1 process at least in 4 times');
}

# write scaling
{
    my $bench_write = sub {
        my $total = shift;
        bench({
            forks => 3,
            parallel => 3,
            total => $total,
            code => sub {
                my $portion = 100;
                my $queue = Stream::Queue->new({ dir => 'tfiles/queue' });
                for (1..$portion) {
                    $queue->write({ id => $_ });
                }
                $queue->commit;
            },
            setup => sub {
                PPB::Test::TFiles::import;
                my $queue = Stream::Queue->new({ dir => 'tfiles/queue' });
                $queue->register_client('client');
                undef $queue;
            },
        });
    };

    my $time = $bench_write->(1000);
    my $double_time = $bench_write->(2000);
    my $quadro_time = $bench_write->(4000);

    diag("writing 1000 chunks: $time");
    diag("writing 2000 chunks: $double_time");
    diag("writing 4000 chunks: $quadro_time");
    cmp_ok($quadro_time / $time, '<', 5, 'write scales good enough');
}

# read scaling
{

    my $bench_read = sub {
        my $commit_count = shift;
        my $portion_size = 100;
        bench({
            forks => 3,
            parallel => 3,
            total => $commit_count * 2,
            setup => sub {
                PPB::Test::TFiles::import;
                my $queue = Stream::Queue->new({ dir => 'tfiles/queue' });
                $queue->register_client('client');
                undef $queue;

                $queue = Stream::Queue->new({ dir => 'tfiles/queue' });
                for (1..$commit_count) {
                    for (1..$portion_size) {
                        $queue->write({ id => $_ });
                    }
                    $queue->commit;
                }
            },
            code => sub {
                my $queue = Stream::Queue->new({ dir => 'tfiles/queue' });
                my $client = $queue->stream('client');
                for (1..($portion_size / 2)) {
                    $client->read();
                }
                $client->commit;
            },
        });
    };

    my $baseline_chunks = 20;
    my $time = $bench_read->($baseline_chunks);
    my $double_time = $bench_read->(2 * $baseline_chunks);
    my $quadro_time = $bench_read->(4 * $baseline_chunks);

    diag("reading $baseline_chunks chunks: $time");
    diag("reading ".($baseline_chunks * 2)." chunks: $double_time");
    diag("reading ".($baseline_chunks * 4)." chunks; $quadro_time");

    cmp_ok($quadro_time / $time, '<', 5, 'read scales good enough');
}
