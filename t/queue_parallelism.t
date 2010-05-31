#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 1;
use PPB::Test::TFiles;
use Time::HiRes qw(time);
use Stream::Queue;
use Yandex::X;
use Params::Validate qw(:all);
use Yandex::Logger;

use lib 'lib';

Log::Log4perl->get_logger('Stream.Queue')->less_logging(1);
Log::Log4perl->get_logger('Yandex.Lockf')->less_logging(1);

sub bench_parallel {
    my $params = validate(@_, {
        forks => { type => SCALAR },
        parallel => { type => SCALAR },
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

# writing
{

    my $bench_write = sub {
        my $parallel = shift;
        bench_parallel({
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
    cmp_ok($write_parallelism, '>', 5, '10 processes are faster than 1 process at least in 5 times');
}
