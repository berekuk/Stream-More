#!/usr/bin/perl

use strict;
use warnings;

use Test::More 0.95;
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

subtest 'write parallelism' => sub {

    my $total = 1000;
    my $portion = 100;
    my $queue;
    my $bench_write = sub {
        my $parallel = shift;
        bench({
            forks => 10,
            parallel => $parallel,
            total => $total,
            code => sub {
                $queue ||= Stream::Queue->new({ dir => 'tfiles/queue' });
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
    diag("writing ".($total * $portion)." items using 1 process: $seq_time");
    diag("writing ".($total * $portion)." items using 10 processes: $parallel_time");
    cmp_ok($write_parallelism, '>', 4, '10 processes are faster than 1 process at least in 4 times');
};

subtest 'write scaling' => sub {
    my $portion = 100;
    my $bench_write = sub {
        my $total = shift;
        bench({
            forks => 3,
            parallel => 3,
            total => $total,
            code => sub {
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

    my $baseline = 1000;
    my $time = $bench_write->($baseline);
    my $double_time = $bench_write->($baseline * 2);
    my $quadro_time = $bench_write->($baseline * 4);

    diag("writing ".($portion * $baseline / 1000)."k items: $time");
    diag("writing ".($portion * $baseline * 2 / 1000)."k items: $double_time");
    diag("writing ".($portion * $baseline * 4 / 1000)."k items: $quadro_time");
    cmp_ok($quadro_time / $time, '<', 5, 'write scales good enough');
};

subtest 'read scaling' => sub {

    my $portion = 100;
    my $bench_read = sub {
        my $commit_count = shift;
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
                    for (1..$portion) {
                        $queue->write({ id => $_ });
                    }
                    $queue->commit;
                }
            },
            code => sub {
                my $queue = Stream::Queue->new({ dir => 'tfiles/queue' });
                my $client = $queue->stream('client');
                for (1..($portion / 2)) {
                    $client->read();
                }
                $client->commit;
            },
        });
    };

    my $baseline = 100;
    my $time = $bench_read->($baseline);
    my $double_time = $bench_read->(2 * $baseline);
    my $quadro_time = $bench_read->(4 * $baseline);

    diag("reading ".($baseline * $portion / 1000)."k items: $time");
    diag("reading ".($baseline * $portion * 2 / 1000)."k items: $double_time");
    diag("reading ".($baseline * $portion * 4 / 1000)."k items: $quadro_time");

    cmp_ok($quadro_time / $time, '<', 8, 'read scales well enough');
};

subtest 'read scaling in large portions' => sub {

    my $portion = 1000;
    my $bench_read = sub {
        my $commit_count = shift;
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
                    for (1..$portion) {
                        $queue->write({ id => $_ });
                    }
                    $queue->commit;
                }
            },
            code => sub {
                my $queue = Stream::Queue->new({ dir => 'tfiles/queue' });
                my $client = $queue->stream('client');
                for (1..$portion) {
                    $client->read();
                }
                $client->commit;
            },
        });
    };

    my $baseline = 10;
    my $time = $bench_read->($baseline);
    my $double_time = $bench_read->(2 * $baseline);
    my $quadro_time = $bench_read->(4 * $baseline);

    diag("reading ".($baseline * $portion / 1000)."k items in large portions: $time");
    diag("reading ".($baseline * $portion * 2 / 1000)."k items in large portions: $double_time");
    diag("reading ".($baseline * $portion * 4 / 1000)."k items in large portions: $quadro_time");

    cmp_ok($quadro_time / $time, '<', 8, 'read in large portions scales well enough');
};

done_testing;
