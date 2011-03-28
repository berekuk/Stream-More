#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 4;

use lib 'lib';

use Yandex::Logger;
use Yandex::X;
use Yandex::Lockf;
use Params::Validate qw(:all);
use Yandex::Persistent;
use List::MoreUtils qw(all);
use PPB::Test::TFiles;
use Time::HiRes qw(sleep);

use Stream::Queue;

Log::Log4perl->get_logger('Stream.Queue')->less_logging(1);
Log::Log4perl->get_logger('Yandex.Lockf')->less_logging(1);

sub stress_run {
    my $params = validate(@_, {
        code => { type => CODEREF },
        parallel => { type => SCALAR, regex => qr/^\d+$/, default => 5 },
        invoke_count => { type => SCALAR, regex => qr/^\d+$/, default => 100 },
        tmp_dir => { default => 'tfiles' },
    });

    my $id2status = sub {
        my $id = shift;
        return Yandex::Persistent->new("$params->{tmp_dir}/stress.$id.status", { format => 'json', auto_commit => 0 });
    };

    for my $id (1..$params->{parallel}) {
        my $status = $id2status->($id);
        $status->{iterations_left} = $params->{invoke_count};
        $status->commit;
    }

    my $new_child = sub {
        my $id = shift;
        my $status = $id2status->($id);
        if ($status->{done}) {
            return;
        }
        undef $status;
        xfork() and return;

        while (1) {
            my $ok = eval {
                $params->{code}->($id);
                1;
            };
            next if $@ =~ /^stress break/;
            die $@ unless $ok;
            my $status = $id2status->($id);
            if ($status->{iterations_left} <= 0) {
                $status->{done} = 1;
                $status->commit;
                last;
            }
            $status->{iterations_left}--;
            $status->commit;
        }
        exit;
    };

    for my $child_id (1..$params->{parallel}) {
        $new_child->($child_id);
    }
    while (my $result = wait) {
        if ($result < 0) {
            #print "stress_run is over\n";
            last;
        }
        die "Child exited with error '$?'" if $?;
        #print "child $result is over\n";
    }; # TODO - randomly kill childs
}

sub stress_break {
    my $probability = shift || 0.001;
    die "stress break" if rand() < $probability; # TODO - type exception
}

# stress writing (3)
{
    my $queue = Stream::Queue->new({ dir => 'tfiles/queue' });
    $queue->register_client('client');
    undef $queue;

    my $max_id = 10;
    my $portions = 3;
    my $invoke_count = 10;
    my $parallel = 3;

    stress_run({
        code => sub {
            my $queue = Stream::Queue->new({ dir => 'tfiles/queue' });
            stress_break();
            for my $portion (1..$portions) {
                for my $id (1..$max_id) {
                    $queue->write({ id => $id, time => time });
                    stress_break();
                }
                $queue->commit;
                stress_break();
            }
        },
        parallel => $parallel,
        invoke_count => $invoke_count,
    });
    print "stress_run is over\n";

    # in the end, storage should contain at least 3 * 10 * 5 * 100 = 15000 items
    # total id count for each id should be equal

    $queue = Stream::Queue->new({ dir => 'tfiles/queue' });
    my $client = $queue->stream('client');

    my $idstat = {};
    my $total = 0;
    while (my $item = $client->read) {
        my @item = ($item);
        validate(@item, {
            time => { type => SCALAR },
            id => { type => SCALAR, regex => qr/^\d+$/ },
        });
        $idstat->{$item->{id}}++;
        $total++;
    }
    $client->commit;
    print "total: $total\n";
    cmp_ok($total, '>=', $portions * $invoke_count * $max_id * $parallel);

    my $number_of_items_by_id = $parallel * $invoke_count * $portions;
    ok(scalar(all { $_ and $_ >= $number_of_items_by_id and $_ <= $number_of_items_by_id * 3 } @$idstat{ 1..$max_id }), 'there are enough items with each id');
    my $idcount = $total / $max_id;
    ok(scalar(all { $_ == $idcount } @$idstat{ 1..$max_id }), 'all ids are equal');
}

# stress reading (1)
{
    PPB::Test::TFiles::import();

    my $total = 1000;
    {
        my $queue = Stream::Queue->new({ dir => 'tfiles/queue' });
        $queue->register_client('client');

        for my $id (1..$total) {
            $queue->write({ id => "id$id", time => time });
            $queue->commit if rand() < $total / 10;
        }
        $queue->commit;
    }

    stress_run({
        code => sub {
            my $queue = Stream::Queue->new({ dir => 'tfiles/queue' });
            stress_break();
            sleep 0.0001 * rand(3);
            my $in = $queue->stream('client');
            my $log = xopen('>>', 'tfiles/log');
            $log->autoflush(1);
            for (1..10) {
                sleep 0.0001 * rand(3);
                my $item = $in->read() or last;
                sleep 0.0001 * rand(3);
                my $lock = lockf('tfiles/lock');
                xprint($log, "$item->{id}\n");
            }
            sleep 0.0001;
            $in->commit;
        },
        parallel => 3,
        invoke_count => int($total / 3) + 100,
    });

    my @ids;
    my $fh = xopen('<', 'tfiles/log');
    while (<$fh>) {
        chomp;
        push @ids, $_;
    }
    my %ids = map { $_ => 1 } @ids;
    is_deeply([ sort keys %ids ], [ sort map { "id$_" } 1..$total ], 'all ids read from queue');
}

# stress rw (1)
{
    PPB::Test::TFiles::import();

    my $get_queue = sub {
        Stream::Queue->new({ dir => 'tfiles/queue', max_chunk_size => 10_000, max_chunk_count => 1000 })
    };

    stress_run({
        code => sub {
            my $id = shift;
            if ($id <= 5) {
                # reading
                sleep 0.001;
                my $queue = $get_queue->();
                stress_break();
                my $in = $queue->stream('client');
                for (1..100) {
                    my $item = $in->read() or last;
                }
                $in->commit;
            }
            elsif ($id <= 9) {
                # writing
                my $out = $get_queue->();
                stress_break();
                for my $portion (1..10) {
                    for my $id (1..10) {
                        $out->write({ id => $id, time => time, data => join '', map { rand } 1..100 });
                        stress_break();
                    }
                    $out->commit;
                    stress_break();
                }
            }
            else {
                # gc
                diag("gc started");
                $get_queue->()->gc;
                diag("gc done");
                sleep 0.1;
            }
        },
        parallel => 10,
        invoke_count => 10,
    });
}
