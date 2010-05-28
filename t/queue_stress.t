#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 3;

use lib 'lib';

use Yandex::Logger;
use Yandex::X;
use Params::Validate qw(:all);
use Yandex::Persistent;
use List::MoreUtils qw(all);
use PPB::Test::TFiles;

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
                $params->{code}->();
                1;
            };
            next unless $ok;
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
            print "stress_run is over\n";
            last;
        }
        print "child $result is over\n";
    }; # TODO - randomly kill childs
}

sub stress_break {
    my $probability = shift || 0.001;
    die if rand() < $probability;
}

# stress writing
{
    my $queue = Stream::Queue->new({ dir => 'tfiles/queue' });
    $queue->register_client('client');
    undef $queue;

    my $max_id = 10;
    my $portions = 3;
    my $invoke_count = 100;
    my $parallel = 5;

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

