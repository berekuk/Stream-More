#!/usr/bin/env perl

use strict;
use warnings;

use lib 'lib';

use Stream::RoundRobin;
use autodie qw(:all);
use Time::HiRes qw(time);

use PPB::Test::TFiles;
use Yandex::Logger;

sub main {
    open my $fh, '>', 'tfiles/clients2time.log';

    for my $c (1 .. 10) {
        INFO "Measuring $c clients";
        system('rm -rf tfiles/rr');
        my $rr = Stream::RoundRobin->new(dir => 'tfiles/rr');
        $rr->register_client("c$_") for 1 .. $c;

        my $start = time;
        for (1 .. 2000) {
            $rr->write("abc\n") for 1 .. 100;
            $rr->commit;
        }
        print {$fh} "$c\t".(time - $start)."\n";
    }
    close $fh;

    system('octave bench/rr_graph.m');

    INFO "Graph generated at tfiles/rr.png";
}

main unless caller;
