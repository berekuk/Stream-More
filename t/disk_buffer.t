#!/usr/bin/perl

use strict;
use warnings;

use parent qw(Test::Class);
use Test::More;
use Test::Deep;

use lib 'lib';

use PPB::Test::TFiles;
use Stream::File;
use Stream::File::Cursor;
use Stream::In::DiskBuffer;
use Time::HiRes qw(sleep time);
use autodie qw(:all);

sub setup :Test(setup) {
    PPB::Test::TFiles->import;
}

sub linear :Test(4) {
    my $file = Stream::File->new('tfiles/file');
    $file->write("$_\n") for 'a'..'z';
    $file->commit;

    my $in = $file->stream(Stream::File::Cursor->new('tfiles/cursor'));

    my $buffered_in = Stream::In::DiskBuffer->new($in, 'tfiles/buffer');

    is($buffered_in->read, "a\n", 'first item');
    is($buffered_in->read, "b\n", 'second item');
    $buffered_in->commit;

    $buffered_in = Stream::In::DiskBuffer->new($in, 'tfiles/buffer');
    is($buffered_in->read, "c\n", 'read after commit');

    $buffered_in = Stream::In::DiskBuffer->new($in, 'tfiles/buffer');
    is($buffered_in->read, "c\n", 'read after reading without commit');
}

sub read_only :Test(4) {
    my $file = Stream::File->new('tfiles/file');
    $file->write("$_\n") for 'a'..'z';
    $file->commit;

    my $in = $file->stream(Stream::File::Cursor->new('tfiles/cursor'));

    my $buffered_in1 = Stream::In::DiskBuffer->new($in, 'tfiles/buffer', { format => 'plain' });
    $buffered_in1->read for 1 .. 3;
    # do not commit
    my $buffered_in2 = Stream::In::DiskBuffer->new($in, 'tfiles/buffer', { format => 'plain' });
    $buffered_in2->read for 1 .. 3;
    undef $buffered_in2;
    # commit neither but unlock
    system("chmod -w -R tfiles/buffer");
    my $buffered_ro = Stream::In::DiskBuffer->new($in, 'tfiles/buffer', { read_only => 1, format => 'plain' });
    my $data = $buffered_ro->read_chunk(5); # lives_ok?
    is_deeply($data, [map {"$_\n"} ('a'..'e')], "read some");
    is($buffered_ro->lag(), (26-5)*2, "lag from chunks");
    $data = $buffered_ro->read_chunk(10);
    is_deeply($data, [map {"$_\n"} ('f'..'o')], "read more");
    is($buffered_ro->lag(), (26-15)*2, "yet lag");
    system("chmod u+w -R tfiles/buffer");
}

sub gc_bug :Test(4) {
    my $file = Stream::File->new('tfiles/file');
    $file->write("$_\n") for 'a'..'z';
    $file->commit;

    my $in = $file->stream(Stream::File::Cursor->new('tfiles/cursor'));

    my $buffered_in = Stream::In::DiskBuffer->new($in, 'tfiles/buffer');

    is($buffered_in->read, "a\n", 'first item');
    $buffered_in->commit;

    is($buffered_in->read, "b\n", 'second item');
    is($buffered_in->read, "c\n", 'third item');
    $buffered_in->commit;

    $buffered_in->gc;
    is($buffered_in->read, "d\n", "gc didn't break any posfiles");
}


sub in_coderef :Tests {
    my $file = Stream::File->new('tfiles/file');
    $file->write("$_\n") for 'a'..'z';
    $file->commit;

    my $gen_in = sub {
        return Stream::File->new('tfiles/file')->stream(Stream::File::Cursor->new('tfiles/cursor'));
    };

    my $db1 = Stream::In::DiskBuffer->new($gen_in, 'tfiles/buffer');
    my $db2 = Stream::In::DiskBuffer->new($gen_in, 'tfiles/buffer');

    my $db1_done;
    my $db2_done;
    my @db1_data;
    my @db2_data;
    while (not $db1_done or not $db2_done) {
        unless ($db1_done) {
            my $item = $db1->read;
            if ($item) {
                push @db1_data, $item;
            }
            else {
                $db1_done++;
            }
        }
        unless ($db2_done) {
            my $item = $db2->read;
            if ($item) {
                push @db2_data, $item;
            }
            else {
                $db2_done++;
            }
        }
    }
    is(@db1_data + @db2_data, 26, '26 items total');
    ok(@db1_data > 10, 'at least 10 items from first db');
    ok(@db2_data > 10, 'at least 10 items from second db');
}

sub gc_race :Tests {

    my $LINES = $ENV{LINES} || 10_000;
    my $WORKERS = 5;

    my $out = Stream::File->new('tfiles/out');

    my $file = Stream::File->new('tfiles/file');
    system('touch tfiles/file');

    for (1 .. $WORKERS) {
        fork and next;
        eval {
            my $time = time;
            my $buffered_in = Stream::In::DiskBuffer->new(
                sub { $file->stream(Stream::File::Cursor->new('tfiles/cursor')) },
                'tfiles/buffer'
            );
            my $i = 0;
            while () {
                $i++;

                $buffered_in->gc if rand() < 5 * $WORKERS / $LINES; # call gc 5 times per worker on average

                $buffered_in->lag if rand() < 5 * $WORKERS / $LINES; # call lag 5 times per worker on average
                #  lag is prone to race contitions too, let's check whether it fails

                last if time >= $time + 10; # should be enough for everyone
                my $line = $buffered_in->read;


                # busy-waiting
                # otherwise it's too probable that one worker will block everyone else and then read everything himself
                next unless $line;

                $out->write($line);
            }
            $buffered_in->commit;
            $out->commit;
        };
        if ($@) {
            diag "Worker error: $@";
            exit 1;
        }
        exit 0;
    }

    for (1 .. $LINES) {
        $file->write("$_\n");
        sleep 1 / $LINES;
    }
    $file->commit;

    while () {
        last if wait == -1;
        is($?, 0, "exit code");
    }

    my @lines = sort { $a <=> $b } split /\n/, qx(cat tfiles/out);
    cmp_deeply(\@lines, [1 .. $LINES], "no dups");
}

__PACKAGE__->new->runtests;
