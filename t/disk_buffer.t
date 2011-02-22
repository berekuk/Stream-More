#!/usr/bin/perl

use strict;
use warnings;

use parent qw(Test::Class);
use Test::More;

use lib 'lib';

use PPB::Test::TFiles;
use Stream::File;
use Stream::File::Cursor;
use Stream::In::DiskBuffer;

sub setup :Test(setup) {
    PPB::Test::TFiles::import;
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
    system("chmod -w -R tfiles/buffer"); die if $?;
    my $buffered_ro = Stream::In::DiskBuffer->new($in, 'tfiles/buffer', { read_only => 1, format => 'plain' });
    my $data = $buffered_ro->read_chunk(5); # lives_ok?
    is_deeply($data, [map {"$_\n"} ('a'..'e')], "read some");
    is($buffered_ro->lag(), (26-5)*2, "lag from chunks");
    $data = $buffered_ro->read_chunk(10);
    is_deeply($data, [map {"$_\n"} ('f'..'o')], "read more");
    is($buffered_ro->lag(), (26-15)*2, "yet lag");
    system("chmod u+w -R tfiles/buffer"); die if $?;
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


__PACKAGE__->new->runtests;

