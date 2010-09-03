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


__PACKAGE__->new->runtests;

