#!/usr/bin/perl

use strict;
use warnings;

use Test::More;
use parent qw(Test::Class);

use lib 'lib';
BEGIN {
    $ENV{STREAM_DIR} = 'etc/stream';
}

use Yandex::X;

use Stream::File::Cursor;
use Stream::Formatter::JSON;
use Stream::File;
use Streams;

sub setup :Test(setup) {
    xsystem("rm -rf tfiles");
    xsystem("mkdir tfiles");
}

sub generic :Test(3) {
    my $storage = Stream::File->new("./tfiles/file");
    my $wrapper = Stream::Formatter::JSON->new;
    my $formatted_storage = $wrapper->wrap($storage);
    $formatted_storage->write({ abc => "def" });
    $formatted_storage->write("ghi");
    $formatted_storage->commit;

    my $in = $formatted_storage->stream(Stream::File::Cursor->new("./tfiles/pos"));
    is_deeply(scalar($in->read), { abc => 'def' }, 'data deserialized correctly');
    is_deeply(scalar($in->read), 'ghi', 'simple strings can be stored too');
    $in->commit;
    $in = $formatted_storage->stream(Stream::File::Cursor->new("./tfiles/pos"));
    is($in->read, undef, 'commit worked, nothing to read');
}

sub legacy :Test(1) {
    xsystem('cp t/data/legacy_json.log tfiles/legacy_json.log');
    my $storage = Stream::File->new("./tfiles/legacy_json.log");
    my $wrapper = Stream::Formatter::JSON->new;
    my $formatted_storage = $wrapper->wrap($storage);
    $formatted_storage->write({ "a\n\\b" => "ddd\n\\eee" });
    $formatted_storage->commit;
    my $in = $formatted_storage->stream(Stream::File::Cursor->new("./tfiles/legacy.pos"));
    my $old_data = $in->read;
    my $new_data = $in->read;
    is_deeply($old_data, $new_data, 'old data and new data are equal');
}

__PACKAGE__->new->runtests();
