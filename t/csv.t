#!/usr/bin/perl

use strict;
use warnings;

use Test::More 0.95;
use Test::Fatal;

use lib 'lib';
BEGIN {
    $ENV{STREAM_DIR} = 'etc/stream';
}

use Yandex::X;
xsystem("rm -rf tfiles");
xsystem("mkdir tfiles");

use Stream::File::Cursor;
use Stream::Formatter::CSV;
use Stream::File;
use Streams;
my $storage = Stream::File->new("./tfiles/file");

ok(exception {
    Stream::Formatter::CSV->new();
}, 'constructor dies when no columns specified');

my $wrapper = Stream::Formatter::CSV->new(qw/ aaa bbb /);
my $formatted_storage = $wrapper->wrap($storage);
$formatted_storage->write({ aaa => 123 }); # all columns are optional
$formatted_storage->write({ aaa => 22, bbb => 33 });
ok(sub {
    $formatted_storage->write({ aaa => 222, bbb => 333, ccc => 444 });
}, 'unknown columns are forbidden');
$formatted_storage->write({ bbb => 33 }); # first column missing
$formatted_storage->write({ aaa => 22, bbb => "c\nd\n" }); # line breaks in data

$formatted_storage->commit;

my $in = $formatted_storage->stream(Stream::File::Cursor->new("./tfiles/pos"));

is_deeply(scalar($in->read), { aaa => 123 }, 'data deserialized correctly');
is_deeply(scalar($in->read), { aaa => 22, bbb => 33 }, 'data with several columns deserialized correctly');
is_deeply(scalar($in->read), { aaa => '', bbb => 33 }, 'data with missing first column deserialized correctly');

TODO: {
    local $TODO = "data with line breaks can't be written using CSV formatter yet - should think backward-compatibility first";
    is_deeply(scalar($in->read), { aaa => 22, bbb => "c\nd\n" }, 'data with line breaks deserialized correctly');
}
$in->commit;

done_testing;
