#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 14;
use PPB::Test::Logger;

use lib 'lib';

use Stream::Queue;
use PPB::Test::TFiles;

my $queue = Stream::Queue->new({
    dir => 'tfiles',
});
$queue->write("abc");
$queue->write("def");
$queue->commit;

my $reader = $queue->stream('client1');
is($reader->read(), 'abc');
$reader->commit;

$reader = $queue->stream('client2');
is($reader->read(), 'abc');
$reader->commit;

is_deeply( [ glob("tfiles/*.chunk") ], [ 'tfiles/1.chunk' ]);

$reader = $queue->stream('client1');
is($reader->read(), 'def');
$reader->commit;

is_deeply( [ glob("tfiles/*.chunk") ], [ 'tfiles/1.chunk' ], 'client2 still prevent chunk from deletion');

$reader = $queue->stream('client1');
is($reader->read(), undef);

undef $reader;

$queue->write({ c => 2, line => 'aaa' });
$queue->write({ c => 2, line => 'bbb' });
$queue->write({ c => 2, line => 'ccc' });
$queue->commit;

$queue->write({ c => 3, line => 'aaa' });
$queue->write({ c => 3, line => 'bbb' });
$queue->commit;

$queue->write({ c => 4, line => 'aaa' });
$queue->write({ c => 4, line => 'bbb' });
undef $queue; # 4-th chunk is not commited

is_deeply( [ glob("tfiles/*.chunk") ], [ 'tfiles/1.chunk', 'tfiles/2.chunk', 'tfiles/3.chunk' ]);

$queue = Stream::Queue->new({
    dir => 'tfiles',
});
$reader = $queue->stream('client2');

is($reader->read(), 'def'); # continue from last line

my $reader2 = $queue->stream('client2'); # another reader with the same client
is_deeply($reader->read(), { c => 2, line => 'aaa' }); # moving to next chunk, complex structure restored
is_deeply($reader2->read(), { c => 3, line => 'aaa' }); # first two chunks locked, 2nd reader reads from 3rd
is_deeply($reader->read(), { c => 2, line => 'bbb' });
is_deeply($reader->read(), { c => 2, line => 'ccc' });
is($reader->read(), undef);  # and 1st reader skips 3rd chunk

$reader->commit;
undef $reader;
undef $reader2;

is_deeply( [ glob("tfiles/*.chunk") ], [ 'tfiles/2.chunk', 'tfiles/3.chunk' ], 'first chunk removed, second still remains');

$queue->clean;

