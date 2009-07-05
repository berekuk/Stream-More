#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 8;

use lib qw(lib);

use Stream::Simple qw(array_seq);

{
    my $s = array_seq([5,6,7,8]);
    ok($s->isa('Stream::Stream'), 'array_seq is a stream');
    is(scalar($s->read), 5, 'read returns first array element');
    is(scalar($s->read), 6, 'read returns first array element');
    is(scalar($s->shift), 7, 'shift is a synonim of read');
    $s->read;
    is(scalar($s->read), undef, 'last read return undef');
}

{
    my $s = array_seq([5,6,7]);
    is_deeply($s->read_chunk(2), [5,6], 'read_chunk works too');
    is_deeply($s->read_chunk(2), [7], 'second read_chunk returns remaining elements');
    is_deeply(scalar($s->read_chunk(2)), undef, 'subsequent read_chunk returns undef');
    $s->commit; # does nothing, but shouldn't fail
}

