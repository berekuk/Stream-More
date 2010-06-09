#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 11;
use Test::Exception;

use lib qw(lib);

use Stream::Simple qw(array_in code_out);

# array_in - read (5)
{
    my $s = array_in([5,6,7,8]);
    ok($s->isa('Stream::In'), 'array_in is a stream');
    is(scalar($s->read), 5, 'read returns first array element');
    is(scalar($s->read), 6, 'read returns first array element');
    is(scalar($s->shift), 7, 'shift is a synonim of read');
    $s->read;
    is(scalar($s->read), undef, 'last read return undef');
}

# array_in - read_chunk (3)
{
    my $s = array_in([5,6,7]);
    is_deeply($s->read_chunk(2), [5,6], 'read_chunk works too');
    is_deeply($s->read_chunk(2), [7], 'second read_chunk returns remaining elements');
    is_deeply(scalar($s->read_chunk(2)), undef, 'subsequent read_chunk returns undef');
    $s->commit; # does nothing, but shouldn't fail
}

# code_out (3)
{
    my @data_in = ('a', 5, { x => 'y' });
    my @data_out;

    throws_ok(sub { &code_out('a') }, qr/Expected callback/, 'code_out throws exception when parameter is not a coderef');

    my $p = code_out(sub { push @data_out, shift });
    ok($p->isa('Stream::Out'), 'code_out result looks like output stream');
    for (@data_in) {
        $p->write($_);
    }
    $p->commit;
    is_deeply(\@data_out, \@data_in, 'code_out result works as anonymous output stream');
}

