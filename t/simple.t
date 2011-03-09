#!/usr/bin/perl

use strict;
use warnings;

use lib 't/lib';
use lib 'lib';

use parent qw(Test::Class);
use Stream::Test::Out;

use Test::More;
use Test::Exception;

use Stream::Simple qw(code_in array_in code_out);

# array_in - read (5)
sub array_in_read :Test(5) {
    my $s = array_in([5,6,7,8]);
    ok($s->isa('Stream::In'), 'array_in is a stream');
    is(scalar($s->read), 5, 'read returns first array element');
    is(scalar($s->read), 6, 'read returns first array element');
    is(scalar($s->shift), 7, 'shift is a synonim of read');
    $s->read;
    is(scalar($s->read), undef, 'last read return undef');
}

sub array_in_read_chunk :Test(3) {
    my $s = array_in([5,6,7]);
    is_deeply($s->read_chunk(2), [5,6], 'read_chunk works too');
    is_deeply($s->read_chunk(2), [7], 'second read_chunk returns remaining elements');
    is_deeply(scalar($s->read_chunk(2)), undef, 'subsequent read_chunk returns undef');
    $s->commit; # does nothing, but shouldn't fail
}

sub code_out_test :Test(3) {
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

sub code_in_test :Test(5) {
    my $n = 5;
    my $in = code_in(sub {
        $n--;
        return $n || undef;
    });
    is($in->read, 4);
    is($in->read, 3);
    is($in->read, 2);
    is($in->read, 1);
    is($in->read, undef);
}

Test::Class->runtests(
    __PACKAGE__->new,
    Stream::Test::Out->new(sub {
        code_out(sub {
            # ignore all data
        })
    }),
);

