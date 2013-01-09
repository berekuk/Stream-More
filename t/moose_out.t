#!/usr/bin/perl

use strict;
use warnings;

use lib 'lib';
use Test::More;

use Scalar::Util qw(blessed);

# core role tests
{
    package X;
    use Moose;
    with 'Stream::Moose::Out::Buffered';

    sub commit {};
    sub write_chunk {};
}

my $WRITTEN = 0;

# Buffered role tests
{
    package Buff;
    use Moose;
    with 'Stream::Moose::Out::Buffered';

    has '+buffer_size' => ( default => 3 );

    sub write_chunk {
        my $self = shift;
        my ($lines) = @_;
        print "Buff\n";
        $WRITTEN += scalar @$lines;
    }

    sub commit {
    }
}

{
    my $f = Buff->new;

    ok(
        $f->DOES('Stream::Out'),
        'buffered out is still an out'
    );

    $f->write(1);
    is $WRITTEN, 0, '1st item is buffered';
    $f->write(2);
    is $WRITTEN, 0, '2nd item is buffered';
    $f->write(3);
    is $WRITTEN, 3, 'write_chunk on 3rd item';
    $f->write(4);
    is $WRITTEN, 3, '4th item is buffered again';
    $f->write(5);
    is $WRITTEN, 3, '5th item is buffered';
    $f->write(6);
    is $WRITTEN, 6, 'write_chunk last 3 items again';

    $f->write_chunk([7,8]);
    is $WRITTEN, 6, 'two items are buffered on write_chunk';
    $f->write_chunk([9,10]);
    is $WRITTEN, 10, 'everything write_chunked on write_chunk with overflow';

    $f->write_chunk([11,12]);
    is $WRITTEN, 10, 'two items buffered';
    $f->commit();
    is $WRITTEN, 12, 'final write_chunk on commit';

    $f->write_chunk([13,14,15]);
    is $WRITTEN, 15, 'write_chunk after commit - buffer emptied on commit and filled again';
}

done_testing;
