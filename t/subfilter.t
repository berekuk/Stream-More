#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 3;

use lib 'lib';

use Stream::Filter::Subfilter;
use Stream::Filter qw(filter);
use Stream::Simple qw(array_in);

{
    my $filter = filter(sub { return shift() ** 2 });
    my $in = array_in([
        { id => 2, value => "abc" },
        { id => 3, value => "def" },
    ]);
    my $subfilter = Stream::Filter::Subfilter->new($filter, sub { shift->{id} }, sub { $_[0]->{square} = $_[1]; return $_[0] });
    my $res1 = $subfilter->write($in->read);
    my $res2 = $subfilter->write($in->read);
    is_deeply($res1, { id => 2, value => "abc", square => 4 });
    is_deeply($res2, { id => 3, value => "def", square => 9 });
}

# same test, but using write_chunk
{
    my $filter = filter(sub { return shift() ** 2 });
    my $in = array_in([
        { id => 2, value => "abc" },
        { id => 3, value => "def" },
    ]);
    my $subfilter = Stream::Filter::Subfilter->new($filter, sub { shift->{id} }, sub { $_[0]->{square} = $_[1]; return $_[0] });
    my $chunk = $subfilter->write_chunk($in->read_chunk(10));
    is_deeply($chunk, [
        { id => 2, value => "abc", square => 4 },
        { id => 3, value => "def", square => 9 },
    ]);
}
