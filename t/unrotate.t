#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 1;

use lib 'lib';

use Streams;
use Stream::Unrotate;

system("rm -rf tfiles && mkdir tfiles");

my @lines;

process(
    Stream::Unrotate->new({
        LogFile => "t/log",
        PosFile => "tfiles/pos",
    }) => processor {
        chomp(my $line = shift);
        push @lines, $line;
    }, 2
);

is_deeply(\@lines, [qw(abc def)], 'process works');



