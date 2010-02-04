#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 11;
use Test::Exception;

use lib 'lib';

use Stream::Out::Any;
use Stream::Simple qw(array_seq);
use Streams;

# 4
{
    my @v;

    my $in = array_seq([ 5..15 ]);
    my $out = Stream::Out::Any->new([
        map {
            my $i = $_;
            processor(sub { push @{$v[$i]}, shift() });
        } (0..2)
    ]);

    while (my $item = $in->read) {
        $out->write($item);
    }
    $out->commit;

    is_deeply(
        [ sort { $a <=> $b } map { @$_ } @v ],
        [5..15],
        'all items transfered'
    );
    for (0..2) {
        cmp_ok(scalar @{ $v[$_] }, '>=', 2, "target stream $_ is balanced");
    }
}

# 3
{
    my @v;

    my $in = array_seq([ 5..15 ]);
    my $out = Stream::Out::Any->new([
        (
            map {
                my $i = $_;
                processor(sub { push @{$v[$i]}, shift() }),
            } (0..1)
        ),
        processor(sub { die }),
    ]);

    while (my $item = $in->read) {
        $out->write($item);
    }
    $out->commit;

    is_deeply(
        [ sort { $a <=> $b } map { @$_ } @v ],
        [5..15],
        'all items transfered'
    );
    for (0..1) {
        cmp_ok(scalar @{ $v[$_] }, '>=', 4, "target stream $_ is balanced");
    }
}

# 3
{
    {
        package t::Stream;
        use base qw(Stream::Out);
        sub write {}
        sub commit { die }
    }
    my @v;

    my $in = array_seq([ 5..15 ]);
    my $out = Stream::Out::Any->new([
        (
            map {
                my $i = $_;
                processor(sub { push @{$v[$i]}, shift() }),
            } (0..1)
        ),
        t::Stream->new,
    ]);

    while (my $item = $in->read) {
        $out->write($item);
    }
    $out->commit;

    is_deeply(
        [ sort { $a <=> $b } map { @$_ } @v ],
        [5..15],
        'all items transfered'
    );
    for (0..1) {
        cmp_ok(scalar @{ $v[$_] }, '>=', 4, "target stream $_ is balanced");
    }
}

{
    my $out = Stream::Out::Any->new([
        (processor(sub { die })) x 3
    ]);
    dies_ok(sub {
        $out->write(1);
    }, 'write in Any stream when all targets are invalid throws exception');
}
