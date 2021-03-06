#!/usr/bin/perl

use strict;
use warnings;

use parent qw(Test::Class);
use Test::More;
use Test::Exception;

use lib 'lib';

use Stream::Out::Any;
use Stream::Simple qw(array_in code_out);
use Streams;

sub balance_three :Test(4) {
    my @v;

    my $in = array_in([ 5..15 ]);
    my $out = Stream::Out::Any->new([
        map {
            my $i = $_;
            code_out(sub { push @{$v[$i]}, shift() });
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

sub balance_two :Test(3) {
    my @v;

    my $in = array_in([ 5..15 ]);
    my $out = Stream::Out::Any->new([
        (
            map {
                my $i = $_;
                code_out(sub { push @{$v[$i]}, shift() }),
            } (0..1)
        ),
        code_out(sub { die }),
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

sub invalidate :Test(3) {
    {
        package t::Stream;
        use base qw(Stream::Out);
        sub write {}
        sub commit { die }
    }
    my @v;

    my $in = array_in([ 5..15 ]);
    my $out = Stream::Out::Any->new([
        (
            map {
                my $i = $_;
                code_out(sub { push @{$v[$i]}, shift() }),
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

sub invalidate_all :Test(1) {
    my $out = Stream::Out::Any->new([
        (code_out(sub { die })) x 3
    ]);
    dies_ok(sub {
        $out->write(1);
    }, 'write in Any stream when all targets are invalid throws exception');
}

sub revalidate :Test(3) {
    {
        package t::RevStream;
        use parent qw(Stream::Out);
        our $flag;
        sub new {
            bless { data => $_[1] } => $_[0];
        }
        sub write {
            die unless $t::RevStream::flag++;
            push @{ $_[0]->{data} }, $_[1];
        }
    }
    my @data;
    my @data_rev;
    my $out = Stream::Out::Any->new([
        code_out(sub { push @data, shift }),
        t::RevStream->new(\@data_rev),
    ], {
        revalidate => 2,
    });

    $out->write($_) for 'a'..'e';
    sleep 1;
    $out->write($_) for 'f'..'h';
    sleep 2;
    $out->write($_) for 'i'..'z';

    like(join('', @data), qr/^abcdefgh/, 'first 8 items went to good stream, rev stream was broken');
    is(scalar(@data_rev), 9, 'remaining items were distributed proportionally');
    is(scalar(@data), 8 + 9);
}

sub targets :Test(2) {
    my @t = map { code_out(sub {}) } 1..3;
    my $out = Stream::Out::Any->new([@t], { shuffle => 0 });

    is
        scalar($out->targets),
        3,
        'targets returns the correct number of streams';

    is(
        ($out->targets)[1],
        $t[1],
        'targets returns the correct objects');
}

__PACKAGE__->new->runtests;
