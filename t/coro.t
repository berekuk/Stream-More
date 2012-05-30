#!/usr/bin/perl

use strict;
use warnings;

use lib 'lib';
use parent qw(Test::Class);
use Test::More;
use Test::Fatal;

use Stream::Filter::Coro;
use Streams qw(filter process);
use Stream::Simple qw( array_in code_out );

use Coro::AnyEvent;
use Coro;

sub main :Tests {
    my $filter = filter {
        Coro::AnyEvent::sleep 1;
        return shift;
    };

    my $time = time;
    my $coro_filter = Stream::Filter::Coro->new(
        threads => 5,
        filter => sub { $filter },
    );

    my @result;
    push @result, $coro_filter->write($_) for 11 .. 15;
    push @result, $coro_filter->commit;

    is scalar @result, 5;
    is_deeply [ sort { $a <=> $b } @result ], [ 11 .. 15 ];
    cmp_ok time - $time, '<', 2;
}

sub error_handling :Tests {
    my $filter = filter {
        die "oops";
    };

    my $coro_filter = Stream::Filter::Coro->new(
        threads => 2,
        filter => sub { $filter },
    );

    like
        exception {
            $coro_filter->write(123);
            $coro_filter->commit;
        },
        qr/oops/;
}

sub multiple_commits :Tests {

    my $filter = filter {
        cede;
        return shift;
    };

    my $time = time;
    my $coro_filter = Stream::Filter::Coro->new(
        threads => 5,
        filter => sub { $filter },
    );

    my @result;
    for my $data ([ 1 .. 10 ], [ 11 .. 20 ]) {
        process(array_in($data) => $coro_filter | code_out { push @result, shift })
    }
    is scalar @result, 20;
    is_deeply [ sort { $a <=> $b } @result ], [ 1 .. 20 ];
}

__PACKAGE__->new->runtests;
