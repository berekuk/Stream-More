#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use lib 'lib';
use lib 't/lib';
use parent qw(Test::Class);
use Stream::Test::Out;
use Stream::Test::StorageWithClients;
use Stream::Test::StorageRW;

use Stream::MemoryStorage;

sub check_lag :Tests {
    my $self = shift;

    my $ms = Stream::MemoryStorage->new();
    my $in = $ms->in('blah');
    $ms->write('foo');
    $ms->write('bar');
    $ms->write('bar2');
    is $in->lag, 10;
    $in->read;
    is $in->lag, 7;
    $in->read;
    $in->read;
    $in->read;
    is $in->lag, 0;
}

my $basic_test = Stream::Test::Out->new(sub {
    Stream::MemoryStorage->new()
});

my $client_test = Stream::Test::StorageWithClients->new(sub {
    Stream::MemoryStorage->new()
});

my $rw_test = Stream::Test::StorageRW->new(sub {
    Stream::MemoryStorage->new()
});

Test::Class->runtests(
    $basic_test,
    $client_test,
    $rw_test,
    __PACKAGE__->new,
);

