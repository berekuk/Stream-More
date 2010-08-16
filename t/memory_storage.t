#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use lib 'lib';
use lib 't/lib';
use Test::Class;
use Stream::Test::Out;
use Stream::Test::StorageWithClients;
use Stream::Test::StorageRW;

use Stream::MemoryStorage;

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
);

