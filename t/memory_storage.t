#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use lib 'lib';
use lib 't/lib';
use Test::Class;
use Stream::Test::Storage;
use Stream::Test::StorageWithClients;
use Stream::Test::StorageRW;

use Stream::MemoryStorage;

my $basic_test = Stream::Test::Storage->new(sub {
    Stream::MemoryStorage->new()
});

my $client_test = Stream::Test::StorageWithClients->new(sub {
    Stream::MemoryStorage->new()
});

my $client_id = 0;
my $rw_test = Stream::Test::StorageRW->new(sub {
    Stream::MemoryStorage->new()
}, sub {
    $client_id++;
    shift()->stream("client$client_id");
});

Test::Class->runtests(
    $basic_test,
    $client_test,
    $rw_test,
);

