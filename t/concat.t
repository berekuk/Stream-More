#!/usr/bin/perl

use strict;
use warnings;

use parent qw(Test::Class);
use Test::More;

use lib 'lib';
use lib 't/lib';
use Stream::Test::Storage;
use Stream::Test::StorageRW;
use Stream::Concat;
use Stream::MemoryStorage;

sub test_concat :Test(...) {
    my $old = Stream::MemoryStorage->new;
    my $new = Stream::MemoryStorage->new;

    $old->write('old1');
    $old->write('old2');
    my $old_client = $old->stream('abc');
    $old_client->read;
    $old_client->commit;

    $new->write('new1');
    $new->write('new2');

    my $concat = Stream::Concat->new(
        $old => $new
    );

    {
        my $concat_client = $concat->stream('abc');
        is($concat_client->read, 'old2');
        is($concat_client->read, 'new1');
        is($concat_client->read, 'new2');
        $concat_client->commit;
    }

    {
        my $concat_client = $concat->stream('blah');
        is($concat_client->read, 'old1');
        is($concat_client->read, 'old2');
        is($concat_client->read, 'new1');
        is($concat_client->read, 'new2');
        $concat_client->commit;
    }
}

my $common_test = Stream::Test::Storage->new(
    sub {
        Stream::Concat->new(
            Stream::MemoryStorage->new() => Stream::MemoryStorage->new()
        );
    },
);

my $i = 0;
my $rw_test = Stream::Test::StorageRW->new(
    sub {
        Stream::Concat->new(
            Stream::MemoryStorage->new() => Stream::MemoryStorage->new()
        );
    },
    sub {
        $i++;
        return shift()->stream("client$i");
    },
);

Test::Class->runtests(
    __PACKAGE__->new, $common_test, $rw_test,
);

