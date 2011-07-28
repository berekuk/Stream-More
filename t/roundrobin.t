#!/usr/bin/perl

use strict;
use warnings;

use lib 'lib';

use Test::More;
use Test::Fatal;
use parent qw(Test::Class);

use Stream::RoundRobin;
use PPB::Test::TFiles;

use Perl6::Slurp;

sub setup :Test(setup) {
    PPB::Test::TFiles->import;
}

sub make_storage {
    my $self = shift;
    return Stream::RoundRobin->new(dir => 'tfiles/a', data_size => 10 * 1024 * 1024);
}

sub dir_option :Test(1) {
    my $self = shift;
    my $storage = Stream::RoundRobin->new(dir => 'tfiles/blahblah', data_size => 1 * 1024 * 1024);
    ok(-d 'tfiles/blahblah', 'create storage with given dir');
}

sub size_option :Test(1) {
    my $self = shift;
    my $storage = Stream::RoundRobin->new(dir => 'tfiles/a', data_size => 123456);
    is(-s 'tfiles/a/data', 123456, 'create data file with given size');
}

sub isa_storage :Test(1) {
    my $self = shift;
    ok($self->make_storage->isa('Stream::Storage'), 'roundrobin is a storage');
}

sub autocreate_dirs :Test(2) {
    my $self = shift;
    my $storage = $self->make_storage;
    ok(-d 'tfiles/a', 'create storage dir');
    ok(-d 'tfiles/a/clients', 'create clients dir');
}

sub write_and_check_data_file :Test(9) {
    my $self = shift;
    my $storage = Stream::RoundRobin->new(dir => 'tfiles/a', data_size => 10);
    $storage->write("abc\n");
    $storage->commit;

    is(slurp('tfiles/a/data'), "abc\n\0\0\0\0\0\0", 'state after first write');
    is($storage->position, 4, 'position after first write');

    $storage->write("e\n");
    $storage->write("f\n");
    $storage->commit;

    is(slurp('tfiles/a/data'), "abc\ne\nf\n\0\0", 'state after second write');
    is($storage->position, 8, 'position after second write');

    $storage->write("1234\n");
    $storage->write("5\n");
    $storage->commit;

    is(slurp('tfiles/a/data'), "34\n5\n\nf\n12", 'state after write with wrap');
    is($storage->position, 5, 'position after write with wrap');

    $storage->write("aaaaaaaaaaaa\n");
    like(exception { $storage->commit }, qr/buffer is too large/);

    is(slurp('tfiles/a/data'), "34\n5\n\nf\n12", 'state not affected after write with overflow');
    is($storage->position, 5, 'position not affected after write with overflow');
}

{
    my @tests;

    my $storage_gen = sub {
        PPB::Test::TFiles->import;
        return Stream::RoundRobin->new( dir => 'tfiles' );
    };

    use Test::Class::Load qw(lib/Stream/Test);

    unless ($ENV{TEST_CUSTOM_ONLY}) {
        push @tests, Stream::Test::Out->new($storage_gen);
        push @tests, Stream::Test::StorageWithClients->new($storage_gen);
        push @tests, Stream::Test::StorageRW->new($storage_gen);
    }

    push @tests, __PACKAGE__->new;

    Test::Class->runtests(@tests);
}
