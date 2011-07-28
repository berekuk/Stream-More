#!/usr/bin/perl

use strict;
use warnings;

use lib 'lib';

use Test::More;
use parent qw(Test::Class);

use Stream::RoundRobin;
use PPB::Test::TFiles;

sub setup :Test(setup) {
    PPB::Test::TFiles->import;
}

sub make_storage {
    my $self = shift;
    return Stream::RoundRobin->new(dir => 'tfiles/a', size => 10 * 1024 * 1024);
}

sub dir_option :Test(1) {
    my $self = shift;
    my $storage = Stream::RoundRobin->new(dir => 'tfiles/blahblah', size => 1 * 1024 * 1024);
    ok(-d 'tfiles/blahblah', 'create storage with given dir');
}

sub size_option :Test(1) {
    my $self = shift;
    my $storage = Stream::RoundRobin->new(dir => 'tfiles/a', size => 123456);
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
