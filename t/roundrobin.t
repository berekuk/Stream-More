#!/usr/bin/perl

use strict;
use warnings;

use lib 'lib';

use Test::More;
use Test::Fatal;
use parent qw(Test::Class);

use namespace::autoclean;
use Try::Tiny;

use Stream::RoundRobin;
use Stream::Simple qw( array_in code_out );
use Streams qw( process );
use PPB::Test::TFiles;
use PPB::Progress;

use Perl6::Slurp;
use List::Util qw(min);

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

# we have one storage and 10 clients here
# we write random amounts of random data to storage and read it in random portions in a random order :)
# then we compare that all clients got the correct result in the end
sub stress :Test {
    my $self = shift;

    my @data;
    my @letter = ('a'..'z', 0 .. 9);
    my $LINE_NUMBER = $ENV{STRESS_LINE_NUMBER} || 10_000;
    if ($LINE_NUMBER < 100) {
        die "line number is too small, you'll get weird exceptions";
    }
    my $LINE_LENGTH = $ENV{STRESS_LINE_LENGTH} || 32;
    my $MIN_LINE_LENGTH = $ENV{STRESS_MIN_LINE_LENGTH} || 0;

    for (1 .. $LINE_NUMBER) {
        my $line = join '', map { $letter[ int rand scalar @letter ] } 1 .. int rand($LINE_LENGTH) + $MIN_LINE_LENGTH;
        $line .= "\n";
        push @data, $line;
    }

    my @results;
    my @outs;
    for my $i (0 .. 9) {
        push @outs, code_out { push @{ $results[$i] }, shift };
    }

    my $storage = Stream::RoundRobin->new(
        dir => 'tfiles',
        data_size => $LINE_LENGTH * $LINE_NUMBER / 10, # we want storage to be as small as possible, to test lots of wrap cases
    );
    $storage->register_client("client$_") for 0 .. 9;

    my $id = 0;
    my $progress = PPB::Progress->new(max => scalar @data);

    while (1) {
        my $success;
        try {
            # we don't use process(), because $storage->commit can fail, and it's hard to remove correct amount of data from array_in in this case
            my $portion_limit = 1 + int rand 100;
            while ($portion_limit > 0 and $id < @data) {
                $storage->write_chunk([ @data[ $id .. min($id + 9, $#data) ] ]);
                $storage->commit;
                $id += 10;
                $portion_limit -= 10;
                $progress->set($id);
            }
        }
        catch {
            # cross exceptions are ok
            unless (/crossed .* position/) {
                die $_;
            }
        };
        for (0 .. 9) {
            $success += process($storage->in("client$_") => $outs[$_], { limit => 1 + int rand 100, chunk_size => 10 });
        }

        # tmp intermediate check
        if ($ENV{STRESS_INTERMEDIATE_CHECK}) {
            for (0 .. 9) {
                my @expected = @data[ 0 .. (scalar(@{ $results[$_] }) - 1) ];
                my @got = @{ $results[$_] };
                chomp for @expected, @got;
                my $expected = join ',', @expected;
                my $got = join ',', @got;
                unless ($expected eq $got) {
                    warn "problem with client $_";
                    warn "expected: $expected";
                    warn "got: $got";
                    die;
                }
            }
        }
        last unless $success;
    }

    is_deeply(\@results, [ map { \@data } 0..9 ]);
}

{
    my @tests;

    my $storage_gen = sub {
        PPB::Test::TFiles->import;
        return __PACKAGE__->make_storage;
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
