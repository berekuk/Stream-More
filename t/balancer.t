use strict;
BEGIN {
    my $step = 1;
    my $time = 0;
    *CORE::GLOBAL::time = sub { $time += $step; return $time; };

    sub set_step {
        $step = shift;
    }
}

use parent qw(Test::Class);
use Test::More;
use Test::Exception;
use Time::HiRes qw/sleep/;

use lib 'lib';

use Stream::Out::Multiplex;
use Stream::Out::Balancer;
use Stream::Simple qw(array_in code_out);
use Streams;

use Test::MockObject;
use Data::Dumper;

sub mock_obj {
    my $mock = Test::MockObject->new();
    $mock->{buffer} = 0;

    $mock->mock('occupancy' => sub { my $self = shift; return $self->{buffer}; } );
    $mock->mock('write'        => sub { my $self = shift; $self->{buffer}++ } );
    $mock->mock('write_chunk'  => sub { my ($self, $chunk) = @_; $self->{buffer} += scalar @$chunk; });
    $mock->mock('commit'       => sub { return 1; });
    $mock->set_isa('Stream::Propagate', 'Stream::Out');
    return $mock;
}

sub initialization: Test(2) {
    eval {
        Stream::Out::Balancer->new([{}]);
    };
    like($@, qr/targets are stream-propagates/, 'check isa');

    eval {
        Stream::Out::Balancer->new([]);
    };
    like($@, qr/at least one target given/, 'check empty targets');
}

sub deviation {
    my $targets = shift;

    my $avg = 0;
    $avg += $_->occupancy() for (@$targets);
    $avg /= scalar @$targets;

    my $sq_avg = 0;
    $sq_avg += ($_->occupancy() - $avg)**2 for (@$targets);
    $sq_avg /= scalar @$targets;

    return sqrt($sq_avg);
}

sub balance: Tests {
    set_step(100);

    my $rand_dev = 0;
    my $normal_dev = 0;
    {
        my $targets_count = 100;
        my $targets = [map {mock_obj()} (1..$targets_count)];

        my $is_done = 0;
        for ( 1 .. 20 ) {
            $targets->[$_]->{buffer} = 1_000_000;
            $targets->[$_]->mock( write_chunk => sub { $is_done++; } );
        }

        my $balancer_rand = Stream::Out::Balancer->new($targets, { normal_distribution => 0 });

        process(array_in([0..1500]) => $balancer_rand, {chunk_size => 1});
        is($is_done, 0, 'no writes in bad 20% - rand ');

        $rand_dev = deviation([ grep { $_->occupancy() < 1_000_000 } @$targets ]);
        print "RAND deviation: $rand_dev\n";
    }

    {
        my $targets_count = 100;
        my $targets = [map {mock_obj()} (0..$targets_count)];

        my $is_done = 0;
        for ( 1 .. 20 ) {
            $targets->[$_]->{buffer} = 1_000_000;
            $targets->[$_]->mock( write_chunk => sub { $is_done++; } );
        }

        my $balancer_norm = Stream::Out::Balancer->new($targets, { normal_distribution => 1 });

        process(array_in([0..1500]) => $balancer_norm, {chunk_size => 1});
        is($is_done, 0, 'no writes in bad 20% - norm ');

        $normal_dev = deviation([ grep { $_->occupancy() < 1_000_000 } @$targets ]);
        print "NORM deviation: $normal_dev\n";
    }

    cmp_ok($normal_dev, '<', $rand_dev, 'NORM deviation lesser than RAND');
}

sub invalid: Tests {
    set_step(5);

    my $targets_count = 100;
    my $targets = [map {mock_obj()} (0..$targets_count)];
    my $balancer_norm = Stream::Out::Balancer->new($targets);
    my $balancer_rand = Stream::Out::Balancer->new($targets, { normal_distribution => 0 });

    my $is_invalid = 0;
    my $is_recovered = 0;
    $targets->[0]->mock( write_chunk => sub { unless ($is_invalid) { $is_invalid = 1; die } else { $is_recovered = 1; } } );

    process(array_in([0..10000]) => $balancer_rand, {chunk_size => 5});
    is( $is_invalid + $is_recovered, 2, 'invalid item set and recovered - rand ' );

    $is_invalid = 0;
    $is_recovered = 0;
    process(array_in([0..10000]) => $balancer_norm, {chunk_size => 5});
    is( $is_invalid + $is_recovered, 2, 'invalid item set and recovered - norm ' );
}


__PACKAGE__->new->runtests;
