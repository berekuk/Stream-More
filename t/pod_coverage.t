#!/usr/bin/perl

use strict;
use warnings;

use Test::More 0.95;

use lib 'lib';

eval "use Test::Pod::Coverage";
plan skip_all => "Test::Pod::Coverage required for testing POD coverage" if $@;

sub coverage_class {
    local $_ = shift;

    return if $ENV{TEST_MODULE} and $_ ne $ENV{TEST_MODULE};
    return if /^Stream::Test/;
    return if /^Stream::Queue::BigChunk/;

    return 'Moose' if /^Stream::RoundRobin/
        or /^Stream::Moose/
        or /^Stream::Queue/
        or /^Stream::Concat/
        or /^Stream::Buffer/
        or /^Stream::In::DiskBuffer::Chunk/
        or /^Stream::Filter::Coro/;

    return 'CountParents';
    # Pod::Coverage doesn't work well with moose roles.
    # Pod::Coverage::Moose solves this problem but doesn't solve the problems which Pod::Coverage::CoundParents solves.
    # Someone should mash-up them together...
}

my %class_options = (
    'Moose' => { cover_requires => 1 },
);

for my $module (all_modules()) {
    my $class = coverage_class($module);
    next unless $class;

    my $options = $class_options{$class} || {};
    pod_coverage_ok($module, {
        coverage_class => "Pod::Coverage::$class",
        %$options,
        also_private => [ qr/^BUILD|DOES|DEMOLISH$/ ],
    });
}

done_testing;
