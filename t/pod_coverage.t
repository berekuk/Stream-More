#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use lib 'lib';

eval "use Test::Pod::Coverage";
plan skip_all => "Test::Pod::Coverage required for testing POD coverage" if $@;

my @modules = grep {
    $_ !~ /^Stream::Test/
    && $_ !~ /^Stream::RoundRobin/
    && $_ !~ /^Stream::Moose/
    # Pod::Coverage doesn't work well with moose roles.
    # Pod::Coverage::Moose solves this problem but doesn't solve the problems which Pod::Coverage::CoundParents solves.
    # Someone should mash-up them together...
    && $_ !~ /^Stream::Queue$/
} all_modules();
plan tests => scalar @modules;

for (@modules) {
    pod_coverage_ok($_, {
        coverage_class => 'Pod::Coverage::CountParents',
        also_private => [ qr/^BUILD$/ ],
    });
}

