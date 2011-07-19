#!/usr/bin/perl

use strict;
use warnings;

use Test::More;

use lib 'lib';

eval "use Test::Pod::Coverage";
plan skip_all => "Test::Pod::Coverage required for testing POD coverage" if $@;

my @modules = grep { $_ !~ /^Stream::Test/ } all_modules();
plan tests => scalar @modules;

for (@modules) {
    pod_coverage_ok($_, {
        coverage_class => 'Pod::Coverage::CountParents',
        also_private => [ qr/^BUILD$/ ],
    });
}

