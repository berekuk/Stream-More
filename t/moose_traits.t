#!/usr/bin/perl

use strict;
use warnings;

use lib 'lib';
use Test::More 0.95;
use Yandex::X;
use Capture::Tiny qw/ capture /;

local $ENV{PERL5LIB} = 'lib';

subtest 'run script' => sub {
    my ($stdout, $stderr) = capture {
        system('echo blah | t/bin/moose_traits.pl');
    };
    like $stdout, qr/in: Stream::Catalog::In::stdin/;
    like $stdout, qr/blah/;
    is $stderr, '';
};

subtest 'run script with options' => sub {
    my ($stdout, $stderr) = capture {
        system('echo blah | t/bin/moose_traits.pl --out null');
    };
    like $stdout, qr/in: Stream::Catalog::In::stdin/;
    unlike $stdout, qr/blah/;
    is $stderr, '';
};

done_testing;
