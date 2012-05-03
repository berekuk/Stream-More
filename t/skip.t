#!/usr/bin/perl

use strict;
use warnings;

use lib 't/lib';
use lib 'lib';

use Stream::Skip;

use Test::More tests => 4;
use Test::Exception;

use Stream::Simple qw(code_in array_in code_out);
use Stream::Utils qw/process/;
    
my $skip = sub ($) {
    my $int = shift;
    return Stream::Skip->new(array_in([split //, '1'x80 ]), { max_data_size => 40, lag_check_interval => $int });
};

ok($skip->()->isa('Stream::In'), 'Skip is a stream IN');

{
    my @res = ();
    my $in = $skip->(1);
    process($in => code_out( sub { my $item = shift; push @res, $item; }), { chunk_size => 1, commit_step => 1});

    is(scalar @res, $in->{max_lag} - 1, scalar @res . " items processed");
}

{
    my @res = ();
    my $in = $skip->(5);
    process($in => code_out( sub { my $item = shift; push @res, $item; }), { chunk_size => 7, commit_step => 1});

    is(scalar @res, 31, scalar @res . " items processed");
}

{
    my @res = ();
    my $in = $skip->(5);
    process($in => code_out( sub { my $item = shift; push @res, $item; }), { chunk_size => 7, commit_step => 9});

    is(scalar @res, 31, scalar @res . " items processed");
}
