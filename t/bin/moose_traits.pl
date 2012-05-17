#!/usr/bin/env perl
package test::moose_traits;

use Moose;
with 'MooseX::Getopt::Strict';
use Stream::Moose;
use Streams qw(process);

has 'in' => (
    is => "ro",
    traits => ["Stream::In"],
    default => "stdin",
);

has 'out' => (
    is => "ro",
    traits => ["Stream::Out"],
    default => "stdout",
);

__PACKAGE__->meta->make_immutable;

if (caller) {
    return __PACKAGE__;
}
else {
    my $x = __PACKAGE__->new_with_options;
    print "in: ".$x->in."\n";
    process($x->in => $x->out);
}
