package Stream::RoundRobin::Types;

# ABSTRACT: moo-like types collection for Stream::RoundRobin

use strict;
use warnings;

use parent qw(Exporter);

our @EXPORT_OK = qw( ClientName );
our %EXPORT_TAGS = (
    'all' => \@EXPORT_OK
);

use MooseX::Types::Moose qw( Str );

sub ClientName {
    sub {
        $_[0] =~ /^[\w\.-]+$/ or die "Invalid client name '$_[0]'";
    }
}

1;
