package Stream::Simple::ArrayIn;

# ABSTRACT: simple on-memory input stream from arrayref

use strict;
use warnings;

use Params::Validate qw(:all);
use List::Util qw( sum );
use parent qw(
    Stream::In
    Stream::In::Role::Lag
    Stream::Mixin::Shift
);

sub new {
    my $class = shift;
    my ($list) = validate_pos(@_, { type => ARRAYREF });
    return bless [$list] => $class;
}

sub read {
    return shift @{$_[0][0]};
}

sub read_chunk {
    my @c = splice @{$_[0][0]}, 0, $_[1] or return;
    return \@c;
}

sub lag {
    my $self = shift;
    return scalar @{ $self->[0] }; # lag in items, not in bytes!
}

1;
