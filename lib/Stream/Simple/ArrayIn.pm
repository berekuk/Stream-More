package Stream::Simple::ArrayIn;

use strict;
use warnings;

use Params::Validate qw(:all);
use parent qw(Stream::In Stream::Mixin::Shift);

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

1;
