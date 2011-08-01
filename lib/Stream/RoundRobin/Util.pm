package Stream::RoundRobin::Util;

use strict;
use warnings;

use parent qw(Exporter);
our @EXPORT = qw( check_cross );

use MooseX::Params::Validate;
use Carp qw(confess);

sub check_cross {
    my ($left, $right, $size, $positions) = validated_list(
        \@_,
        left => { isa => 'Int' },
        right => { isa => 'Int' },
        size => { isa => 'Int' },
        positions => { isa => 'HashRef[Int]' }, # name -> int value
    );

    if ($right > $size) {
        confess "right side is too big ($right)";
    }
    if ($left > $size) {
        confess "left side is too big ($left)";
    }
    if ($left < $right) {
        # easy
        for my $name (keys %$positions) {
            my $position = $positions->{$name};
            if (
                $position > $left and $position <= $right
                or $position == 0 and $right == $size
            ) {
                confess "crossed $name position ($position)"
            }
        }
    }
    else {
        check_cross(left => $left, right => $size, size => $size, positions => $positions);
        check_cross(left => 0, right => $right, size => $size, positions => $positions);
    }

}

1;
