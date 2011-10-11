package Stream::RoundRobin::Util;

use strict;
use warnings;

# ABSTRACT: utilities for Stream::RoundRobin class family

use parent qw(Exporter);
our @EXPORT = qw( check_cross );

use Params::Validate;
use Carp qw(confess);

=head1 FUNCTIONS

=over

=item B<< check_cross(left => $number, right => $number, size => $number, positions => $hashref) >>

Check that any of positions cross the interval from left to right.

=cut
sub check_cross {
    my $params = validate(@_, {
        left => 1,
        right => 1,
        size => 1,
        positions => 1, # name -> int value
    });
    my ($left, $right, $size, $positions) = @$params{qw/ left right size positions /};

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
=back

=cut

1;
