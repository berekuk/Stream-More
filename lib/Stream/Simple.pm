package Stream::Simple;

use strict;
use warnings;

=head1 NAME

Stream::Simple - simple procedural-style constructors of streams without any positions

=head1 SYNOPSIS

    use Stream::Simple;

    $stream = array_seq([5,6,7]);

=head1 DESCRIPTION

This package is an adaptation of PPB::Join::Sequence::* modules.

=head1 FUNCTIONS

=over

=cut

use base qw(Exporter);
our @EXPORT_OK = qw/ array_seq /;

use Params::Validate qw(:all);

=item B<array_seq($list)>

Creates stream which shifts items from specified list and returns them as stream values.

=cut
sub array_seq($) {
    my ($list) = validate_pos(@_, { type => ARRAYREF });
    return Stream::Simple::Array->new($list);
}

=back

=cut

package Stream::Simple::Array;

use strict;
use warnings;

use Params::Validate qw(:all);
use base qw(Stream::Stream Stream::Mixin::Shift);

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

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

