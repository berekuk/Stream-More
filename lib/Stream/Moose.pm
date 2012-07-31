package Stream::Moose;

use strict;
use warnings;

# ABSTRACT: load stream traits

=head1 SYNOPSIS

    use Moose;
    use Stream::Moose;

    has 'in' => (
        is => 'ro',
        traits => ['Stream::In'],
        default => 'stdin',
    );

    __PACKAGE__->new->in; # stdin input stream from catalog

=head1 DESCRIPTION

This class defined L<Moose> traits C<Stream::In> and C<Stream::Out>.

These traits:

=over

=item *

Add type constraint

=item *

Enable coercion from string via stream catalog

=item *

Add C<Getopt> from L<MooseX::Getopt>

=back

=cut

use Stream::Moose::Trait::In;
use Stream::Moose::Trait::Out;

1;
