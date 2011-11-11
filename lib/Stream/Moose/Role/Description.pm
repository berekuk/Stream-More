package Stream::Moose::Role::Description;

# ABSTRACT: role for streams which can report their detailed descriptions

use Moose::Role;

=head1 METHODS

=over

=item B<< description() >>

Returns object description, in free-form plain text.

=cut
requires 'description';

=back

=cut

1;
