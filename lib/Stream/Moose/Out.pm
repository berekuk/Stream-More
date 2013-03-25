package Stream::Moose::Out;

# ABSTRACT: role for output stream classes

=head1 DESCRIPTION

Moose-based output streams should implement this role.

=cut

use Moo::Role;

use Stream::Moose::FakeIsa;
with FakeIsa('Stream::Out');

requires 'write', 'write_chunk', 'commit';

=head1 SEE ALSO

L<Stream::Out> - base API description lives there by now.

=cut

1;
