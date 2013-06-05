package Stream::Moose::In;

# ABSTRACT: role for stream storage classes

=head1 DESCRIPTION

Moose-based input streams should implement this role.

=cut

use Moo::Role;
use Stream::Moose::FakeIsa;
with FakeIsa('Stream::In');

use namespace::clean;

requires 'read', 'read_chunk', 'commit';

=head1 SEE ALSO

L<Stream::In> - base API description lives there by now.

=cut

1;
