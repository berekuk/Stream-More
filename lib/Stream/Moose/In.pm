package Stream::Moose::In;

# ABSTRACT: role for stream storage classes

=head1 DESCRIPTION

Moose-based input streams should implement this role.

=cut

use Moose::Role;
with 'Stream::Moose::FakeIsa' => { extra => ['Stream::In'] };

requires 'read', 'read_chunk', 'commit';

=head1 SEE ALSO

L<Stream::In> - base API description lives there by now.

=cut

1;
