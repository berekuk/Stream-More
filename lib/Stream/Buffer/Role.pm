package Stream::Buffer::Role;

# ABSTRACT: role for any Stream::Buffer::* classes

use namespace::autoclean;
use Moose::Role;

=head1 METHODS

=over

=item B<save($chunk, $limit)>

Save C<@$chunk> items into a buffer, enumerate them and return up to C<$limit> of them back.

=cut
requires 'save';

=item B<load($limit)>

Load up to C<$limit> enumerated items from a buffer.

=cut
requires 'load';

=item B<delete($ids)>

Remove items identified by C<@$ids> from the buffer.

=cut
requires 'delete';

=item B<lag()>

Measure buffer size in bytes.

=cut
requires 'lag';

1;
