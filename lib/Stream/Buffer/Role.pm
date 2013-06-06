package Stream::Buffer::Role;

# ABSTRACT: role for any Stream::Buffer::* classes

use Moo::Role;

use namespace::clean;

=head1 METHODS

=over

=item B<save($chunk)>

Save C<@$chunk> items into a buffer.

For example:

    $buffer->save([ "abc", "def" ]);

=cut
requires 'save';

=item B<load($limit)>

Load up to C<$limit> enumerated items from a buffer.

For example:

    $buffer->load(2); # returns [ [5, "abc"], [6, "def"] ]

=cut
requires 'load';

=item B<delete($ids)>

Remove items identified by C<@$ids> from the buffer.

For example:

    $buffer->delete([5,6]);

=cut
requires 'delete';

=item B<lag()>

Measure buffer size in bytes.

=cut
requires 'lag';

=back
=cut

1;
