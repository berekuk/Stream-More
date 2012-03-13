package Stream::Moose::Filter;

# ABSTRACT: role for stream filters

=head1 DESCRIPTION

Moose-based stream filters should implement this role.

=cut

use namespace::clean -except => 'meta';

use Moose::Role;
with 'Stream::Moose::FakeIsa' => { extra => ['Stream::Filter'] };

use Stream::Filter;
use MooseX::Role::WithOverloading;
use overload
    '|' => $Stream::Filter::{'(|'},
    '""' => sub { $_[0] },
;

requires 'write', 'write_chunk', 'commit';

=head1 SEE ALSO

L<Stream::Filter> - base API description lives there by now.

=cut

1;
