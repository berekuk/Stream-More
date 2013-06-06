package Stream::Moose::Storage::Occupancy;

# ABSTRACT: role for storages that are able to measure their occupancy

use Moo::Role;
with 'Stream::Moose::Storage';

use namespace::clean;

around DOES => sub {
    my ($orig, $self) = (shift, shift);
    my ($class) = @_;

    return 1 if $class eq 'Stream::Storage::Role::Occupancy';
    return $self->$orig(@_);
};

=over

=item B<occupancy>

    Calculate storage occupancy

=cut

requires 'occupancy';

=back

=cut

1;
