package Stream::Moose::Role::Description;

# ABSTRACT: role for streams which can report their detailed descriptions

use UNIVERSAL::DOES;

use Moo::Role;

around DOES => sub {
    my ($orig, $self) = (shift, shift);
    my ($class) = @_;

    if ($class eq 'Stream::Role::Description') {
        return 1;
    }

    return $self->$orig(@_);
};

=head1 METHODS

=over

=item B<< description() >>

Returns object description, in free-form plain text.

=cut
requires 'description';

=back

=cut

1;
