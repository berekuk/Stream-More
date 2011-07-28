package Stream::Moose::FakeIsa;

# there is nothing stream-specific in this package
# remember that you have to add the following code to the class consuming this role:
# sub isa { $_[0]->next::method if $_[0]->next::can }

use MooseX::Role::Parameterized;

parameter 'extra' => (
    is => 'ro',
    isa => 'ArrayRef[Str]',
    required => 1,
);

use Class::C3;

role {
    my $p = shift;

    around 'isa' => sub {
        my ($orig, $self) = (shift, shift);
        my $class = $_[0];
        for my $extra_isa (@{ $p->extra }) {
            return 1 if $class eq $extra_isa;
        }
        return $self->$orig(@_);
    };
};
