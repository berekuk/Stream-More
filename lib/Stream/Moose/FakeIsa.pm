package Stream::Moose::FakeIsa;

# ABSTRACT: add fake classes to moose-based class isa method

# there is nothing stream-specific in this package
# remember that you have to add the following code to the class consuming this role:
# sub isa { $_[0]->next::method if $_[0]->next::can }

use Package::Variant
    importing => ['Moo::Role'],
    subs => [qw( around )];

sub make_variant {
    my ($class, $target_package, @extra) = @_;

    around 'isa' => sub {
        my ($orig, $self) = (shift, shift);
        my $isa = $_[0];
        for my $extra_isa (@extra) {
            return 1 if $isa eq $extra_isa;
        }
        return $self->$orig(@_);
    };
};

1;
