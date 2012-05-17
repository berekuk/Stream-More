package Stream::Moose::Trait::Out;

use namespace::autoclean;
use Moose::Role;
with 'MooseX::Getopt::Meta::Attribute::Trait';
use Moose::Util::TypeConstraints;
use MooseX::Getopt::OptionTypeMap;

use Streams qw(catalog);

around should_coerce => sub {
    my $orig = shift;
    my $self = shift;

    my $current_val = $self->$orig(@_);

    return $current_val if defined $current_val;

    return 1;
};

my $type = subtype 'Stream::Moose::Type::Out'
    => as 'Object'
    => where { $_->DOES('Stream::Out') };

coerce $type
    => from 'Str'
    => via { catalog->out($_) };

override 'has_type_constraint' => sub {
    1;
};

override 'type_constraint' => sub {
    $type
};

MooseX::Getopt::OptionTypeMap->add_option_type_to_map(
    $type => '=s'
);

1;

package Moose::Meta::Attribute::Custom::Trait::Stream::Out;
sub register_implementation { 'Stream::Moose::Trait::Out' }

1;
