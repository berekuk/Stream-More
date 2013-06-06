package Stream::Moose::In::Lag;

# ABSTRACT: lag role for input streams

use UNIVERSAL::DOES;

use Moo::Role;
with 'Stream::Moose::In';

use namespace::clean;

around DOES => sub {
    my ($orig, $self) = (shift, shift);
    my ($class) = @_;

    return 1 if $class eq 'Stream::In::Role::Lag';
    return $self->$orig(@_);
};

requires 'lag';

1;
