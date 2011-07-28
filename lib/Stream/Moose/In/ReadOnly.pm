package Stream::Moose::In::ReadOnly;

use Moose::Role;

requires 'read_only';

before [qw( commit )] => sub {
    my ($orig, $self) = @_;
    croak "Input stream $self is read only" if $self->read_only;
};

1;
