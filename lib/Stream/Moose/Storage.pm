package Stream::Moose::Storage;

# ABSTRACT: role for stream storage classes

use Moo::Role;

use Stream::Moose::FakeIsa;
with 'Stream::Moose::Out', FakeIsa('Stream::Storage');

requires 'in';

# please use $storage->in(...) instead of $storage->stream(...)
sub stream {
    my $self = shift;
    $self->in(@_);
}

1;
