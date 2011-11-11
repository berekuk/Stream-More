package Stream::Moose::Storage;

# ABSTRACT: role for stream storage classes

use Moose::Role;
with
    'Stream::Moose::Out',
    'Stream::Moose::FakeIsa' => { extra => ['Stream::Storage'] },
;

requires 'in';

# please use $storage->in(...) instead of $storage->stream(...)
sub stream {
    my $self = shift;
    $self->in(@_);
}

1;
