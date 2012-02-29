package Stream::Moose::Storage::ClientList;

# ABSTRACT: role for storages with named managed clients

use Moose::Role;
with 'Stream::Moose::Storage';

use Class::DOES::Moose;
extra_does 'Stream::Storage::Role::ClientList';

requires
    'client_names',
    'register_client',
    'unregister_client',
;

sub has_client($$) {
    my $self = shift;
    my $name = shift;
    return grep { $_ eq $name } $self->client_names;
}

1;
