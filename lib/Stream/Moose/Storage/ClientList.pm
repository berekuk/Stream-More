package Stream::Moose::Storage::ClientList;

use Moose::Role;
with
    'Stream::Moose::FakeIsa' => { extra => ['Stream::Storage::Role::ClientList'] },
    'Stream::Moose::Storage',
;

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
