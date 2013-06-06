package Stream::Moose::Storage::ClientList;

# ABSTRACT: role for storages with named managed clients

use UNIVERSAL::DOES;

use Moo::Role;
with 'Stream::Moose::Storage';

around DOES => sub {
    my ($orig, $self) = (shift, shift);
    my ($class) = @_;

    return 1 if $class eq 'Stream::Storage::Role::ClientList';
    return $self->$orig(@_);
};

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
