package Stream::Moose::Storage::AutoregisterClients;

# ABSTRACT: role for storages supporting named clients which want to register them automatically

use namespace::autoclean;

use Moose::Role;
with 'Stream::Moose::Storage::ClientList';

use Carp;

has 'autoregister' => (
    is => 'ro',
    isa => 'Bool',
    default => 1, # TODO - set via parameterized role?
    documentation => 'If true, automatically register client at first C<in()> call',
);

before 'in' => sub {
    my $self = shift;
    my $client = shift;
    unless ($self->has_client($client)) {
        unless ($self->autoregister) {
            croak "Client $client not found and autoregister is disabled";
        } 
        if ($self->read_only) {
            croak "Client $client not found and storage is read only";
        }
        $self->register_client($client);
    }
};

1;
