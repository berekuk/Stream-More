package Stream::Moose::Storage::AutoregisterClients;

# ABSTRACT: role for storages supporting named clients which want to register them automatically

use Moo::Role;
with 'Stream::Moose::Storage::ClientList';

use Types::Standard qw(Bool);
use Carp;

use namespace::clean;

has 'autoregister' => (
    is => 'ro',
    isa => Bool,
    default => sub { 1 }, # TODO - set via parameterized role?
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
