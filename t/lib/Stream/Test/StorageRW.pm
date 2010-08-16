package Stream::Test::StorageRW;

use strict;
use warnings;

use namespace::autoclean;

use parent qw(Test::Class);
use Test::More;
use Params::Validate qw(:all);
use Yandex::X;

=head1 NAME

Stream::Test::StorageRW - test storage rw capabilities

=head1 DESCRIPTION

Since we want this class to be useful both for storages supporting named clients, and for storages supporting cursor-style input streams only, constructor arguments are pretty complex. Sorry.

=cut

sub new {
    my $class = shift;
    my ($storage_gen, $client_gen) = validate_pos(@_, { type => CODEREF }, { type => CODEREF } );
    my $self = $class->SUPER::new;
    $self->{storage_gen} = $storage_gen;
    $self->{client_gen} = $client_gen;
    return $self;
}

sub setup :Test(setup) {
    my $self = shift;
    $self->{storage} = $self->{storage_gen}->();
}

sub teardown :Test(teardown) {
    my $self = shift;
    delete $self->{storage};
}

sub storage {
    my $self = shift;
    return $self->{storage};
}

sub new_client {
    my $self = shift;
    return $self->{client_gen}->($self->storage);
}

sub simple_read_write :Test(6) {
    my $self = shift;
    my $storage = $self->{storage};
    $storage->write(123);
    $storage->write('abc');
    $storage->commit;

    {
        my $client = $self->new_client;
        is($client->read, 123);
        is($client->read, 'abc');
        is($client->read, undef);
        $client->commit;
    }

    {
        my $client = $self->new_client;
        is($client->read, 123);
        $client->commit;
        is($client->read, 'abc');
        is($client->read, undef);
    }
}

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

