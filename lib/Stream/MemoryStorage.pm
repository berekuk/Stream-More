package Stream::MemoryStorage;

# ABSTRACT: in-memory storage with support for named clients

use strict;
use warnings;

use parent qw(Stream::Storage);
use Streams 0.9.0 qw();
use parent qw(Stream::Storage::Role::ClientList);

use namespace::autoclean;
use Carp;
use Params::Validate qw(:all);
use Stream::MemoryStorage::In;
use List::Util qw(sum);

sub new {
    my ($class) = validate_pos(@_, 1);
    return bless {
        data => [],
        client_pos => {},
        client_lock => {},
    } => $class;
}

sub write {
    my $self = shift;
    my $item = shift;
    push @{ $self->{data} }, $item;
}

sub _read {
    my $self = shift;
    my $pos = shift;
    return $self->{data}[$pos];
}

sub _lag {
    my $self = shift;
    my ($pos) = @_;

    return sum(map { length } @{$self->{data}}[ $pos .. @{$self->{data}} - 1 ]) || 0;
}

sub _lock_client {
    my ($self, $client) = @_;
    my $lock = $self->{client_lock}{$client};
    if ($lock) {
        # already locked
        return 0;
    }
    $self->{client_lock}{$client} = 1;
    return 1;
}

sub _unlock_client {
    my ($self, $client) = @_;
    $self->{client_lock}{$client} = 0;
}

sub _get_client_pos {
    my $self = shift;
    my $client = shift;
    return $self->{client_pos}{$client} || 0;
}

sub _set_client_pos {
    my $self = shift;
    my $client = shift;
    my $pos = shift;
    $self->{client_pos}{$client} = $pos;
}

sub client_names {
    my $self = shift;
    return keys %{ $self->{client_pos} };
}

sub register_client {
    my $self = shift;
    my $name = shift;
    $self->{client_pos}{$name} = 0;
}

sub unregister_client {
    my $self = shift;
    my $name = shift;
    delete $self->{client_pos}{$name};
    delete $self->{client_lock}{$name};
}

sub in {
    my $self = shift;
    my ($client) = validate_pos(@_, { type => SCALAR });
    unless ($self->_lock_client($client)) {
        croak "Constructing two clients with same name '$client' for one MemoryStorage - not implemented yet";
    }
    return Stream::MemoryStorage::In->new({
        storage => $self,
        client => $client,
    });
}

# ->stream is deprecated, use ->in instead
sub stream {
    my $self = shift;
    return $self->in(@_);
}

1;
