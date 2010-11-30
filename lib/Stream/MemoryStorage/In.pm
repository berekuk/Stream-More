package Stream::MemoryStorage::In;

use strict;
use warnings;

use namespace::autoclean;
use Params::Validate qw(:all);
use parent qw(Stream::In);

sub new {
    my $class = shift;
    my $self = validate(@_, {
        storage => 1,
        client => 1,
    });
    $self->{pos} = $self->{storage}->_get_client_pos($self->{client});
    return bless $self => $class;
}

sub read {
    my $self = shift;
    return $self->{storage}->_read($self->{pos}++);
}

sub commit {
    my $self = shift;
    $self->{storage}->_set_client_pos($self->{client}, $self->{pos});
}

sub DESTROY {
    my $self = shift;
    $self->{storage}->_unlock_client($self->{client});
}

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;
