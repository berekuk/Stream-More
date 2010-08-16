package Stream::Concat::In;

use strict;
use warnings;

=head1 NAME

Stream::Concat::In - composite input stream

=cut

use parent qw(Stream::In);

use Params::Validate;

sub new {
    my $class = shift;
    my ($old, $new) = validate_pos(@_, { isa => 'Stream::In' }, { isa => 'Stream::In' });
    return bless {
        old => $old,
        new => $new,
    } => $class;
}

sub read {
    my $self = shift;
    unless ($self->{old_depleted}) {
        my $item = $self->{old}->read;
        return $item if defined $item;
        $self->{old_depleted} = 1;
    }
    return $self->{new}->read;
}

sub read_chunk {
    my $self = shift;
    unless ($self->{old_depleted}) {
        my $chunk = $self->{old}->read_chunk(@_);
        return $chunk if defined $chunk;
        $self->{old_depleted} = 1;
    }
    return $self->{new}->read_chunk(@_);
}

sub commit {
    my $self = shift;
    $self->{old}->commit;
    $self->{new}->commit;
}

=head1 SEE ALSO

You are probably looking for L<Stream::Concat::Storage>.

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

