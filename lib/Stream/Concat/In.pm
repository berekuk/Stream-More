package Stream::Concat::In;

use strict;
use warnings;

=head1 NAME

Stream::Concat::In - concatenate several input streams

=cut

use parent qw(Stream::In);

use Params::Validate;
use Carp;

sub new {
    my $class = shift;

    my @in = @_;
    croak "At least one input stream must be specified" unless @in;
    for (@in) {
        croak "Invalid argument '$_'" unless $_->isa('Stream::In');
    }

    return bless {
        in => \@in,
        current => 0,
    } => $class;
}

sub read {
    my $self = shift;

    while (1) {
        return if $self->{current} >= @{ $self->{in} };
        my $item = $self->{in}[ $self->{current} ]->read;
        return $item if defined $item;
        $self->{current}++;
    }
}

sub read_chunk {
    my $self = shift;

    while (1) {
        return if $self->{current} >= @{ $self->{in} };
        my $chunk = $self->{in}[ $self->{current} ]->read_chunk(@_);
        return $chunk if defined $chunk;
        $self->{current}++;
    }
}

sub commit {
    my $self = shift;
    $_->commit for @{ $self->{in} };
}

=head1 SEE ALSO

L<Stream::Concat>

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;
