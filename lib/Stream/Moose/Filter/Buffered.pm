package Stream::Moose::Filter::Buffered;

# ABSTRACT: helper for buffered filters

=head1 SYNOPSIS

    package MyFilter;

    use Moose;
    with 'Stream::Moose::Filter::Buffered';

    sub flush {
        my $self = shift;
        my ($lines) = @_;

        ... # process lines, return filtered result
    }

=cut

use Moose::Role;
with 'Stream::Moose::Filter';

has 'buffer_size' => (
    is => 'ro',
    isa => 'Int',
    default => 100,
);

has '_buffer' => (
    is => 'rw',
    default => sub { [] },
);

sub write {
    my $self = shift;
    return @{ $self->write_chunk([@_]) };
}

sub write_chunk {
    my $self = shift;
    my ($chunk) = @_;

    push @{ $self->_buffer }, @$chunk;
    return [] if (scalar @{ $self->_buffer }) < $self->buffer_size;
    return $self->_flush;
}

sub commit {
    my $self = shift;
    return @{ $self->_flush };
}

sub _flush {
    my $self = shift;
    my $chunk = $self->_buffer;
    $self->_buffer([]);

    return $self->flush($chunk);
}

requires 'flush';

1;

=head1 SEE ALSO

L<Stream::Moose::Filter>

=cut
