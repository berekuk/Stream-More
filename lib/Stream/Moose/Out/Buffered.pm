package Stream::Moose::Out::Buffered;

# ABSTRACT: helper for buffered outs

=head1 SYNOPSIS

    package MyOut;

    use Moose;
    with 'Stream::Moose::Out::Buffered';

    sub write_chunk {
        my $self = shift;
        my ($lines) = @_;
    }

=cut

use Moose::Role;
with 'Stream::Moose::Out::Chunked';

has 'buffer_size' => (
    is => 'ro',
    isa => 'Int',
    default => 100,
);

has '_buffer' => (
    is => 'rw',
    default => sub { [] },
);

has '_bufferize' => (
    is => 'rw',
    default => 1,
);

around 'write_chunk' => sub {
    my $orig = shift;
    my $self = shift;
    my ($chunk) = @_;

    push @{ $self->_buffer }, @$chunk;
    return if ($self->_bufferize && (scalar @{ $self->_buffer }) < $self->buffer_size);

    $self->$orig( $self->_buffer );
    $self->_buffer([]);
};

before 'commit' => sub {
    my $self = shift;
    if ( @{ $self->_buffer } ) {
        $self->_bufferize(0);
        $self->write_chunk([]);
        $self->_buffer([]);
    }
};

requires 'write_chunk';

1;

=head1 SEE ALSO

L<Stream::Moose::Out::Chunked>

=cut
