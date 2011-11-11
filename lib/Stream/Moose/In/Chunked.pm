package Stream::Moose::In::Chunked;

# ABSTRACT: role for output streams which want to implement read() with read_chunk()

use Moose::Role;
with 'Stream::Moose::In';

sub read {
    my $self = shift;
    my $chunk = $self->read_chunk(1);
    return unless $chunk;
    return unless scalar @$chunk;
    confess "read_chunk returned too big chunk" if @$chunk > 1;
    return $chunk->[0];
}

1;
