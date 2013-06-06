package Stream::Moose::In::Chunked;

# ABSTRACT: role for output streams which want to implement read() with read_chunk()

use Moo::Role;
with 'Stream::Moose::In';

use Carp qw(confess);
use namespace::clean;

sub read {
    my $self = shift;
    my $chunk = $self->read_chunk(1);
    return undef unless $chunk;
    return undef unless scalar @$chunk;
    confess "read_chunk returned too big chunk" if @$chunk > 1;
    return $chunk->[0];
}

1;
