package Stream::Moose::Out::Chunked;

# ABSTRACT: role for output streams which want to implement write() with write_chunk()

use Moose::Role;
with 'Stream::Moose::Out';

sub write {
    my $self = shift;
    my $data = shift;
    $self->write_chunk([$data], @_);
}

1;
