package Stream::Moose::Out::Chunked;

# ABSTRACT: role for output streams which want to implement write() with write_chunk()

use Moo::Role;
with 'Stream::Moose::Out';

use namespace::clean;

sub write {
    my $self = shift;
    my $data = shift;
    $self->write_chunk([$data], @_);
}

1;
