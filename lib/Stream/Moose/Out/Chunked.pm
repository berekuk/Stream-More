package Stream::Moose::Out::Chunked;

use Moose::Role;
with 'Stream::Moose::Out';

sub write {
    my $self = shift;
    my $data = shift;
    $self->write_chunk([$data], @_);
}

1;
