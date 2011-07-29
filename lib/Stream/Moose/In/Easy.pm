package Stream::Moose::In::Easy;

use Moose::Role;
with 'Stream::Moose::In';

sub read_chunk($$) {
    my ($self, $limit) = @_;
    my @chunk;
    while (defined($_ = $self->read)) {
        push @chunk, $_;
        last if @chunk >= $limit;
    }
    return unless @chunk; # return false if nothing can be read
    return \@chunk;
}

sub commit {
}

1;
