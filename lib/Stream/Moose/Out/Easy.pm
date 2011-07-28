package Stream::Moose::Out::Easy;

use Moose::Role;
with 'Stream::Moose::Out';

sub write_chunk($$;$) {
    my ($self, $chunk, @extra) = @_;
    confess "write_chunk method expects arrayref, you specified: '$chunk'" unless ref($chunk) eq 'ARRAY'; # can chunks be blessed into something?
    for my $item (@$chunk) {
        $self->write($item, @extra);
    }
    return;
}

sub commit {
}

1;
