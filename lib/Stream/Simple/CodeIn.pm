package Stream::Simple::CodeIn;

# ABSTRACT: input stream generated from callback

use strict;
use warnings;

use parent qw(Stream::In);

sub new {
    my ($class, $code) = @_;
    return bless { callback => $code } => $class;
}

sub read {
    my ($self, $item) = @_;
    return $self->{callback}->();
}

1;
