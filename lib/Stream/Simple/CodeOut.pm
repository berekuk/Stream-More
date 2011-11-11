package Stream::Simple::CodeOut;

# ABSTRACT: output stream generated from callback

use strict;
use warnings;

use parent qw(Stream::Out);

sub new {
    my ($class, $callback) = @_;
    return bless { callback => $callback } => $class;
}

sub write {
    my ($self, $item) = @_;
    $self->{callback}->($item);
}

1;

