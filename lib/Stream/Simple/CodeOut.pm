package Stream::Simple::CodeOut;

# ABSTRACT: output stream generated from callback

use strict;
use warnings;

use parent qw(Stream::Out);

sub new {
    my ($class, $callback, $commit_callback) = @_;
    return bless {
        callback => $callback,
        commit_callback => $commit_callback,
    } => $class;
}

sub write {
    my ($self, $item) = @_;
    $self->{callback}->($item);
}

sub commit {
    my ($self) = @_;
    $self->{commit_callback}->() if $self->{commit_callback};
}

1;

