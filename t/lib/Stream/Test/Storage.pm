package Stream::Test::Storage;

use strict;
use warnings;

use parent qw(Test::Class);
use Test::More;
use Params::Validate qw(:all);
use namespace::autoclean;

sub new {
    my $class = shift;
    my ($storage_gen) = validate_pos(@_, { type => CODEREF });
    my $self = $class->SUPER::new;
    $self->{storage_gen} = $storage_gen;
    return $self;
}

sub setup :Test(setup) {
    my $self = shift;
    $self->{storage} = $self->{storage_gen}->();
}

sub teardown :Test(teardown) {
    my $self = shift;
    delete $self->{storage};
}

sub is_stream_storage :Test(1) {
    my $self = shift;
    ok($self->{storage}->isa('Stream::Storage'));
}

sub write_scalar :Test(1) {
    my $self = shift;
    $self->{storage}->write('abc');
    $self->{storage}->write(5);
    $self->{storage}->write(123);
    pass('write succeeded');
}

sub write_chunk_scalar :Test(1) {
    my $self = shift;
    $self->{storage}->write_chunk([ 'a', 'b', 5 ]);
    $self->{storage}->write_chunk([ 'a', 'b', 6 ]);
    pass('write_chunk succeeded');
}

sub commit_scalar :Test(1) {
    my $self = shift;
    $self->{storage}->write('abc');
    $self->{storage}->write(5);
    $self->{storage}->write_chunk([ 'a'..'e' ]);
    $self->{storage}->commit;
    $self->{storage}->write('a b');
    $self->{storage}->commit;
    pass('commit with mixed writes succeeded');
}

1;
