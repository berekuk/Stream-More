package Stream::Simple::SwitchOut;

# ABSTRACT: output stream with switch -> case semantic

use strict;
use warnings;

use Stream::Simple::CodeOut;
use parent qw(Stream::Out);

# $switch should be func-ref, case - hash with cases like { 'a' => stream-pipe }
sub new {
    my $class = shift;
    my ($switch, $cases) = @_;
    $cases->{otherwise} = Stream::Simple::CodeOut->new(sub{}) unless ($cases->{otherwise}); # out::null
    
    return bless {
        switch => $switch,
        cases => { map { $_ => { stream => $cases->{$_}, dirty => 0 } } keys %$cases },
    } => $class;
}

sub write {
    my ($self, $item) = @_;
    my $value = $self->{switch}->($item) || 'otherwise';

    my $out = $self->{cases}->{$value} || $self->{cases}->{otherwise};
    $out->{stream}->write($item);
    $out->{dirty} = 1;
}

sub commit {
    my ($self) = @_;
    
    for my $case (values %{$self->{cases}}) {
        $case->{stream}->commit() if $case->{dirty}--;
    }
}

1;

