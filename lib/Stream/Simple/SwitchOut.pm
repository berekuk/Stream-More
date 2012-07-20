package Stream::Simple::SwitchOut;

# ABSTRACT: output stream with switch -> case semantic

use strict;
use warnings;

use Stream::Simple::CodeOut;
use parent qw(Stream::Out);

# $switch should be func-ref, case - hash with cases like { 'a' => stream-pipe }
sub new($$$;$) {
    my $class = shift;
    my ($switch, $cases, $chunk_size) = @_;
    $cases->{default} = Stream::Simple::CodeOut->new(sub{}) unless ($cases->{default}); # out::null

    return bless {
        switch => $switch,
        chunk_size => ($chunk_size || 1000),
        cases => { map { $_ => { stream => $cases->{$_}, dirty => 0, chunk => [] } } keys %$cases },
    } => $class;
}

sub write {
    my $self = shift;

    $self->write_chunk([@_]);
}

sub write_chunk($$) {
    my ($self, $chunk) = @_;

    $self->write_item($_) for (@$chunk);

    for my $case (values %{$self->{cases}}) {
        if (scalar @{$case->{chunk}} >= $self->{chunk_size}) {
            $case->{stream}->write_chunk($case->{chunk});
            $case->{chunk} = [];
        }
    }
}

sub write_item($$) {
    my ($self, $item) = @_;
    my $value = $self->{switch}->($item) || 'default';

    my $case = $self->{cases}->{$value} || $self->{cases}->{default};

    push @{$case->{chunk}}, $item;
    $case->{dirty} = 1;
}

sub commit {
    my ($self) = @_;

    for my $case (values %{$self->{cases}}) {
        $case->{stream}->write_chunk($case->{chunk});
        $case->{stream}->commit() if $case->{dirty}--;
        $case->{chunk} = [];
    }
}

1;

