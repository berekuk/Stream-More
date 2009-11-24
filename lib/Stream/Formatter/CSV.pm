package Stream::Formatter::CSV;

use strict;
use warnings;

=head1 NAME

Stream::Formatter::CSV - CSV <=> hashref formatter

=head1 DESCRIPTION

This is not really a CSV formatter, it doesn't implement correct quoting.

=cut

use base qw(Stream::Formatter);
use Stream::Filter qw(filter);
use Params::Validate qw(:all);
use Scalar::Util qw(reftype);
use Carp;

sub new {
    my $class = shift;
    my @columns = @_;
    unless (@columns) {
        croak "No columns specified";
    }
    for my $column (@columns) {
        croak "Wrong column $column" if reftype($column);
        croak "Undefined column specified" unless defined $column;
    }
    return bless { columns => \@columns } => $class;
}

sub write_filter {
    my $self = shift;
    return filter(sub {
        my $item = shift;
        my @values;
        for my $column (@{$self->{columns}}) {
            my $value = $item->{$column};
            $value = '' unless defined $value;
            push @values, $value;
        }
        for my $key (keys %$item) {
            unless (grep { $_ eq $key }  @{$self->{columns}}) {
                croak "Unknown key '$key' in item $item";
            }
        }
        return (join("\t", @values)."\n");
    });
}

sub read_filter {
    my $self = shift;
    return filter(sub {
        my $line = shift;
        chomp $line;
        my @fields = split /\t/, $line; # TODO - allow separator to be customized
        my $item = {};
        for my $column (@{$self->{columns}}) {
            my $value = shift @fields;
            next unless defined $value;
            $item->{$column} = $value; # TODO - unescape?
        }
        return $item;
    });
}

=head1 BUGS & CAVEATS

Should think through backward-compatibility issues and implement correct quoting.

There is no way to specify custom separator yet.

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

