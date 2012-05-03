package Stream::Skip;

use strict;
use warnings;

use parent qw(
    Stream::In
    Stream::In::Role::Lag
);

use namespace::autoclean;

use Params::Validate qw(:all);

use Carp;
use Yandex::Logger;

sub new {
    my $class = shift;
    my ($in, @options) = validate_pos(@_, { type => OBJECT }, { type => HASHREF, optional => 1 });

    my $self = validate(@options, {
        skip_percent => { default => '95' },
        max_data_size => { default => 1024 * 1024 * 1024 }, # 1 GB
        lag_check_interval => { default => '1000' },
    });

    $self->{in_stream} = $in;
    $self->{count} = 0;
    $self->{skip} = 0;
    $self->{max_lag} = ($self->{skip_percent} * $self->{max_data_size}) / 100;
    
    return bless $self, $class;
}

sub check_lag {
    my $self = shift;
    return ($self->{in_stream}->lag() >= $self->{max_lag})
}

sub read {
    my $self = shift;

    if ($self->{count}++ >= $self->{lag_check_interval}) {
        $self->{skip} = $self->check_lag();
        $self->{count} = 0;
    }

    my $item = $self->{in_stream}->read(@_);
    return (($self->{skip})?():($item));
}

sub read_chunk {
    my ($self, $data_size) = @_;

    if (($self->{count} += $data_size) >= $self->{lag_check_interval}) {
        $self->{skip} = $self->check_lag();
        $self->{count} = 0;
    }

    my $chunk = $self->{in_stream}->read_chunk($data_size);
    return ( ($self->{skip})?[]:$chunk );
}

sub lag {
    my $self = shift;
    return $self->{in_stream}->lag(@_);
}

sub commit {
    my $self = shift;
    return $self->{in_stream}->commit();
}

1;