package Stream::Skip;

# ABSTRACT: IN stream decorator for skiping items

use strict;
use warnings;

=head1 SYNOPSIS

   use Stream::Skip;

   $in = Stream::Skip->new($input_stream, {}); # constructor may has some options:

=cut

use parent qw(
    Stream::In
    Stream::In::Role::Lag
);

use namespace::autoclean;

use Params::Validate qw(:all);

use Carp;
use Yandex::Logger;

=head1 CONSTRUCTOR OPTIONS

You may set either:

=over

=item *

mag_lag - for the max lag when we'll start skipping

=back

OR:

=over

=item *

skip_percent => '95' - percent of max data when we'll start skipping

=item *

max_data_size => 1024 * 1024 * 1024 - max data size of input stream

=item *

lag_check_interval => 1000 - lag check interval in items

=back

=cut
sub new {
    my $class = shift;
    my ($in, @options) = validate_pos(@_, { type => OBJECT }, { type => HASHREF, optional => 1 });

    my $self = validate(@options, {
        skip_percent => { default => '95' },
        max_data_size => { default => 1024 * 1024 * 1024 }, # 1 GB
        lag_check_interval => { default => '1000' },
        max_lag => { default => -1 },
    });

    $self->{in_stream} = $in;
    $self->{count} = 0;

    $self->{max_lag} = ($self->{skip_percent} * $self->{max_data_size}) / 100 if $self->{max_lag} < 0;
    eval {
        $self->{skip} = ($in->lag() >= $self->{max_lag});
    };
    if ($@) {
        $self->{skip} = 0;
    }

    return bless $self, $class;
}

=head1 METHODS

=over

=item B<check_lag()>

Gets lag of input stream and checks whether it exceeds our limit

=cut
sub check_lag {
    my $self = shift;
    eval {
        return ($self->{in_stream}->lag() >= $self->{max_lag})
    };
    return 0;
}

sub read {
    my $self = shift;

    if ($self->{count}++ >= $self->{lag_check_interval}) {
        if ($self->{skip}) {
            INFO "Skipped: $self->{count}";
            $self->{in_stream}->commit();
        }
        $self->{skip} = $self->check_lag();
        $self->{count} = 0;
    }

    my $item = $self->{in_stream}->read(@_);
    return (($self->{skip})?():($item));
}

sub read_chunk {
    my ($self, $data_size) = @_;

    if (($self->{count} += $data_size) >= $self->{lag_check_interval}) {
        if ($self->{skip}) {
            INFO "Skipped: $self->{count}";
            $self->{in_stream}->commit();
        }
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

=back

=cut

1;
