package Stream::Out::Balancer;

use strict;
use warnings;

# ABSTRACT: write data into one of several output streams

=head1 SYNOPSIS

    use Stream::Out::Balancer;

    $multi_out = Stream::Out::Balancer->new([ $out1, $out2, $out3 ], { options });

    $multi_out->write($item);
    $multi_out->write($item2);
    $multi_out->commit;

=head1 DESCRIPTION

This is an output stream which balances all writes into several underlying output streams.

If some output streams fail on write or on commit, all uncommited writes into them will be copied into other output streams.

=cut

use Yandex::Logger;

use parent qw(Stream::Out::Any);

use namespace::autoclean;
use Params::Validate qw(:all);
use Scalar::Util qw(blessed);
use List::Util qw(shuffle);

use Carp;

=head1 CONSTRUCTOR

=over

=item B<< new($targets) >>

=item B<< new($targets, $options) >>

Construct new C<< Stream::Out::Balancer >> object.

C<$targets> should be an arrayref with output streams.

C<$options> is an optional hashref with some of following fields:

=over

=item I< revalidate >

If set, output streams which threw an exception on C<write> or C<commit> ill be banned only for the given number of seconds, and then this class will try to write to them and commit them again.

=item I< normal_distribution >

If set and false, C<$targets> will be shuffled in uniform distribution.

Otherwise (by default), it will has normal distribution.

=back

=back

=cut
sub new {
    my $class = shift;

    my ($targets, @options) = validate_pos(@_, {
        type => ARRAYREF,
        callbacks => {
            'targets are stream-propagates' => sub {
                for (@{ shift() }) {
                    return unless blessed($_);
                    return unless $_->isa('Stream::Propagate');
                }
                return 1;
            },
            'at least one target given' => sub {
                return scalar @{ shift() };
            }
        },
    }, 0);

    my $options = validate(@options, {
        cache_period => { type => SCALAR, regex => qr/^\d+$/, default => 60 },
        revalidate => { type => SCALAR, regex => qr/^\d+$/, default => 30 },
        balance_percent => { type => SCALAR, regex => qr/^\d*\.?\d+$/, default => 0.8 }, # allowed percent of slow/broken maching
        normal_distribution => { type => BOOLEAN, default => 1 },                        # use uniform distribution or normal one
        brake_adaptive => { type => BOOLEAN, default => 1 },                             # apply balance percent to total hosts count or to alive ones
    });

    my $self = $class->SUPER::new($targets, { shuffle => 0, revalidate => $options->{revalidate} });
    $self->{$_} = $options->{$_} for (keys %$options);
    $self = bless $self => $class;

    $self->{indexes} = [ $self->_indexes ];

    $self->{timestamp} = time;
    $self->{w} = 0;
    $self->{n} = $self->{indexes}->[$self->{w}];
    $self->{w_total} = scalar @{ $self->{indexes} };

    return $self;
}

=head1 METHODS

This class inherits L<Stream::Out::Any> API and doesn't have any additional public methods.

=over

=item B<_targets(@)>

Takes:   target list.
Returns: targets sorted according to their occupancy.

=cut
sub _indexes { # resort targets list according to their current occupancy
    my ($self) = @_;
    my $targets = $self->{targets};

    my %coefs = map { my $occ = eval { $targets->[$_]->occupancy }; (defined $occ) ? ($_ => $occ) : () } (0 .. $#$targets);
    die "All targets are DOWN" unless keys %coefs;

    my @sorted_targets = sort { $coefs{$a} <=> $coefs{$b} } keys %coefs;

    confess "All hosts are down" unless @sorted_targets;

    my $max_trusted_id = 0;
    unless ( $self->{brake_adaptive} ) {
        $max_trusted_id = int($self->{balance_percent} * $#$targets);
        $max_trusted_id = $#sorted_targets if ($max_trusted_id > $#sorted_targets);
    } else {
        $max_trusted_id = int($self->{balance_percent} * $#sorted_targets);
    }

    my @idxs = ();
    if ($self->{normal_distribution} && $max_trusted_id > 0) {
        my $A = 5;
        my $K = - ( $max_trusted_id**2 / log( 1 / $A ) );
        my $norm_gen = sub { my ($x, $k) = @_; return int($A * exp( - $x**2 / $k )); };

        @idxs = shuffle map { ($sorted_targets[ $_ ]) x $norm_gen->($_, $K) } (0 .. $#sorted_targets);
    } else {
        @idxs = shuffle map { $sorted_targets[ $_ ] } (0 .. $max_trusted_id );
    }

    return @idxs;
}

=item B<_next_target>

Set internal target pointer ->{n} to the next target ( skip invalids and takes into account distribution type )

=cut
sub _next_target {
    my $self = shift;
    my $time = time;

    if ($self->{timestamp} < $time - $self->{cache_period}) { # refresh once in a minute
        $self->{indexes} = [ $self->_indexes() ];
        $self->{invalid} = [];
        $self->{timestamp} = $time;
        $self->{w_total} = scalar @{ $self->{indexes} };
    }

    do {
        $self->{w} = ($self->{w} + 1) % $self->{w_total};
        $self->{n} = $self->{indexes}->[$self->{w}];
    } while ($self->_is_invalid($self->{n}));
}

=back

=cut

1;
