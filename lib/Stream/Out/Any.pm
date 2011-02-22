package Stream::Out::Any;

use strict;
use warnings;

=head1 NAME

Stream::Out::Any - write data into one of several output streams

=head1 SYNOPSIS

    use Stream::Out::Any;

    $multi_out = Stream::Out::Any->new([ $out1, $out2, $out3 ]);

    $multi_out->write($item);
    $multi_out->write($item2);
    $multi_out->commit;

=cut

use Yandex::Version '{{DEBIAN_VERSION}}';
use Yandex::Logger;

use parent qw(Stream::Out);

use namespace::autoclean;
use Params::Validate qw(:all);
use Scalar::Util qw(blessed);
use List::MoreUtils qw(all);

sub new {
    my $class = shift;
    my ($targets, @options) = validate_pos(@_, {
        type => ARRAYREF,
        callbacks => {
            'targets are streams' => sub {
                for (@{ shift() }) {
                    return unless blessed($_);
                    return unless $_->isa('Stream::Out');
                }
                return 1;
            }
        },
    }, 0);

    my $options = validate(@options, {
        revalidate => { type => SCALAR, regex => qr/^\d+$/, optional => 1 },
    });
    my $total = @$targets;
    my $self = bless {
        targets => $targets,
        buffers => [],
        invalid => [ (undef) x $total ],
        n => 0,
        total => $total,
        %$options,
    } => $class;
    return $self;
}

sub write {
    my ($self, $item) = @_;
    $self->write_chunk([$item]);
}

sub _next_target {
    my $self = shift;
    do {
        $self->{n} = ($self->{n} + 1) % $self->{total};
    } while ($self->_is_invalid($self->{n}));
}

sub _check_invalid {
    my $self = shift;
    if (all { $self->_is_invalid($_) } (0 .. $self->{total} - 1)) {
        die "All targets are invalid";
    }
}

sub _is_invalid {
    my ($self, $i) = @_;
    if (defined $self->{revalidate} and defined $self->{invalid}[$i] and $self->{invalid}[$i] < time) {
        INFO "Target #$i revalidated";
        undef $self->{invalid}[$i];
    }
    return defined $self->{invalid}[$i];
}

sub _mark_invalid {
    my ($self, $i) = @_;
    if (defined $self->{revalidate}) {
        $self->{invalid}[ $i ] = time + $self->{revalidate};
    }
    else {
        $self->{invalid}[ $i ] = 1;
    }
    $self->_check_invalid;
    $self->_next_target;

    for (@{ $self->{buffers}[$i] }) {
        $self->write_chunk($_);
    }
    $self->{buffers}[$i] = [];
}

sub write_chunk {
    my ($self, $chunk) = @_;
    my $n = $self->{n};
    eval {
        $self->{targets}[$n]->write_chunk($chunk);
    };
    if ($@) {
        WARN "Write to target #$n failed: $@";
        $self->_mark_invalid($n);

        return $self->write_chunk($chunk);
    }
    push @{ $self->{buffers}[$n] }, $chunk;
    $self->_next_target;
}

sub commit {
    my $self = shift;
    do {
        $self->_check_invalid;
        for my $i (0..$self->{total}-1) {
            next if $self->_is_invalid($i);
            my $target = $self->{targets}[$i];

            eval {
                $target->commit();
            };
            if ($@) {
                WARN "Commiting target #$i failed: $@";
                $self->_mark_invalid($i);
            }
            $self->{buffers}[$i] = [];
        }
    } while ( grep { @$_ } @{ $self->{buffers} } );
}

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

