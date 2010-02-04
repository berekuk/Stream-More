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

use base qw(Stream::Out);

use Params::Validate qw(:all);
use Scalar::Util qw(blessed);

sub new {
    my $class = shift;
    my ($targets) = validate_pos(@_, {
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
    });
    my $total = @$targets;
    my $self = bless {
        targets => $targets,
        buffers => [],
        invalid => [ (0) x $total ],
        n => 0,
        total => $total,
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
    } while ($self->{invalid}[ $self->{n} ]);
}

sub _check_invalid {
    my $self = shift;
    my $all = sub { $_ || return 0 for @_; 1 };
    if ($all->(@{ $self->{invalid} })) {
        die "All targets are invalid";
    }
}

sub _mark_invalid {
    my ($self, $i) = @_;
    $self->{invalid}[ $i ] = 1;
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
            next if $self->{invalid}[$i];
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

