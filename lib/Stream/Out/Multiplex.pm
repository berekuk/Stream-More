package Stream::Out::Multiplex;

use strict;
use warnings;

=head1 NAME

Stream::Out::Multiplex - multiplex data into several output streams

=head1 SYNOPSIS

    use Stream::Out::Multiplex;

    $multi_out = Stream::Out::Multiplex->new([ $out1, $out2, $out3 ]);

    $multi_out->write;
    $multi_out->commit;

=cut

use Yandex::Version '{{DEBIAN_VERSION}}';

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
    my $self = bless { targets => $targets } => $class;
    return $self;
}

sub write {
    my ($self, $item) = @_;
    for my $target (@{$self->{targets}}) {
        $target->write($item);
    }
}

sub write_chunk {
    my ($self, $chunk) = @_;
    for my $target (@{$self->{targets}}) {
        $target->write_chunk($chunk);
    }
}

sub commit {
    my $self = shift;
    for my $target (@{$self->{targets}}) {
        $target->commit();
    }
}


=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

