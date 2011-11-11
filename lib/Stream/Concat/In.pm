package Stream::Concat::In;

# ABSTRACT: concatenate several input streams

use namespace::autoclean;
use Moose;
use Moose::Util qw(apply_all_roles);
use List::MoreUtils qw(all);
use List::Util qw(sum);

use Params::Validate;
use Carp;

sub isa {
    return 1 if $_[1] eq __PACKAGE__;
    $_[0]->next::method if $_[0]->next::can;
} # ugly hack

has 'current' => (
    is => 'rw',
    isa => 'Int',
    default => 0,
);

has 'in' => (
    is => 'ro',
    isa => 'ArrayRef',
    required => 1,
);

=head1 METHODS

=over

=item C<BUILDARGS(@in)>

Constructor of this class accepts the list of input streams.

=cut
sub BUILDARGS {
    my $class = shift;
    my @in = @_;

    croak "At least one input stream must be specified" unless @in;
    for (@in) {
        croak "Invalid argument '$_'" unless $_->isa('Stream::In');
    }

    return {
        in => \@in,
    }
}

sub BUILD {
    my $self = shift;

    if (all {
        $_->does('Stream::Moose::In::Lag')
        or $_->does('Stream::In::Role::Lag')
    } @{ $self->in }) {
        apply_all_roles($self, 'Stream::Moose::In::Lag');
    }
}

sub read {
    my $self = shift;

    while (1) {
        return if $self->current >= @{ $self->in };
        my $item = $self->in->[ $self->current ]->read;
        return $item if defined $item;
        $self->current($self->current + 1);
    }
}

sub read_chunk {
    my $self = shift;

    while (1) {
        return if $self->current >= @{ $self->in };
        my $chunk = $self->in->[ $self->current ]->read_chunk(@_);
        return $chunk if defined $chunk;
        $self->current($self->current + 1);
    }
}

=item B< lag >

Instances of this class implement C<lag()> method and C<Stream::Moose::In::Lag> role if and only if all input streams implement them.

=cut
sub lag {
    my $self = shift;

    die 'unimplemented' unless $self->does('Stream::Moose::In::Lag');

    return sum(map { $_->lag } @{ $self->in });
}

sub commit {
    my $self = shift;
    $_->commit for @{ $self->in };
}

=back

=cut

with 'Stream::Moose::In';

=head1 SEE ALSO

L<Stream::Concat>

=cut

__PACKAGE__->meta->make_immutable;
