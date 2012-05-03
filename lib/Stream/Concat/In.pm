package Stream::Concat::In;

# ABSTRACT: concatenate several input streams

=head1 SYNOPSIS

    my $concat = Stream::Concat::In->new(@input_streams);

    $concat->read; # read from the first one, then second, etc.

=head1 DESCRIPTION

Reads from the list of given input streams sequentially.

Note that this module won't read from previous streams after it moved to subsequent ones, but it will return new data from the last stream if it appeared there, even if it returned undef previously.

=cut

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

    # TODO - which solution is better, this one, or overriding DOES (like Stream::Concat does)?
    if (all { $_->DOES('Stream::In::Role::Lag') } @{ $self->in }) {
        apply_all_roles($self, 'Stream::Moose::In::Lag');
    }
}

sub read_chunk {
    my $self = shift;

    my $total_in = @{ $self->in };

    while (1) {
        return if $self->current >= $total_in;
        my $chunk = $self->in->[ $self->current ]->read_chunk(@_);
        return $chunk if defined $chunk;
        return if $self->current == $total_in - 1;
        $self->current($self->current + 1);
    }
}

=item B< lag >

Instances of this class implement C<lag()> method and C<Stream::Moose::In::Lag> role if and only if all input streams implement them.

=cut
sub lag {
    my $self = shift;
    return sum(map { $_->lag } @{ $self->in });
}

sub commit {
    my $self = shift;
    $_->commit for @{ $self->in };
}

=back

=cut

with 'Stream::Moose::In::Chunked';

=head1 SEE ALSO

L<Stream::Concat>

=cut

__PACKAGE__->meta->make_immutable;
