package Stream::Filter::Coro;

# ABSTRACT: parallelize any filter

=head1 SYNOPSIS

    my $parallel_filter = Stream::Filter::Coro->new(
        threads => 5,
        filter => $filter,
    );
    # now, just use $parallel_filter instead of $filter

=cut

use namespace::autoclean;
use Moose;
with 'Stream::Moose::Filter::Easy';

use Try::Tiny;

has 'threads' => (
    is => 'ro',
    isa => 'Int',
    required => 1,
);

has 'filter' => (
    is => 'ro',
    required => 1,
);

sub BUILD {
    require Coro;
}

has '_buffer' => (
    is => 'rw',
    default => sub { [] },
);

=head1 METHODS

=over

=cut

sub write {
    my $self = shift;
    my ($item) = @_; # TODO - support additional parameters somehow?
    push @{ $self->_buffer }, $item;
    return $self->flush if @{ $self->_buffer } >= $self->threads;
    return ();
}

sub commit {
    my $self = shift;
    $self->write($_) for $self->filter->commit;
    return $self->flush;
}

=item B<flush()>

Force flushing.

Returns all filtered items.

This method will be called automatically per every I<threads> items, processing all buffered items in parallel.

=cut
sub flush {
    my $self = shift;
    my @buffer = @{ $self->_buffer };

    my @result;
    my @coros;
    my $error = 'unknown';
    my $error_total = 0;
    for my $item (@buffer) {
        push @coros, Coro::async(sub {
            try {
                push @result, $self->filter->write($item);
            }
            catch {
                $error = $_;
                $error_total++;
            };
        });
    }
    $_->join for @coros;

    $error .= "($error_total errors total)" if $error_total > 1;
    die $error if $error_total;

    $self->_buffer([]);

    return @result;
}

=back

=cut

__PACKAGE__->meta->make_immutable;
