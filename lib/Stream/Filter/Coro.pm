package Stream::Filter::Coro;

# ABSTRACT: parallelize any filter

=head1 SYNOPSIS

    my $parallel_filter = Stream::Filter::Coro->new(
        threads => 5,
        filter => sub { MyFilter->new }, # callback should create the new instance of filter to guarantee thread-safety
    );
    # now, just use $parallel_filter instead of $filter

=cut

use Moo;
with 'Stream::Moose::Filter::Easy';

use Try::Tiny;
use Types::Standard qw(Int CodeRef Object);
use Scalar::Util qw(blessed);

use namespace::clean;

has 'threads' => (
    is => 'ro',
    isa => Int,
    required => 1,
);

has 'alive_threads' => (
    is => 'rw',
    isa => Int,
    default => sub { 0 },
);

has 'chunk_size' => (
    is => 'ro',
    isa => Int,
    default => sub { 1 },
);

has 'filter' => (
    is => 'rw',
    isa => CodeRef|Object,
);

sub BUILD {
    my $self = shift;
    require Coro;
    require Coro::Channel;
    if (blessed $self->filter) {
        warn "Passing filter object to Stream::Filter::Coro is deprecated, use sub { ... } instead";
        my $filter = $self->filter;
        $self->filter(sub { $filter });
    }
}

has '_in_channel' => (
    is => 'lazy',
    default => sub {
        Coro::Channel->new(1);
    },
    clearer => '_clear_in',
);

has '_out_channel' => (
    is => 'lazy',
    default => sub {
        Coro::Channel->new; # TODO - maxsize?
    },
    clearer => '_clear_out',
);

has '_coros' => (
    is => 'lazy',
    predicate => 1,
    clearer => 1,
);
sub _build__coros {
    my $self = shift;
    my @coros;
    my $in = $self->_in_channel;
    my $out = $self->_out_channel;
    $self->alive_threads(0);
    for my $i (1 .. $self->threads) {
        my $filter = $self->filter->();
        push @coros, Coro::async(sub {
            my $ok = eval {
                while (my $task = $in->get) {
                    if ($task->{action} eq 'write') {
                        my @result = $filter->write($task->{item});
                        $out->put({ item => $_ }) for @result;
                    }
                    elsif ($task->{action} eq 'commit') {
                        my @result = $filter->commit;
                        $out->put({ item => $_ }) for @result;
                        return 1;
                    }
                    else {
                        die "Invalid task $task, action $task->{action}";
                    }
                }
                1;
            };
            $self->alive_threads( $self->alive_threads - 1 );
            unless ( $ok ) {
                my $err = $@;

                if ( !$self->alive_threads && $in->size ) {
                    $in->get while ( $in->size );
                    # empty "in" queue, because we don't have enough alive workers to cope with it
                }

                $out->put({ exception => $err })
            }
        });
        $self->alive_threads( $self->alive_threads + 1 );
    }
    return \@coros;
}

sub _read_all {
    my $self = shift;
    my @result;
    while ($self->_out_channel->size) {
        my $result = $self->_out_channel->get;
        if (exists $result->{exception}) {
            if ($self->_has_coros) {
                $self->_in_channel->shutdown;
                $_->join for @{ $self->_coros };
            }

            $self->_clear_coros;
            $self->_clear_in;
            $self->_clear_out;
            die $result->{exception};
        }
        else {
            push @result, $result->{item};
        }
    }
    return @result;
}

=head1 METHODS

=over

=cut

sub write {
    my $self = shift;
    my ($item) = @_; # TODO - support additional parameters somehow?
    $self->_coros; # force coros reconstruction after commit
    die "no alive threads" unless $self->alive_threads;
    $self->_in_channel->put({ action => 'write', item => $item });
    Coro::cede();
    return $self->_read_all;
}

sub commit {
    my $self = shift;
    return unless $self->_has_coros;

    for ( 1 .. $self->alive_threads ) {
        last unless $self->alive_threads;
        $self->_in_channel->put({ action => 'commit' });
    }
    for ( @{ $self->_coros } ) {
        $_->join;
    }
    my @result = $self->_read_all;

    $self->_clear_coros;
    $self->_clear_in;
    $self->_clear_out;

    return @result;
}

sub DEMOLISH {
    local $@;
    my $self = shift;
    if ($self->_has_coros) {
        $self->_in_channel->shutdown;
        $_->join for @{ $self->_coros };
    }
}

=back

=cut

1;
