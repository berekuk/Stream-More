package Stream::In::Buffer;

# ABSTRACT: stream decorator to allow non sequential commits

use strict;
use warnings;

use parent qw(
    Stream::In
    Stream::In::Role::Lag
);

use namespace::autoclean;

use Params::Validate qw(:all);

use Yandex::Logger;
use Stream::Buffer::SQLite;
use Stream::Buffer::Persistent;
use Stream::Buffer::File;

=head1 SYNOPSIS

    $b_in = Stream::In::Buffer->new($in, { dir => $dir, size => $size });
    my ($id1, $data1) = @{$b_in->read()};
    my ($id2, $data2) = @{$b_in->read()};
    my ($id3, $data3) = @{$b_in->read()};
    ...
    $b_in->commit([$id1, $id3]);
    undef $b_in; # $data2 remains uncommited and would be returned again.

=head1 DESCRIPTION

MQ!
No read/only support.

=head1 METHODS

=over

=item B<< new($in, $params) >>

Constructor.

C<$params> are Stream::Buffer::SQLite constructor params.

=cut
sub new {
    my $class = shift;
    my ($in, @options) = validate_pos(@_, { type => CODEREF | OBJECT }, { type => HASHREF, optional => 1 });

    my $opts = validate(@options, {
        dir => 1,
        max_chunk_size => 0,
        max_chunk_count => 0,
        max_log_size => 0,
        buffer_class => { default => 'Stream::Buffer::File' },
    });
    my $self = { max_chunk_size => $opts->{max_chunk_size}, active_size => 0 };
    bless $self => $class;
    $self->{in} = ref $in eq "CODE" ? $in : sub { $in };

    my $buffer_class = $opts->{buffer_class};
    $self->{buffer} = $buffer_class->new($opts);

    return $self;
}

sub read_chunk {
    my $self = shift;
    my ($limit) = @_;
    my $remain = ( $self->{max_chunk_size} ) ? $self->{max_chunk_size} - $self->{active_size} : $limit;

    my $result = [];
    push @$result, @{$self->{buffer}->load($limit)};
    $limit -= @$result;
    $remain -= @$result;
    if ( $limit <= 0 ) {
        $self->{active_size} += @$result;
        return $result;
    }

    my $in = $self->{in}->();
    # $in is supposed to be thread-safe
    my $chunk = $in->read_chunk( ( $remain > $limit ) ? $remain : $limit );

    if ($chunk) {
        if (@$chunk) {
            $self->{buffer}->save($chunk);
            push @$result, @{ $self->{buffer}->load($limit) };
        }
        $in->commit;
    }

    return unless @$result;

    $self->{active_size} += @$result;
    return $result;
}

sub read {
    my $self = shift;
    my $chunk = $self->read_chunk(1);
    return unless $chunk;
    return $chunk->[0];
}

sub commit {
    my $self = shift;
    my ($ids) = @_;

    return unless $ids; # process($mq => code_out(sub { [$id, $item] = shift; ... $mq->commit([$id]) }));

    $self->{active_size} -= @$ids;
    $self->{buffer}->delete($ids);
}

sub lag {
    my $self = shift;
    my $lag = $self->{in}->()->lag();
    $lag += $self->{buffer}->lag();
    return $lag;
}

=back

=cut

1;
