package Stream::Filter::Subfilter;

# ABSTRACT: filter part of given data using another filter

use strict;
use warnings;

use parent qw(Stream::Filter);

use Params::Validate qw(:all);

=head1 SYNOPSIS

    # importing
    use Stream::Filter::Subfilter;

    $filter = filter(sub { return shift() ** 2 });
    $in = array_seq([
        { id => 2, value => "abc" },
        { id => 3, value => "def" },
    ]);

    # constructing
    $subfilter = Stream::Filter::Subfilter->new($filter, sub { shift->{id} }, sub { $_[0]->{square} = $_[1]; return $_[0] });

    # using
    $res1 = $subfilter->write($in->read); # { id => 2, value => "abc", square => 4 }
    $res2 = $subfilter->write($in->read); # { id => 3, value => "def", square => 9 }

=head1 DESCRIPTION

This module is helpful when you have filter processing some fixed data type, and input stream providing rows which contain that data type inside one of it fields.

For example, you can have stream providing hashrefs C<{ id => $id, comment => $comment }> and filter which fetches value by id.

Filter B<must> return the same number of items as it's been given in C<write> and C<write_chunk> methods. And in the same order, of course.

=cut

sub new {
    my $class = shift;
    my ($filter, $reader, $writer) = validate_pos(@_, { isa => 'Stream::Filter' }, { type => CODEREF }, { type => CODEREF });
    return bless {
        filter => $filter,
        reader => $reader,
        writer => $writer,
    } => $class;
}

sub write {
    my ($self, $item) = @_;
    return $self->{writer}->($item, $self->{filter}->write($self->{reader}->($item)));
}

sub write_chunk {
    my ($self, $chunk) = @_;
    my $subchunk = [ map { scalar($self->{reader}->($_)) } @$chunk ];
    my $filtered_subchunk = $self->{filter}->write_chunk($subchunk);
    unless (@$filtered_subchunk == @$chunk) {
        die "filtered chunk has wrong size ".@$filtered_subchunk.", original chunk had size ".@$chunk;
    }
    my $filtered_chunk;
    for my $i (0..(@$chunk - 1)) {
        $filtered_chunk->[$i] = $self->{writer}->($chunk->[$i], $filtered_subchunk->[$i]);
    }
    return $filtered_chunk;
}

sub commit {
    my $self = shift;
    $self->{filter}->commit;
}

1;
