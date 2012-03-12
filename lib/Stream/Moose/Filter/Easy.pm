package Stream::Moose::Filter::Easy;

# ABSTRACT: role to implement filters with only one write() method

=head1 SYNOPSIS

    use Moose;
    with 'Stream::Moose::Filter::Easy';

    sub write {
        ...
    }

=head1 DESCRIPTION

This role lets you define filter class with only one method, B<write>.

B<write_chunk> delegates its calls to B<write>, and B<commit> method does nothing.

Note that this role doesn't implement buffering!

=cut

use namespace::clean -except => 'meta';
use Moose::Role;
use MooseX::Role::WithOverloading;
with 'Stream::Moose::Filter';

sub write_chunk {
    my ($self, $chunk) = @_;
    confess "write_chunk method expects arrayref, you specified: '$chunk'" unless ref($chunk) eq 'ARRAY'; # can chunks be blessed into something?
    my @result_chunk;
    for my $item (@$chunk) {
        push @result_chunk, $self->write($item);
    }
    return \@result_chunk;
}

sub commit {
}

1;
