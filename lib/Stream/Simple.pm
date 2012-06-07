package Stream::Simple;

# ABSTRACT: simple procedural-style constructors of some streams

use strict;
use warnings;

=head1 SYNOPSIS

    use Stream::Simple;

    $in = array_in([5,6,7]);

    $out = code_out(sub { print shift });

=head1 FUNCTIONS

=over

=cut

use parent qw(Exporter);
our @EXPORT_OK = qw/
    array_seq array_in code_in code_out memory_storage
    coro_filter coro_out
/;

use Carp;
use Stream::Simple::ArrayIn;
use Stream::Simple::CodeIn;
use Stream::MemoryStorage;
use Stream::Simple::CodeOut;
use Stream::Filter qw/filter/;
use Stream::Filter::Coro;
use Params::Validate qw(:all);

=item B<array_in($list)>

Creates stream which shifts items from specified list and returns them as stream values.

=cut
sub array_in($) {
    my ($list) = validate_pos(@_, { type => ARRAYREF });
    return Stream::Simple::ArrayIn->new($list);
}

=item B< code_in($coderef) >

Creates input stream which generates items by calling given callback.

=cut
sub code_in(&) {
    my ($callback) = validate_pos(@_, { type => CODEREF });
    return Stream::Simple::CodeIn->new($callback);
}

=item B<array_seq($list)>

Obsolete alias for C<array_in()>. C<_seq> postfixes are reserved for C<PPB::Join> objects or at least for sorted sequences.

=cut
*array_seq = \&array_in;

=item B<< code_out($write) >>

=item B<< code_out($write, $commit) >>

B<$write> and B<$commit> should be coderefs.

Creates anonymous output stream which calls specified callback on every C<write> call.
It also will call commit callback, if it is specified and C<commit> is called.

This is just another version of C<processor()> from C<Stream::Out>. I think C<processor()> will become deprecated someday, just to keep stream base classes clean and to make C<Stream::Simple> consistent and complete collection of common procedural-style stream builders.

=cut
sub code_out(&;&) {
    my ($callback, $commit_callback) = @_;
    croak "Expected callback" unless ref($callback) eq 'CODE';
    croak "Expected commit callback" if ($commit_callback && ref($commit_callback) ne 'CODE');
    # alternative constructor
    return Stream::Simple::CodeOut->new($callback, $commit_callback);
}

=item B<< coro_out($coderef) >>

Creates anonymous output stream which calls specified callback on every C<write> call in coros.

=cut
sub coro_out {
    my ($threads, $out) = validate_pos(
        @_,
        { type => SCALAR, regex => qr/^\d+$/ },
        { type => OBJECT },
    );
    $out->DOES('Stream::Out') or croak "Expected Stream::Out object, got $out";

    return coro_filter($threads => filter(sub {
        $out->write(@_);
        return;
    }, sub {
        $out->commit();
        return;
    })) | code_out(sub{});
}

=item B<< memory_storage() >>

Construct new in-memory storage, i.e. instance of L<Stream::MemoryStorage>.

=cut
sub memory_storage() {
    return Stream::MemoryStorage->new();
}

=item B<< coro_filter($threads => $filter) >>

Create L<Stream::Filter::Coro> filter.

C<$filter> can be a callback returning C<Stream::Filter> object, or an actual C<Stream::Filter> object.
Second mode is deprecated and can cause cryptic bugs if filter keeps any state.

=cut
sub coro_filter($$) {
    my ($threads, $filter) = validate_pos(
        @_,
        { type => SCALAR, regex => qr/^\d+$/ },
        { type => OBJECT },
    );
    $filter->DOES('Stream::Filter') or croak "Expected filter object, got $filter";

    return Stream::Filter::Coro->new(
        threads => $threads,
        filter => $filter,
    );
}

=back

=cut

1;
