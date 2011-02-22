package Stream::Bin::Filter;

use strict;
use warnings;

use Yandex::Version '{{DEBIAN_VERSION}}';

=head1 NAME

Stream::Bin::Filter - base class for many of your /usr/bin/ scripts

=head1 SYNOPSIS

use parent qw(Stream::Bin::Filter);

sub write {
    my $line = shift;
    next if /updated/;
    return $line;
}

__PACKAGE__->run( storage => 'bulca.storelog', named_targets => { blogs => 'indexer/blogs', comments => 'indexer/comments' } );

=head1 DESCRIPTION

This module tries to do all common things in data-processing scripts for you.

By now, it's not very successful at that task...

=head1 METHODS

=over

=cut

use Yandex::Logger;

use Carp;
use Getopt::Long 2.33;
use Pod::Usage;
use Scalar::Util qw(blessed);

use Stream::Utils qw(catalog process);
use Params::Validate qw(:all);

use parent qw(Stream::Filter);

=item I<run($params)>

Run processing.

=cut
sub run {
    my $self = shift;
    $self = $self->new unless blessed($self);

    if (caller(1)) {
        # not included from script, do nothing
        return;
    }
    my ($name) = $0 =~ m{^/usr/bin/(.+)} or return;

    my $params = validate(@_, {
        storage => { type => SCALAR },
        target => { type => SCALAR | HASHREF, optional => 1 },
        named_targets => { type => HASHREF, optional => 1 },
    });

    INFO "started";

    my $limit;
    GetOptions(
        'limit=i'   => \$limit,
    ) or pod2usage(2);

    my $storage = $params->{storage};
    unless (blessed $storage) {
        $storage = catalog->storage($storage);
    }
    unless ($storage->isa('Stream::Log')) {
        croak "Stream::Bin::Filter process any storage except Stream::Log";
    }

    my $posdir = "/var/lib/stream/cursor/bin/$name";
    unless (-e $posdir) {
        mkdir $posdir, 0755 or die "mkdir failed: $!";
    }

    if (defined $params->{target} and defined $params->{named_targets}) {
        die "You should specify only one of 'target' or 'named_targets'";
    }

    if ($params->{target}) {
        $params->{named_targets} = { main => $params->{target} };
    }

    unless (defined $params->{named_targets}) {
        die "You should specify one of 'target' or 'named_targets'";
    }

    my $err_count = 0;
    for (my $name = keys %{$params->{named_targets}}) {
        my $target = $params->{named_targets}{$name};
        eval {
            unless (blessed $target) {
                $target = catalog->out($target);
            }

            my $posfile = "$posdir/$name";
            my $in = $storage->stream(Stream::Log::Cursor->new({ PosFile => $posfile }));

            my $processed = process($in => $self | $target, $limit);
            if ($name eq 'main') {
                INFO "$processed processed";
            }
            else {
                INFO "$name: $processed processed";
            }
        };
        if ($@) {
            ERROR "Processing $name failed: $@";
            $err_count++;
        }
    }
    exit $err_count;
}

=back

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

