package Stream::Formatter::JSON;

use strict;
use warnings;

=head1 NAME

Stream::Formatter::JSON - JSON formatter

=cut

# forcing XS parser - JSON.pm older than 2.00 is broken
# TODO - consider JSON::Any instead (but it can load broken JSON.pm, that would be bad)
use JSON::XS;

use Stream::Formatter 0.6.0;
use parent qw(Stream::Formatter);
use Stream::Filter qw(filter);
use Stream::Utils qw(catalog);

sub write_filter {
    my $json = JSON::XS->new;

    return filter(sub {
        my $item = shift;
        return $json->encode({ data2 => $item })."\n";
    });
}

sub read_filter {
    my $json = JSON::XS->new;
    my $legacy_filter = catalog->filter('line2str');
    my $filter = filter(sub {
        my $item = shift;

        # legacy mode, to be removed
        if ($item =~ /^{"data"/) {
            $item = $legacy_filter->write($item);
            return $json->decode($item)->{data};
        }

        my $decoded = $json->decode($item);
        return $decoded->{data2};
    });
}

1;
