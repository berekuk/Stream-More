package Stream::Formatter::JSON;

# ABSTRACT: JSON formatter

use strict;
use warnings;

# forcing XS parser - JSON.pm older than 2.00 is broken
# TODO - consider JSON::Any instead (but it can load broken JSON.pm, that would be bad)
use JSON::XS;

use Stream::Formatter 0.6.0;
use parent qw(Stream::Formatter);
use Stream::Filter qw(filter);
use Stream::Utils qw(catalog);

sub write_filter {
    my $json = JSON::XS->new->allow_nonref;

    return filter(sub {
        my $item = shift;
        return $json->encode({ data2 => $item })."\n";
    });
}

sub read_filter {
    my $json = JSON::XS->new->allow_nonref;
    my $filter = filter(sub {
        my $item = shift;

        my $decoded = $json->decode($item);

        # We'll stop wrapping data in 'data2' hash in the next release.
        # For the smoother upgrade, we assert that 'data2' key is actually present in data.
        #
        # By now, this code is the main execution path.
        # In the next release, it'll become legacy execution path.
        # And in the release after that, it'll be removed completely.
        if (ref $decoded eq 'HASH' and scalar(keys %$decoded) == 1 and exists $decoded->{data2}) {
            return $decoded->{data2};
        }

        return $decoded;
    });
}

1;
