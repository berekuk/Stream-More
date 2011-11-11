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
        return $json->encode($item)."\n";
    });
}

sub read_filter {
    my $json = JSON::XS->new->allow_nonref;
    my $filter = filter(sub {
        my $item = shift;

        my $decoded = $json->decode($item);

        if (ref $decoded eq 'HASH' and scalar(keys %$decoded) == 1 and exists $decoded->{data2}) {
            return $decoded->{data2}; # legacy mode
        }
        return $decoded;
    });
}

1;
