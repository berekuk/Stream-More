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
use base qw(Stream::Formatter);
use Stream::Filter qw(filter);
use Stream::Utils qw(catalog);

sub write_filter {
    my $json = JSON::XS->new;

    return filter(sub {
        my $item = shift;
        $json->encode({ data => $item });
    }) | catalog->filter('str2line');
}

sub read_filter {
    my $json = JSON::XS->new;
    return catalog->filter('line2str') | filter(sub {
        my $item = shift;
        my $decoded = $json->decode($item);
        return $decoded->{data};
    });
}

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

