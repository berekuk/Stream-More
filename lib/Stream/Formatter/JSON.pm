package Stream::Formatter::JSON;

use strict;
use warnings;

=head1 NAME

Stream::Formatter::JSON - JSON formatter

=cut

use JSON;
use Stream::Formatter 0.6.0;
use base qw(Stream::Formatter);
use Stream::Filter qw(filter);
use Stream::Utils qw(catalog);

sub write_filter {
    my $json = JSON->new;

    if ($JSON::VERSION < 2) {
        return filter(sub {
            my $item = shift;
            $json->objToJson({ data => $item });
        }) | catalog->filter('str2line') ;
    }
    else {
        $json->allow_nonref(1);
        return filter(sub {
            my $item = shift;
            $json->encode({ data => $item }); # we have to keep this precaution because someone can upgrade from 1 to 2 version of JSON.pm
        }) | catalog->filter('str2line') ;
    }
}

sub read_filter {
    my $json = JSON->new;
    if ($JSON::VERSION < 2) {
        return catalog->filter('line2str') | filter(sub {
            my $item = shift;
            my $decoded = $json->jsonToObj($item);
            return $decoded->{data};
        });
    }
    else {
        $json->allow_nonref(1);
        return catalog->filter('line2str') | filter(sub {
            my $item = shift;
            my $decoded = $json->decode($item);
            return $decoded->{data};
        });
    }
}

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

