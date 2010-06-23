#!/usr/bin/perl

use strict;
use warnings;

use Test::More tests => 8;
use Test::Mock::LWP::Dispatch;
use HTTP::Response;

use lib 'lib';

use Test::Exception;
use Stream::Propagate;

# simple write+commit
{
    my %uri2contents;
    my $map_id = $mock_ua->map(qr{^\Qhttp://accept.stream.com:1248\E}, sub {
        my $request = shift;
        push @{ $uri2contents{$request->uri} }, $request->content;
        return HTTP::Response->new(200, 'OK');
    });

    my $out = Stream::Propagate->new({
        name => 'blah',
        endpoint => 'http://accept.stream.com:1248',
    });
    $out->write('abc');
    $out->write({ x => 5, y => 6 });
    $out->commit;

    is_deeply(\%uri2contents, {
        'http://accept.stream.com:1248/write?name=blah&format=json' => [
        q({"data":"abc"}
{"data":{"y":6,"x":5}}
) # TODO - replace exact matching with regex since x/y order is unpredictable
        ]}, 'commit commits, default format is json');
    $mock_ua->unmap($map_id);
}

# error handling
{
    my $map_id = $mock_ua->map(qr{^\Qhttp://accept.stream.com:1248\E}, sub {
        return HTTP::Response->new(500, 'Oops');
    });

    my $out = Stream::Propagate->new({
        name => 'blah',
        endpoint => 'http://accept.stream.com:1248',
    });
    $out->write('abc');
    $out->write({ x => 5, y => 6 });

    throws_ok(sub {
        $out->commit;
    }, qr/Propagating into blah .* failed: .*Oops/, 'commit fails when POST fails');
    $mock_ua->unmap($map_id);
}

# checks in plain format mode
{
    my $out = Stream::Propagate->new({
        name => 'blah',
        endpoint => 'http://accept.stream.com:1248',
        format => 'plain',
    });
    throws_ok(sub {
        $out->write(['abc']);
    }, qr/Only strings/, 'write fails when format is plain and item is non-string');

    throws_ok(sub {
        $out->write('abc');
    }, qr/don't end with \\n/, "write fails when format is plain and item don't end in \\n");
}

# plain format
{
    my %uri2contents;
    my $map_id = $mock_ua->map(qr{^\Qhttp://accept.stream.com:1248\E}, sub {
        my $request = shift;
        push @{ $uri2contents{$request->uri} }, $request->content;
        return HTTP::Response->new(200, 'OK');
    });

    my $out = Stream::Propagate->new({
        name => 'blah',
        endpoint => 'http://accept.stream.com:1248',
        format => 'plain',
    });
    $out->write("abc\n");
    $out->write("def\n");
    $out->commit;

    is_deeply(\%uri2contents, {
        'http://accept.stream.com:1248/write?name=blah&format=plain' => [
        q(abc
def
)
        ]}, 'POST in plain format');
    $mock_ua->unmap($map_id);
}

# storable format
{
    my %uri2contents;
    my $map_id = $mock_ua->map(qr{^\Qhttp://accept.stream.com:1248\E}, sub {
        my $request = shift;
        push @{ $uri2contents{$request->uri} }, $request->content;
        return HTTP::Response->new(200, 'OK');
    });

    my $out = Stream::Propagate->new({
        name => 'blah',
        endpoint => 'http://accept.stream.com:1248',
        format => 'storable',
    });
    $out->write("abc");
    $out->write({ x => 5, y => 6 });
    $out->commit;

    my $expected_url = 'http://accept.stream.com:1248/write?name=blah&format=storable';
    is_deeply( [ keys %uri2contents ], [ $expected_url ], 'url when POSTing in storable format');

    my @lines = split /\n/, $uri2contents{$expected_url}[0];

    use Stream::Formatter::LinedStorable;
    my $filter = Stream::Formatter::LinedStorable->new->read_filter;
    is(
        $filter->write("$lines[0]\n"),
        'abc',
        'plain string decoded from storable format'
    );
    is_deeply(
        $filter->write("$lines[1]\n"),
        { x => 5, y => 6 },
        'hashref decoded from storable format'
    );
    $mock_ua->unmap($map_id);
}
