#!/usr/bin/perl

use strict;
use warnings;

use lib 't/lib';
use lib 'lib';

use parent qw(Test::Class);
use Stream::Test::Out;

use Test::More;
use Test::Exception;

use Streams qw(filter process);
use Stream::Simple qw(code_in array_in code_out coro_filter coro_out memory_storage);

# array_in - read (5)
sub array_in_read :Test(5) {
    my $s = array_in([5,6,7,8]);
    ok($s->isa('Stream::In'), 'array_in is a stream');
    is(scalar($s->read), 5, 'read returns first array element');
    is(scalar($s->read), 6, 'read returns first array element');
    is(scalar($s->shift), 7, 'shift is a synonim of read');
    $s->read;
    is(scalar($s->read), undef, 'last read return undef');
}

sub array_in_read_chunk :Test(3) {
    my $s = array_in([5,6,7]);
    is_deeply($s->read_chunk(2), [5,6], 'read_chunk works too');
    is_deeply($s->read_chunk(2), [7], 'second read_chunk returns remaining elements');
    is_deeply(scalar($s->read_chunk(2)), undef, 'subsequent read_chunk returns undef');
    $s->commit; # does nothing, but shouldn't fail
}

sub code_out_test :Test(6) {
    my @data_in = ('a', 5, { x => 'y' });
    my @data_out;

    throws_ok(sub { &code_out('a') }, qr/Expected callback/, 'code_out throws exception when parameter is not a coderef');
    throws_ok(sub { &code_out(sub { }, 'a') }, qr/Expected commit callback/, 'code_out throws exception when second parameter is not a coderef');

    my $p = code_out(sub { push @data_out, shift });
    ok($p->isa('Stream::Out'), 'code_out result looks like output stream');
    for (@data_in) {
        $p->write($_);
    }
    $p->commit;
    is_deeply(\@data_out, \@data_in, 'code_out result works as anonymous output stream');

    my @buffer;
    @data_out = ();
    $p = code_out(sub { push @buffer, shift}, sub { push @data_out, splice @buffer });
    for (@data_in) {
        $p->write($_);
    }
    is_deeply(\@data_out, [], 'code_out waites for commit');

    $p->commit;
    is_deeply(\@data_out, \@data_in, 'code_out done commit callback');
}

sub code_in_test :Test(5) {
    my $n = 5;
    my $in = code_in(sub {
        $n--;
        return $n || undef;
    });
    is($in->read, 4);
    is($in->read, 3);
    is($in->read, 2);
    is($in->read, 1);
    is($in->read, undef);
}

sub coro_filter_test :Test(1) {
    ok coro_filter(5 => filter {})->isa('Stream::Filter::Coro');
}

sub coro_out_die :Test(1) {
    use Coro::AnyEvent;

    my $code_out = sub {
        return code_out(
            sub {
                Coro::AnyEvent::sleep(2);
                die "test die";
            }
        )
    };

    my $coro_out = coro_out(3 => $code_out);

    throws_ok(sub {
        process(
            array_in([ 1 .. 5 ])
            => $coro_out,
            { commit => 0 },
        );
    }, qr/test die/, 'coro_out throws exception when there is an exception in nested sub');
}

sub coro_out_test :Test(5) {
    my $id = 0;
    my %res_hash;

    use Coro::AnyEvent;

    my $code_out = sub {
        my $cur_id = $id++;
        my @items;
        return code_out(
            sub {
                my $item = shift;
                Coro::AnyEvent::sleep(1);
                push @items, $item;
                return $item;
            },
            sub {
                push @{$res_hash{$cur_id}}, @items;
                @items = ();
            }
        )
    };

    my $coro_out = coro_out(5 => $code_out);
    ok $coro_out->isa('Stream::Out');

    my $start = time;

    process(
        array_in([ 1 .. 10])
        => $coro_out
    );

    my $end = time;
    cmp_ok $end - $start, '<', 2.5;
    cmp_ok $end - $start, '>', 1.5;

    is_deeply
    [0..4],
    [ sort { $a <=> $b } keys %res_hash ];

    my @result;
    foreach my $key (keys %res_hash) {
        push @result, @{$res_hash{$key}};
    }

    is_deeply
    [1 .. 10],
    [ sort { $a <=> $b } @result ];
}

Test::Class->runtests(
    __PACKAGE__->new,
    Stream::Test::Out->new(sub {
        code_out(sub {
            # ignore all data
        })
    }),
);

