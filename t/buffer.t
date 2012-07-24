#!/usr/bin/perl

use strict;
use warnings;

use parent qw(Test::Class);
use Test::More;
use Test::Deep;
use Test::Fatal;

use lib 'lib';
use PPB::Test::TFiles;

use Yandex::X;

use Stream::Simple qw(array_in code_in);
use Stream::In::Buffer;

sub new {
    my $class = shift;
    my $self = $class->SUPER::new;
    $self->{buffer_options} = { @_ };
    return $self; # all constructor parameters will become options for the buffer
}

# get buffer with defaults + overrides from arguments
sub _buffer {
    my $self = shift;
    my ($in, $options) = @_;
    $options ||= {};
    return Stream::In::Buffer->new($in, {
        dir => "tfiles/",
        %{ $self->{buffer_options} },
        %$options
    });
}

sub setup :Test(setup) {
    PPB::Test::TFiles::import;
}

sub simple :Tests {
    my $self = shift;

    my $in = array_in(["a" .. "z"]);
    my $mq = $self->_buffer($in);

    my $items = $mq->read_chunk(3);
    cmp_deeply($items, [ [ignore(), "a"], [ignore(), "b"], [ignore(), "c"] ]);

    $mq->commit([$items->[0]->[0], $items->[2]->[0]]);
    undef $mq;

    $mq = $self->_buffer($in);
    $items = $mq->read_chunk(2);
    cmp_deeply($items, [ [ignore(), "b"], [ignore(), "d"] ]);
}

sub id :Tests {
    my $self = shift;

    my $in = array_in(["a" .. "z"]);
    my $mq = $self->_buffer($in);

    $mq->read();
    my $item = $mq->read();
    is($item->[0], 1, "autoincrement id");

    undef $mq;
    $mq = $self->_buffer($in);
    $mq->read();
    $mq->read();
    $item = $mq->read();
    is($item->[0], 2, "autoincrement id after reset");

    $mq->commit([0,1]);
    
    undef $mq;
    $mq = $self->_buffer($in);

    $mq->read();
    $item = $mq->read();
    is($item->[0], 3, "autoincrement id after commit");
}

sub lazy :Tests {
    my $self = shift;

    my $arr = ["a" .. "z"];
    my $in = array_in($arr);
    my $mq = $self->_buffer($in);

    $mq->read_chunk(3);
    cmp_ok(scalar(@$arr), ">=", 26-3);
    $mq->read_chunk(5);
    cmp_ok(scalar(@$arr), ">=", 26-8);

    my $arr_size = @$arr;
    undef $mq;
    $mq = $self->_buffer($in);
    $mq->read_chunk(8);
    cmp_ok(scalar(@$arr), "==", $arr_size);
}

sub size :Tests {
    my $self = shift;

    my $in = array_in(["a" .. "z"]);
    my $mq = $self->_buffer($in, { max_chunk_size => 4 });
    my $items = $mq->read_chunk(3);
    $mq->commit([map {$_->[0]} @$items]);
    $items = $mq->read_chunk(3); # lives
    like(exception { $mq->read_chunk(2) }, qr/size exceeded/, "max_chunk_size check");
}

sub lag :Tests {
    my $self = shift;

    my $in = array_in(["a" .. "z"]);
    my $mq = $self->_buffer($in);

    is($mq->lag(), 26);
    my $items = $mq->read_chunk(10);
    cmp_ok($mq->lag(), ">=", 16); #TODO: count uncommited
    $mq->commit([map {$_->[0]} splice @$items, 0, 5]);

    undef $mq; #FIXME: sum lags in all chunks!
    $mq = $self->_buffer($in);
    is($mq->lag(), 21);
    $items = $mq->read_chunk(10);
    cmp_ok($mq->lag(), ">=", 11);
}

sub concurrent :Tests {
    my $self = shift;

    my $in = array_in(["a" .. "z"]);

    my $mq1 = $self->_buffer($in);
    my $mq2 = $self->_buffer($in);

    $mq1->read_chunk(3);
    $mq2->read_chunk(3);

    undef $mq1;
    undef $mq2;

    $mq1 = $self->_buffer($in);
    $mq2 = $self->_buffer($in);

    my $items;
    push @$items, @{$mq1->read_chunk(4)};
    push @$items, @{$mq2->read_chunk(4)};

    cmp_deeply([map { $_->[1] } @$items], bag("a" .. "h"));

    like(exception { $self->_buffer($in, { max_chunk_count => 2 }) }, qr/limit exceeded/);
}

sub read_chunk :Tests {
    my $self = shift;

    my $in = array_in(["a", "b"]);
    my $mq = $self->_buffer($in);

    cmp_deeply($mq->read_chunk(3), [[ ignore(), "a" ], [ ignore(), "b" ]]);
    is($mq->read_chunk(2), undef); # not []!
}

sub buffers :Tests {
    my $self = shift;

    my $in = array_in(["a" .. "z"]);

    my @mq = map { $self->_buffer($in) } (1 .. 4);
    $_->read_chunk(2) for @mq;
    undef @mq;
    
    my $mq1 = $self->_buffer($in);
    my $mq2 = $self->_buffer($in);

    my ($items1, $items2);

    push @$items1, @{$mq1->read_chunk(6)};
    is({ map { ($_->[1] => 1) } @$items1 }->{"i"}, undef, "switch to another unlocked buffer");

    push @$items2, @{$mq2->read_chunk(4)};
    cmp_deeply([map { $_->[1] } @$items1, @$items2], bag("a" .. "j"), "all pending buffers fetched");

    $mq2->commit([map { $_->[0] } @$items2]);
    undef $mq1;
    undef $mq2;

    is(int(xqx("ls tfiles/ | wc -l")), 1, "buffers are merged and purged");
}

sub commit :Tests {
    my $self = shift;

    my $in = array_in(["a" .. "z"]);

    my $mq = $self->_buffer($in);
    my $item = $mq->read();
    is($item->[1], "a");
    $mq->commit([$item->[0]]);
    undef $mq;

    $mq = $self->_buffer($in);
    $item = $mq->read();
    is($item->[1], "b");
    $mq->commit();
    undef $mq;

    $mq = $self->_buffer($in);
    $item = $mq->read();
    is($item->[1], "b");
    like(exception { $mq->commit([42]) }, qr/unknown id/);
}

sub unlink_lockf_race :Tests {
    my $self = shift;

    for (1..5) {
        xfork and next;
        my $in = array_in(["a" .. "z"]);
        eval {
            for (0..100) {
                my $mq = $self->_buffer($in);
            }
        };
        if ($@) {
            warn $@;
            exit(1);
        }
        exit;
    }
    while () {
        last if wait == -1;
        is($?, 0);
    }
}

#__PACKAGE__->new(buffer_class => 'Stream::Buffer::Persistent')->runtests;
#__PACKAGE__->new(buffer_class => 'Stream::Buffer::SQLite')->runtests;
__PACKAGE__->new(buffer_class => 'Stream::Buffer::File')->runtests;
