use Test::More;
use lib 'lib';

use Streams;
use Stream::Simple::SwitchOut;
use Stream::Simple qw/code_out array_in switch_out/;
use Stream::MemoryStorage;
use Benchmark qw/:all/;

{ # ISA
    my @res = ();
    my $out = Stream::Simple::SwitchOut->new(sub { $_[0]->{type} }, { 'good' => code_out(sub { push @res, shift; }), 'bad' => code_out(sub {}) });

    isa_ok($out, 'Stream::Out');
}

{ # Switch
    my @good = ();
    my @bad = ();
    my $out = Stream::Simple::SwitchOut->new(sub { $_[0]->{type} }, { 'good' => code_out(sub { push @good, shift; }), 'bad' => code_out(sub { push @bad, shift; }) });

    process( array_in([ { type => 'good' }, { type => 'bad' }, { type => 'good' } ]) => $out );

    is(scalar @good, 2, 'items processed - good');
    is(scalar @bad, 1, 'items processed - bad');
}

{ # default => null
    my @res = ();
    my $out = Stream::Simple::SwitchOut->new(sub { $_[0]->{type} }, { 'good' => code_out(sub { push @res, shift; }) });

    process( array_in([ { type => 'good' }, { type => 'bad' }, { type => 'good' } ]) => $out );

    is(scalar @res, 2, 'items processed - default - null');
}

{ # default
    my @res = ();
    my @default = ();
    my $out = Stream::Simple::SwitchOut->new(sub { $_[0]->{type} }, { 'good' => code_out(sub { push @res, shift; }), 'default' => code_out(sub { push @default, shift;})});

    process( array_in([ { type => 'good' }, { type => 'bad' }, { type => 'good' }, { test => 'undef' }, { test => 'sth else', type => 'mystic' } ]) => $out );

    is(scalar @res, 2, 'items processed - default - null');
    is(scalar @default , 3, 'items processed - default - collect');
}

{ # write_chunk
    my @res = ();
    my $out = Stream::Simple::SwitchOut->new(sub { $_[0]->{type} }, { 'good' => code_out(sub { push @res, shift; }) });

    $out->write({ type => 'good' }, { type => 'bad' }, { type => 'good' });
    is(scalar @res, 0, 'no write items unless chunk-size');

    $out->write_chunk([ { type => 'good' }, { type => 'bad' }, { type => 'good' } ]);
    is(scalar @res, 0, 'no write_chunk items unless chunk-size');

    $out->commit();

    is(scalar @res, 4, 'items processed');
}

{ # >>> simple tests
    my $simple = switch_out { $_[0]->{test} } { test => code_out(sub{}) };

    isa_ok($simple, 'Stream::Out');

    eval {
        $simple = switch_out { $_[0]->{test} } { test => {} };
    };
    like($@, qr/Expected stream pipe/, 'check arguments');

    eval {
        $simple = switch_out { $_[0]->{test} } { test => '123' };
    };
    like($@, qr/Expected stream pipe/, 'check arguments');

    eval {
        $simple = switch_out { $_[0]->{test} } [123];
    };
    like($@, qr/Expected cases - hashref/, 'check arguments');
}

{
    my @res = ();
    my $commit_count = 0;
    my $out = switch_out { $_[0]->{type} } { 'good' => code_out(sub { push @res, shift; }, sub { $commit_count++; }) };

    process( array_in([ { type => 'good' }, { type => 'bad' }, { type => 'good' }, { test => 'undef' }, { test => 'sth else', type => 'mystic' } ]) => $out );

    is(scalar @res, 2, 'items processed - default - null');
    is($commit_count, 1, 'commites once');

    process( array_in([ { type => 'bad' }, { test => 'undef' }, { test => 'sth else', type => 'mystic' } ]) => $out );
    is(scalar @res, 2, 'no good items');
    is($commit_count, 1, 'no commit whether not dirty');
}

{
    my $out = switch_out { $_[0]->{type} } { 'good' => Stream::MemoryStorage->new(), bad => Stream::MemoryStorage->new(), default => Stream::MemoryStorage->new() };
    my $items = [];

    push @$items, { type => (($_ % 2 == 0)?'good':($_ % 3)?'bad':'test'), id => $_ } for (0..10000);

    my $r = timethese( 5000, {
        'write' => sub {
            process( array_in($items) => $out, { chunk_size => 1, commit_step => 10000 });
        },
        'write_chunk 1000' => sub {
            process( array_in($items) => $out, { chunk_size => 1000, commit_step => 10000 });
        },
        'write_chunk 10000' => sub {
            process( array_in($items) => $out, { chunk_size => 10000, commit_step => 10000 });
        },
    } );

    like(@{cmpthese $r}->[1]->[4], qr/-\d+%/, 'write - slower');
}

done_testing;
