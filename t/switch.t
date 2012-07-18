use Test::More;
use lib 'lib';

use Streams;
use Stream::Simple::SwitchOut;
use Stream::Simple qw/code_out array_in switch_out/;

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

{ # otherwise => null
    my @res = ();
    my $out = Stream::Simple::SwitchOut->new(sub { $_[0]->{type} }, { 'good' => code_out(sub { push @res, shift; }) });

    process( array_in([ { type => 'good' }, { type => 'bad' }, { type => 'good' } ]) => $out );

    is(scalar @res, 2, 'items processed - otherwise - null');
}

{ # otherwise
    my @res = ();
    my @otherwise = ();
    my $out = Stream::Simple::SwitchOut->new(sub { $_[0]->{type} }, { 'good' => code_out(sub { push @res, shift; }), 'otherwise' => code_out(sub { push @otherwise, shift;})});

    process( array_in([ { type => 'good' }, { type => 'bad' }, { type => 'good' }, { test => 'undef' }, { test => 'sth else', type => 'mystic' } ]) => $out );

    is(scalar @res, 2, 'items processed - otherwise - null');
    is(scalar @otherwise , 3, 'items processed - otherwise - collect');
}

{
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

    is(scalar @res, 2, 'items processed - otherwise - null');
    is($commit_count, 1, 'commites once');

    process( array_in([ { type => 'bad' }, { test => 'undef' }, { test => 'sth else', type => 'mystic' } ]) => $out );
    is(scalar @res, 2, 'no good items');
    is($commit_count, 1, 'no commit whether not dirty');
}

done_testing;
