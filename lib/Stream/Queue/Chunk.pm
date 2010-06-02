package Stream::Queue::Chunk;

use strict;
use warnings;

use parent qw(Stream::In);

use Yandex::X;
use Yandex::Lockf 3.0.0;
use Stream::Formatter::LinedStorable;
use Stream::File::Cursor;
use Stream::File;
use Params::Validate qw(:all);

sub new {
    my ($class, $dir, $id, $data) = validate_pos(@_, 1, { type => SCALAR }, { type => SCALAR, regex => qr/^\d+$/ }, { type => ARRAYREF });
    my $file = "$dir/$id.chunk";
    my $storage = Stream::Formatter::LinedStorable->wrap(
        Stream::File->new("$file.new")
    );
    $storage->write_chunk($data);
    $storage->commit;
    xrename("$file.new" => $file);

    return $class->load($dir, $id);
}

sub load {
    my ($class, $dir, $id) = validate_pos(@_, 1, { type => SCALAR }, { type => SCALAR, regex => qr/^\d+$/ });

    my $lock = lockf("$dir/$id.lock", { blocking => 0 }) or return;
    return unless -e "$dir/$id.chunk";

    my $in = Stream::Formatter::LinedStorable->wrap(
        Stream::File->new("$dir/$id.chunk")
    )->stream(Stream::File::Cursor->new("$dir/$id.status"));

    return bless {
        in => $in,
        lock => $lock,
        id => $id,
        dir => $dir,
    } => $class;
}

sub read {
    my $self = shift;
    return $self->{in}->read;
}

sub commit {
    my $self = shift;
    return $self->{in}->commit;
}

sub id {
    my $self = shift;
    return $self->{id};
}

sub remove {
    my $self = shift;
    my $prefix = "$self->{dir}/$self->{id}";
    xunlink("$prefix.chunk");
    xunlink("$prefix.status");
    xunlink("$prefix.lock");
}

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

