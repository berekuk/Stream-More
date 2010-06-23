package Stream::Queue::Chunk;

use strict;
use warnings;

use parent qw(Stream::In);

=head1 NAME

Stream::Queue::Chunk - represents one client chunk

=head1 METHODS

=over

=cut

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

=item B<< load($dir, $id) >>

Construct chunk object corresponding to existing chunk.

=cut
sub load {
    my ($class, $dir, $id) = validate_pos(@_, 1, { type => SCALAR }, { type => SCALAR, regex => qr/^\d+$/ });

    return unless -e "$dir/$id.chunk"; # this check is unnecessary, but it reduces number of fanthom lock files
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

=item B<< id() >>

Get chunk id.

=cut
sub id {
    my $self = shift;
    return $self->{id};
}

=item B<< remove() >>

Remove chunk and all related files.

=cut
sub remove {
    my $self = shift;
    my $prefix = "$self->{dir}/$self->{id}";
    xunlink("$prefix.chunk");
    xunlink("$prefix.status");
    xunlink("$prefix.status.lock") if -e "$prefix.status.lock";
    xunlink("$prefix.lock");
}

=back

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

