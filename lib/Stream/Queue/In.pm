package Stream::Queue::In;

# ABSTRACT: input stream for Stream::Queue

=head1 SYNOPSIS

    $in = $queue->stream('client_name');
    $in->read;
    $in->read;
    $in->commit;

=head1 DESCRIPTION

This class is an input stream for L<Stream::Queue> class. It is not parallel itself and locks data after every read() and until the next commit.

It is wrapped in L<Stream::In::DiskBuffer> when constructed via C<stream> method of L<Stream::Queue> storage, though.

=cut

use namespace::autoclean;
use Moose;

use Params::Validate;

use Carp;
use IO::Handle;
use Yandex::Logger;
use Yandex::Lockf 3.0.0;
use Yandex::X;
use Stream::Formatter::LinedStorable;
use Stream::File::Cursor;

sub isa {
    return 1 if $_[1] eq __PACKAGE__;
    $_[0]->next::method if $_[0]->next::can;
} # ugly hack


has 'storage' => (
    is => 'ro',
    isa => 'Stream::Queue',
    required => 1,
);

has 'client' => (
    is => 'ro',
    isa => 'Str', # qr/^[\w\.-]+$/
    required => 1,
);

has 'dir' => (
    is => 'ro',
    isa => 'Str',
    lazy => 1,
    default => sub {
        my $self = shift;
        return $self->storage->dir."/clients/".$self->client."/pos";
    },
);

has 'uncommited' => (
    is => 'ro',
    isa => 'HashRef',
    lazy_build => 1,
);
sub _build_uncommited {
    return {};
}

=head1 METHODS

=over

=item B<< lock() >>

Get lock.

=cut
sub lock {
    my $self = shift;
    $self->check_read_only;
    return lockf($self->dir."/lock");
}

sub _info2in {
    my ($self, $info, $opts) = @_;
    $opts ||= {};
    $opts->{cache} = 1 unless defined $opts->{cache};

    my $cached = $self->uncommited->{in}{ $info->{id} }; # it can already be in "uncommited" cache
    return $cached if $cached;

    # otherwise, create it now
    my $file_in = eval { Stream::File->new($info->{file}) }; # it probably means that gc already removed it
    return unless $file_in;

    my $new = $self->read_only ? "new_ro" : "new";
    my $in = Stream::Formatter::LinedStorable->wrap($file_in)->stream(Stream::File::Cursor->$new($self->dir."/$info->{id}.pos"));
    if ($opts->{cache}) {
        $self->uncommited->{in}{ $info->{id} } = $in;
    }

    return $in;
}

=item B<< chunk_lag($chunk_info) >>

The lag of a single chunk

=cut

sub chunk_lag {
    my ($self, $info) = @_;
    my $in = $self->_info2in($info, { cache => 0 });
    return unless $in; # chunk already removed?
    return $in->lag;
}

sub lag {
    my $self = shift;
    my $lag = 0;
    for my $info ($self->storage->chunks_info) {
        my $chunk_lag = $self->chunk_lag($info);
        next unless defined $chunk_lag;
        $lag += $chunk_lag;
    }
    return $lag;
}

sub read_chunk {
    my $self = shift;
    my $chunk_size = shift;

    my $lock;
    $lock = $self->lock unless $self->uncommited->{lock} or $self->read_only;

    my @chunks_info = $self->storage->chunks_info;

    my $data;
    for my $info (@chunks_info) {
        my $in = $self->_info2in($info); # find in cache or open new chunk
        next unless $in;

        my $portion_size = $chunk_size;
        $portion_size -= @$data if $data;
        my $portion_data = $in->read_chunk($portion_size) or next;
        push @$data, @$portion_data;
        last if @$data >= $chunk_size;
    }
    if ($data and @$data and $lock) {
        # returning non-empty chunk, keeping this client locked until "commit" will be called
        $self->uncommited->{lock} = $lock;
    }
    return $data;
}

=item C<< read() >>

Read new item from queue.

=cut
sub read {
    my $self = shift;
    my $chunk = $self->read_chunk(1);
    return unless $chunk;
    return unless @$chunk > 0;
    croak "invalid chunk" unless @$chunk == 1;
    return @$chunk;
}

sub commit {
    my $self = shift;

    return unless $self->has_uncommited;
    for (values %{ $self->uncommited->{in} }) {
        $_->commit;
    }
    $self->clear_uncommited;
    return ();
}

=item C<< pos($id) >>

Get position in bytes for given chunk id.

=cut
sub pos {
    my ($self, $id) = @_;
    my $cursor_file = $self->dir."/$id.pos";
    unless (-e $cursor_file) {
        return 0;
    }
    my $new = $self->read_only ? "new_ro" : "new";
    my $cursor = Stream::File::Cursor->$new($cursor_file);
    return $cursor->position;
}

=item C<< gc() >>

Cleanup lost files.

=cut
sub gc {
    my $self = shift;
    $self->check_read_only;

    my $lock = $self->lock;
    opendir my $dh, $self->dir or die "Can't open '".$self->dir."': $!";
    while (my $file = readdir $dh) {
        next if $file eq '.';
        next if $file eq '..';
        next if $file =~ /(^meta|^lock$)/;
        my $unlink = sub {
            xunlink($self->dir."/$file");
        };
        my $id;
        if (($id) = $file =~ /^(\d+)\.(?: pos | pos\.lock )$/x) {
            unless (-e $self->storage->dir."/$id.chunk") {
                $unlink->();
                DEBUG "[".$self->client."] Lost file $file for $id chunk removed";
            }
            next;
        }

        WARN "[".$self->client."] Removing unknown file $file";
        $unlink->();
    }
    closedir $dh or die "Can't close '".$self->dir."': $!";
}

=back

=cut

with
    'Stream::Moose::In::Chunked',
    'Stream::Moose::In::Lag',
    'Stream::Moose::In::ReadOnly',
;

1;
