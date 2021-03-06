package Stream::Queue;

# ABSTRACT: output stream supporting parallel writing

=head1 SYNOPSIS

    $queue = Stream::Queue->new({
        dir => '/var/spool/yandex-ppb-something/queue',
    });
    $queue->write("abc");
    $queue->write("def");
    $queue->commit;

    $reader = $queue->stream('client1');
    print $reader->read(); # abc
    print $reader->read(); # def
    $reader->commit;
    $reader = $queue->stream('client1');
    print $reader->read(); # undef

    $queue->stream('client2');
    print $reader->read(); # abc

=head1 DESCRIPTION

C<Stream::Queue> and C<Stream::Queue::In> implement local file-based FIFO queue which can be written and read from in parallel, cleanup itself and support L<Streams> API.

=head1 METHODS

=over

=cut

use Moose;

sub isa {
    return 1 if $_[1] eq __PACKAGE__;
    $_[0]->next::method if $_[0]->next::can;
} # ugly hack

use namespace::autoclean;

use Yandex::Logger;
use Yandex::Lockf 3.0;

use Format::Human::Bytes;
use Params::Validate;
use File::Path qw(rmtree);
use File::Spec;
use Set::Object;
use IO::Handle;
use Carp;
use Yandex::Persistent 1.3.0;
use Yandex::X 1.1.0;

use Stream::In::DiskBuffer;
use Stream::Queue::In;
use Stream::Queue::BigChunkDir;

use Moose::Util::TypeConstraints;

our $CURRENT_VERSION = 2;

=item B<< new( dir => $dir ) >>

Constructor. C<$dir> must be writable and empty on first invocation.

See source code for other possible attributes.

=cut

# FIXME - this shouldn't be 'rw', but we need to call rel2abs in BUILD
has 'dir' => (
    is => 'rw',
    isa => 'Str',
    required => 1,
);

has 'format' => (
    is => 'ro',
    isa => subtype( as 'Str', where { $_ eq 'storable' } ),
    default => 'storable',
);

has 'gc_period' => (
    is => 'ro',
    isa => 'Int',
    default => 300,
);

has 'max_chunk_size' => (
    is => 'ro',
    isa => 'Int',
    default => 50 * 1024 * 1024,
);

has 'max_chunk_count' => (
    is => 'ro',
    isa => 'Int',
    default => 100,
);

has '_gc_timestamp_cached' => (
    is => 'rw',
    isa => 'Int',
    init_arg => undef,
);

has 'chunk_dir' => (
    is => 'ro',
    isa => class_type('Stream::Queue::BigChunkDir'),
    default => sub {
        my $self = shift;

        return Stream::Queue::BigChunkDir->new({
            dir => $self->dir,
            max_chunk_size => $self->max_chunk_size,
            max_chunk_count => $self->max_chunk_count,
        });
    },
    lazy => 1,
    init_arg => undef,
);

has 'out' => (
    is => 'ro',
    isa => class_type('Stream::Out'),
    lazy_build => 1,
);

sub _meta_file {
    my $self = shift;
    return $self->dir."/queue.meta";
}

sub _build_out {
    my $self = shift;
    $self->chunk_dir->out;
}

sub BUILD {
    my $self = shift;
    $self->dir( File::Spec->rel2abs($self->dir) );
    # TODO - check that dir is writable
    unless (-d $self->dir) {
        $self->check_read_only;
        xmkdir($self->dir);
    }
    unless (-d $self->dir."/clients") {
        $self->check_read_only;
        xmkdir($self->dir."/clients");
    }
    $self->chunk_dir;
    $self->try_convert;
    $self->try_gc;
}

=item B<< write($item) >>

Write new item into queue.

Item can be any string or serializable structure, but it can't be C<undef>.

=cut
sub write($$) {
    my ($self, $item) = @_;
    unless (defined $item) {
        croak "Can't write undef";
    }
    $self->out->write($item);
}

=item B<< commit() >>

Commit written items.

=cut
sub commit {
    my $self = shift;
    unless ($self->has_out) {
        DEBUG "Nothing to commit";
        return;
    }
    $self->out->commit;
    $self->clear_out; # out recreated after every commit, otherwise we would hold one old chunk opened for too long
    $self->try_gc;
}

=item B<< register_client($client_name) >>

Register new client with this queue.

Once registered, client must read queue regularly; otherwise queue will become overfill after some time.

=cut
sub register_client {
    my $self = shift;
    my ($client) = validate_pos(@_, { regex => qr/^[\w\.-]+$/ });
    $self->check_read_only();

    my $status = $self->meta_persistent; # global lock
    if ($self->has_client($client)) {
        return; # already registered
    }
    INFO "Registering $client at ".$self->dir;
    xmkdir($self->dir."/clients/$client");
    xmkdir($self->dir."/clients/$client/buffer");
    xmkdir($self->dir."/clients/$client/pos");
}

=item B<< unregister_client($client_name) >>

Unregister client named C<$client_name>.

=cut
sub unregister_client {
    my $self = shift;
    my ($client) = validate_pos(@_, { regex => qr/^[\w\.-]+$/ });
    $self->check_read_only();
    my $status = $self->meta_persistent; # global lock
    unless ($self->has_client($client)) {
        WARN "No such client '$client', can't unregister";
        return;
    }
    rmtree($self->dir."/clients/$client");
}

=item B<< has_client($client_name) >>

Check whether queue has client C<$client_name> registered.

=cut
sub has_client {
    my $self = shift;
    my ($client) = validate_pos(@_, { regex => qr/^[\w\.-]+$/ });
    unless (-d $self->dir."/clients/$client") {
        return;
    }
    return 1;
}

sub in {
    my $self = shift;
    my ($client) = validate_pos(@_, { regex => qr/^[\w\.-]+$/ });

    return Stream::In::DiskBuffer->new(
        Stream::Queue::In->new({
            storage => $self,
            client => $client,
            read_only => $self->read_only,
        }) => $self->dir."/clients/$client/buffer",
        {
            read_only => $self->read_only,
            read_lock => 0,
        }
    );
}

=back

=head1 INTERNAL METHODS

These methods are not a part of public API and are used only by C<Stream::Queue::*> modules.

=over

=item B<< meta_persistent() >>

Get metadata object.

It's implemented as simple persistent, so it also works as global queue lock.

=cut
sub meta_persistent {
    my $self = shift;
    return Yandex::Persistent->new($self->_meta_file, { auto_commit => 0, format => 'json', read_only => $self->read_only });
}

=item B<< chunks_info() >>

Proxy method for chunks_info() method from C<Stream::Queue::BigChunkDir>.

=cut
sub chunks_info {
    my $self = shift;
    return $self->chunk_dir->chunks_info;
}

sub client_names {
    my $self = shift;
    my @client_names = map { File::Spec->abs2rel( $_, $self->dir."/clients" ) } grep { -d $_ } glob $self->dir."/clients/*";
    return @client_names;
}

=item B<< clients() >>

Get all storage clients as plain list.

=cut
sub clients {
    my $self = shift;
    return map { $self->stream($_) } $self->client_names;
}

=item B<< try_gc() >>

Do garbage collecting if it's necessary.

=cut
sub try_gc {
    my $self = shift;

    return if $self->read_only;

    if ( $self->_gc_timestamp_cached and time < $self->_gc_timestamp_cached + $self->gc_period ) {
        return;
    }

    my $meta = $self->meta_persistent;
    unless (defined $meta->{gc_timestamp}) {
        $meta->{gc_timestamp} = time;
        $self->_gc_timestamp_cached($meta->{gc_timestamp});
        $meta->commit;
        return;
    }
    $self->_gc_timestamp_cached( $meta->{gc_timestamp} );

    if (time > $meta->{gc_timestamp} + $self->gc_period) {
        $self->_gc_timestamp_cached( $meta->{gc_timestamp} = time );
        $meta->commit;
        $self->gc($meta);
    }
}

=item B<< try_convert() >>

Check if queue is in old format and needs to be converted.

=cut
sub try_convert {
    my $self = shift;

    my $meta = $self->meta_persistent;
    return if $meta->{version} and $meta->{version} == 2;

    $self->check_read_only();

    my $cd_meta = $self->chunk_dir->meta;
    if ($cd_meta->{id}) {
        if (not defined $meta->{version}) {
            croak "version field not found in ".$self->dir." metadata";
        }
        if ($meta->{version} == 1) {
            $self->convert($meta);
        }
        elsif ($meta->{version} != $CURRENT_VERSION) {
            croak "Unknown queue version '$meta->{version}'";
        }
    }
    else {
        # new queue
        $meta->{version} = $CURRENT_VERSION;
        $meta->commit;
    }
}

=item B<< convert() >>

Convert queue from old format. All client positions will be lost, sorry.

=cut
sub convert {
    my ($self, $meta) = @_;
    INFO "Converting from version format 1";
    $self->check_read_only();

    my @client_names = $self->client_names;
    for my $client (@client_names) {
        for my $dir ($self->dir."/clients/$client/buffer", $self->dir."/clients/$client/pos") {
            xmkdir($dir) unless -d $dir;
        }

        my $rename_all = sub {
            my $target = shift;
            my @list = @_;
            for my $file (@list) {
                my $renamed_file = $file;
                $renamed_file =~ s{^(.*)/(.*)$}{$1/$target/$2};
                xrename($file => $renamed_file);
            }
        };
        $rename_all->('buffer', glob $self->dir."/clients/$client/*.chunk");
        $rename_all->('buffer', glob $self->dir."/clients/$client/*.status");
        $rename_all->('buffer', $self->dir."/clients/$client/meta") if -e $self->dir."/clients/$client/meta";
        $rename_all->('pos', glob $self->dir."/clients/$client/*.pos");

        # cleanup
        for my $file (glob $self->dir."/clients/$client/*") {
            next if $file =~ m{/buffer$};
            next if $file =~ m{/pos$};
            next if -d $file; # shouldn't happen anyway
            DEBUG "Removing $file";
            xunlink($file);
        }
    }

    $meta->{version} = $CURRENT_VERSION;
    $meta->commit;
    INFO "Converted successfully";
}

=item B<< gc() >>

Remove all unused chunks and cleanup clients' statuses.

This method is called automatically from time to time, so usually you shouldn't call it manually.

=cut
sub gc {
    my ($self, $meta) = @_;
    $self->check_read_only();

    $meta ||= $self->meta_persistent; # queue is locked when gc is active

    my $cd_lock = $self->chunk_dir->lock; # chunk dir is locked too

    DEBUG "Starting gc";

    my @chunks_info = $self->chunk_dir->chunks_info;
    my @clients = $self->clients;

    # make sure that we switch to new output chunk on every gc;
    # otherwise, some client can get stuck with .pos file belonging to the old chunk with the same id.
    if ($self->has_out) {
        $self->out->commit;
        $self->clear_out;
    }
    $self->chunk_dir->next_out;

    CHUNK:
    for my $info (@chunks_info) {
        DEBUG "processing chunk $info->{id}";
        my $lock = lockf($info->{lock_file}, { blocking => 0 }) or next;
        for my $client (@clients) {
            my $lag = $client->in->chunk_lag($info);
            next CHUNK if not defined $lag; #FIXME: O_o
            next CHUNK if $lag > 0;
        }
        DEBUG "Removing $info->{file}";
        # chunk can be safely removed
        xunlink($info->{lock_file}) if -e $info->{lock_file}; # theoretically, previous gc could fail after this unlink and before unlinking chunk itself
        xunlink($info->{file});
    }

    for (@clients) {
        $_->gc();
        $_->in->gc();
    }
}

sub description {
    my $self = shift;
    my $max_size = $self->max_chunk_size * $self->max_chunk_count;
    my $real_size = 0;
    for my $info ($self->chunk_dir->chunks_info) {
        $real_size += -s $info->{file};
    }
    my $occupancy = $real_size / $max_size;
    $occupancy = int($occupancy / 1000) * 10;
    return
        "dir: ".$self->dir."\n"
        ."max size: ".Format::Human::Bytes->new->base2($max_size)."\n"
        ."occupancy: ".$occupancy."%";
    ;
}

=back

=cut

use Stream::Moose::Role::AutoOwned;
with
    'Stream::Moose::Out::Easy',
    'Stream::Moose::Storage',
    'Stream::Moose::Out::ReadOnly',
    'Stream::Moose::Storage::ClientList',
    'Stream::Moose::Storage::AutoregisterClients',
    AutoOwned(file_method => '_meta_file'),
    'Stream::Moose::Role::Description',
;

__PACKAGE__->meta->make_immutable;
