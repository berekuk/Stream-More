package Stream::Queue;

use strict;
use warnings;

=head1 NAME

Stream::Queue - output stream supporting parallel writing

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

use parent qw(Stream::Storage Stream::Storage::Role::ClientList);

use Yandex::Version '{{DEBIAN_VERSION}}';

use Yandex::Logger;
use Yandex::Lockf;

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

our $CURRENT_VERSION = 2;

=item B<< new({ dir => $dir }) >>

Constructor. C<$dir> must be writable and empty on first invocation.

Options:

=over

=item I<autoregister>

If true, automatically register client at first C<stream()> call.

Default is true.

=back

=cut
sub new {
    my $class = shift;
    my $self = validate(@_, {
        dir => 1,
        format => { default => 'storable' },
        autoregister => { default => 1 },
        gc_period => { default => 300 },
        read_only => { default => 0 }, #TODO: 0, 1, and undef by default = auto-upgrade
        max_chunk_size => { default => 50 * 1024 * 1024, regex => qr/^\d+$/ },
        max_chunk_count => { default => 100, regex => qr/^\d+$/ },
    });
    unless ($self->{format} eq 'storable') {
        croak "Only 'storable' format is supported";
    }
    $self->{dir} = File::Spec->rel2abs($self->{dir});
    # TODO - check that dir is writable
    # TODO - create dir?
    unless (-d $self->{dir}) {
        xmkdir($self->{dir});
    }
    unless (-d "$self->{dir}/clients") {
        xmkdir("$self->{dir}/clients");
    }
    $self->{chunk_dir} = Stream::Queue::BigChunkDir->new({
        dir => $self->{dir},
        max_chunk_size => $self->{max_chunk_size},
        max_chunk_count => $self->{max_chunk_count},
    });
    bless $self => $class;
    unless ($self->{read_only}) {
        $self->try_convert;
        $self->try_gc;
    }
    return $self;
}

sub _check_ro ($) {
    my ($self) = @_;
    if ($self->{read_only}) {
        die "Stream is read only";
    }
}

=item B<< write($item) >>

Write new item into queue.

Item can be any string or serializable structure, but it can't be C<undef>.

=cut
sub write($$) {
    my ($self, $item) = @_;
    $self->_check_ro();
    unless (defined $item) {
        croak "Can't write undef";
    }
    unless (exists $self->{out}) {
        $self->{out} = $self->{chunk_dir}->out;
    }
    $self->{out}->write($item);
}

=item B<< commit() >>

Commit written items.

=cut
sub commit {
    my $self = shift;
    $self->_check_ro();
    unless ($self->{out}) {
        DEBUG "Nothing to commit";
        return;
    }
    $self->{out}->commit;
    delete $self->{out}; # out recreated after every commit, otherwise we would hold one old chunk opened for too long
    $self->try_gc;
}

=item B<< register_client($client_name) >>

Register new client with this queue.

Once registered, client must read queue regularly; otherwise queue will become overfill after some time.

=cut
sub register_client {
    my $self = shift;
    my ($client) = validate_pos(@_, { regex => qr/^[\w-]+$/ });
    $self->_check_ro();

    my $status = $self->meta; # global lock
    if ($self->has_client($client)) {
        return; # already registered
    }
    INFO "Registering $client at $self->{dir}";
    xmkdir("$self->{dir}/clients/$client");
    xmkdir("$self->{dir}/clients/$client/buffer");
    xmkdir("$self->{dir}/clients/$client/pos");
}

=item B<< unregister_client($client_name) >>

Unregister client named C<$client_name>.

=cut
sub unregister_client {
    my $self = shift;
    $self->_check_ro();
    my ($client) = validate_pos(@_, { regex => qr/^[\w-]+$/ });
    my $status = $self->meta; # global lock
    unless ($self->has_client($client)) {
        WARN "No such client '$client', can't unregister";
        return;
    }
    rmtree("$self->{dir}/clients/$client");
}

=item B<< has_client($client_name) >>

Check whether queue has client C<$client_name> registered.

=cut
sub has_client {
    my $self = shift;
    my ($client) = validate_pos(@_, { regex => qr/^[\w-]+$/ });
    unless (-d "$self->{dir}/clients/$client") {
        return;
    }
    return 1;
}

sub stream {
    my $self = shift;
    my ($client) = validate_pos(@_, { regex => qr/^[\w-]+$/ });

    unless ($self->has_client($client)) {
        unless ($self->{autoregister}) {
            croak "Client $client not found and autoregister is disabled";
        }
        $self->register_client($client);
    }
    return Stream::In::DiskBuffer->new(
        Stream::Queue::In->new({
            storage => $self,
            client => $client,
            read_only => $self->{read_only},
        }) => "$self->{dir}/clients/$client/buffer",
    );
}

sub class_caps {
    { persistent => 1 }
}

=back

=head1 INTERNAL METHODS

These methods are not a part of public API and are used only by C<Stream::Queue::*> modules.

=over

=item B<< meta() >>

Get metadata object.

It's implemented as simple persistent, so it also works as global queue lock.

=cut
sub meta {
    my $self = shift;
    return Yandex::Persistent->new("$self->{dir}/queue.meta", { auto_commit => 0, format => 'json' });
}

=item B<< format() >>

Get queue's internal format.

=cut
sub format {
    my $self = shift;
    return $self->{format};
}

=item B<< dir() >>

Get queue's dir.

=cut
sub dir {
    my $self = shift;
    return $self->{dir};
}

=item B<< chunks_info() >>

Proxy method for chunks_info() method from C<Stream::Queue::BigChunkDir>.

=cut
sub chunks_info {
    my $self = shift;
    return $self->{chunk_dir}->chunks_info;
}

sub client_names {
    my $self = shift;
    my @client_names = map { File::Spec->abs2rel( $_, "$self->{dir}/clients" ) } grep { -d $_ } glob "$self->{dir}/clients/*";
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
    if ($self->{gc_timestamp_cached} and time < $self->{gc_timestamp_cached} + $self->{gc_period}) {
        return;
    }

    my $meta = $self->meta;
    unless (defined $meta->{gc_timestamp}) {
        $self->{gc_timestamp_cached} = $meta->{gc_timestamp} = time;
        $meta->commit;
        return;
    }
    $self->{gc_timestamp_cached} = $meta->{gc_timestamp};
    if (time > $meta->{gc_timestamp} + $self->{gc_period}) {
        $self->{gc_timestamp_cached} = $meta->{gc_timestamp} = time;
        $meta->commit;
        $self->gc($meta);
    }
}

=item B<< try_convert() >>

Check if queue is in old format and needs to be converted.

=cut
sub try_convert {
    my $self = shift;

    my $meta = $self->meta;
    my $cd_meta = $self->{chunk_dir}->meta;
    if ($cd_meta->{id}) {
        if (not defined $meta->{version}) {
            croak "version field not found in $self->{chunk_dir} metadata";
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

    my @client_names = $self->client_names;
    for my $client (@client_names) {
        for my $dir ("$self->{dir}/clients/$client/buffer", "$self->{dir}/clients/$client/pos") {
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
        $rename_all->('buffer', glob "$self->{dir}/clients/$client/*.chunk");
        $rename_all->('buffer', glob "$self->{dir}/clients/$client/*.status");
        $rename_all->('buffer', "$self->{dir}/clients/$client/meta") if -e "$self->{dir}/clients/$client/meta";
        $rename_all->('pos', glob "$self->{dir}/clients/$client/*.pos");

        # cleanup
        for my $file (glob "$self->{dir}/clients/$client/*") {
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
    $meta ||= $self->meta; # queue is locked when gc is active
    my $cd_lock = $self->{chunk_dir}->lock; # chunk dir is locked too

    my @chunks_info = $self->{chunk_dir}->chunks_info;
    my @clients = $self->clients;

    CHUNK:
    for my $info (@chunks_info) {
        my $lock = lockf($info->{lock_file}, { blocking => 0 }) or next;
        for my $client (@clients) {
            my $pos = $client->in->pos($info->{id});
            my $size = -s $info->{file};
            my $lag = $size - $pos;
            next CHUNK if not defined $lag;
            next CHUNK if $lag > 0;
        }
        # chunk can be safely removed
        xunlink($info->{lock_file}) if -e $info->{lock_file}; # theoretically, previous gc could fail after this unlink and before unlinking chunk itself
        xunlink($info->{file});
    }

    for (@clients) {
        $_->gc();
        $_->in->gc();
    }
}

=back

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

