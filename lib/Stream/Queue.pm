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

use parent qw(Stream::Storage);

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

use Stream::Queue::In;
use Stream::Queue::BigChunkDir;

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
    $self->try_convert;
    $self->try_gc;
    return $self;
}

=item B<< write($item) >>

Write new item into queue.

Item can be any string or serializable structure, but it can't be C<undef>.

=cut
sub write($$) {
    my ($self, $item) = @_;
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
    my $status = $self->meta; # global lock
    if ($self->has_client($client)) {
        return; # already registered
    }
    INFO "Registering $client at $self->{dir}";
    xmkdir("$self->{dir}/clients/$client");
}

=item B<< unregister_client($client_name) >>

Unregister client named C<$client_name>.

=cut
sub unregister_client {
    my $self = shift;
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
    return Stream::Queue::In->new({
        storage => $self,
        client => $client,
    });
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

=item B<< clients() >>

Get all storage clients as plain list.

=cut
sub clients {
    my $self = shift;
    my @client_names = map { File::Spec->abs2rel( $_, "$self->{dir}/clients" ) } grep { -d $_ } glob "$self->{dir}/clients/*";
    return map { Stream::Queue::In->new({ storage => $self, client => $_ }) } @client_names;
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
            undef $cd_meta;
            $self->convert($meta);
        }
    }
    else {
        # new queue
        $meta->{version} = 1;
        $meta->commit;
    }
    if ($meta->{version} != 1) {
        croak "Unknown queue version '$meta->{version}'";
    }
}

=item B<< convert() >>

Convert queue from old format. All client positions will be lost, sorry.

=cut
sub convert {
    my ($self, $meta) = @_;
    INFO "Converting from old format";

    if (-d "$self->{dir}/convert") {
        WARN "convert dir already exists, previous convert probably failed";
    }
    else {
        xmkdir("$self->{dir}/convert");
        xsystem("mv $self->{dir}/*.chunk $self->{dir}/convert/");
    }

    my @chunks = glob "$self->{dir}/convert/*.chunk";
    my $converted = Stream::Formatter::LinedStorable->wrap(Stream::File->new("$self->{dir}/convert/converted.chunk"));
    my $filter = Stream::Formatter::LinedStorable->new->read_filter();
    for my $file (@chunks) {
        next unless $file =~ /\d+.chunk$/;
        INFO "Converting $file";
        my $fh = xopen('<', $file);
        my $mixed_flag;
        while (1) {
            my $item;
            if ($mixed_flag) {
                $item = $filter->write($fh->getline);
            }
            else {
                use Storable qw(fd_retrieve);
                my $pos = tell $fh;
                $item = eval { fd_retrieve($fh) };
                if ($@) {
                    WARN "error: $@, probably new-format lines follow";
                    seek $fh, $pos, 0;
                    $mixed_flag = 1;
                }
                $item = $$item;
            }
            $converted->write($item);
            last if $fh->eof;
        }
    }
    $converted->commit;
    if (-e "$self->{dir}/convert/converted.chunk") {
        xrename("$self->{dir}/convert/converted.chunk" => "$self->{dir}/1.chunk");
    }
    xsystem("rm -rf $self->{dir}/convert");
    xsystem("rm -f $self->{dir}/clients/*/status");
    xsystem("rm -f $self->{dir}/clients/*/status.lock");

    $meta->{version} = 1;
    $meta->commit;

    my $cd_meta = $self->{chunk_dir}->meta;
    $cd_meta->{id} = 1;
    $cd_meta->commit;
    INFO "Converted successfully";
}

=item B<< gc() >>

Remove all unused chunks and cleanup clients' statuses.

This method is called authomatically on each clients commits, so usually you shouldn't call it manually.

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
            my $pos = $client->pos($info->{id});
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
    }
}

=back

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

