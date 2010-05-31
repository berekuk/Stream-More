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

use Yandex::Version '{{DEBIAN_VERSION}}';

use Yandex::Logger;

use base qw(Stream::Storage);

use Params::Validate;
use File::Path qw(rmtree);
use File::Spec;
use Set::Object;
use IO::Handle;
use Carp;
use Yandex::Persistent 1.3.0;
use Yandex::X 1.1.0;

use Stream::Queue::In;
use Stream::Queue::Appender;

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
    bless $self => $class;
    $self->try_gc;
    return $self;
}

=item B<< meta() >>

Get metadata object.

It's implemented as simple persistent, so it also works as global queue lock.

=cut
sub meta {
    my $self = shift;
    return Yandex::Persistent->new("$self->{dir}/meta", { auto_commit => 0, format => 'json' });
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

=item B<< write($item) >>

Write new item into queue.

Item can be any string or serializable structure, but it can't be C<undef>.

It will stay in temporary file until C<commit>.

=cut
sub write($$) {
    my ($self, $item) = @_;
    unless (exists $self->{appender}) {
        $self->{appender} = Stream::Queue::Appender->new($self);
    }
    $self->{appender}->write($item);
}

=item B<< commit() >>

Commit temporary file with written items into queue.

Each C<commit()> invocation creates new chunk; each chunk can be read in parallel with others, so try to keep chunks not too large and not too small.

=cut
sub commit {
    my $self = shift;
    unless ($self->{appender}) {
        DEBUG "Nothing to commit";
        return;
    }
    $self->{appender}->commit;
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

=item B<< clients() >>

Get all storage clients as plain list.

=cut
sub clients {
    my $self = shift;
    my @client_names = map { File::Spec->abs2rel( $_, "$self->{dir}/clients" ) } grep { -d $_ } glob "$self->{dir}/clients/*";
    return map { Stream::Queue::In->new({ storage => $self, client => $_ }) } @client_names;
}

=item B<< chunk_status_file($id) >>

Get chunk status filename by id. It doesn't check if file actually exists.

=cut
sub chunk_status_file {
    my $self = shift;
    my ($id) = validate_pos(@_, 1);
    return "$self->{dir}/$id.status";
}

=item B<< chunk_file($id) >>

Get chunk filename by id. It doesn't check if file actually exists.

=cut
sub chunk_file {
    my $self = shift;
    my ($id) = validate_pos(@_, 1);
    return $self->dir."/$id.chunk";
}

=item B<< chunk_status($id) >>

Get status (actually, persistent object) of chunk by id.

=cut
sub chunk_status {
    my $self = shift;
    my ($id, $options) = validate_pos(@_, 1, { default => {} });
    my $file = $self->chunk_status_file($id);
    return Yandex::Persistent->new($file, { format => 'json', auto_commit => 0, %$options });
}

=item B<< clean_ids(@ids) >>

Try to clean chunks with given ids (they'll be cleaned only if all clients completed them).

Returns list of ids for actually removed chunks.

=cut
sub clean_ids {
    my $self = shift;
    my @ids = @_;
    my @clients = $self->clients;
    my @removed_ids;

    ID:
    for my $id (@ids) {
        my $file = $self->chunk_file($id);
        my $chunk_status = $self->chunk_status($id);

        for my $client (@clients) {
            my $client_status = $client->chunk_status($id) or next ID;
            if (not $client_status->{status}{pos} or not $chunk_status->{size} or $client_status->{status}{pos} < $chunk_status->{size}) {
                next ID; # client $client not finished chunk yet
            }
        }
        xunlink($file);
        $chunk_status->delete();

        push @removed_ids, $id;
        undef $chunk_status;
    }
    return @removed_ids;
}

=item B<< try_gc() >>

Do garbage collecting if it's necessary.

=cut
sub try_gc {
    my $self = shift;
    my $meta = $self->meta;
    unless (defined $meta->{gc_timestamp}) {
        $meta->{gc_timestamp} = time;
        $meta->commit;
        return;
    }
    if (time - $meta->{gc_timestamp} > $self->{gc_period}) {
        $self->gc();
    }
}

=item B<< gc() >>

Remove all unused chunks and cleanup clients' statuses.

This method is called authomatically on each clients commits, so usually you shouldn't call it manually.

=cut
sub gc {
    my $self = shift;
    my $meta = $self->meta; # queue is locked when gc is active
    my @clients = $self->clients;
    my $done_ids;

    unless (@clients) {
        for my $chunk (glob "$self->{dir}/*.chunk") {
            xunlink($chunk);
        }
        INFO "$self->{dir}: no clients registered, all chunks removed";
        return;
    }

    my @removed_ids;

    opendir my $dh, $self->{dir} or die "Can't open '$self->{dir}': $!";
    while (my $file = readdir $dh) {
        next if $file eq '.';
        next if $file eq '..';
        next if $file eq 'meta';
        next if $file eq 'meta.lock';
        next if $file eq 'clients';

        # readdir can cache some data, file can be already removed (FIXME - there is a race when this check doesn't help)
        next unless -f "$self->{dir}/$file";

        my $id;
        if (($id) = $file =~ /^(\d+)\.(?:chunk)$/) {
            push @removed_ids, $self->clean_ids($id);
            next;
        }
        elsif (($id) = $file =~ /^(\d+)\.(?:status|status\.lock)$/) {
            unless (-e $self->chunk_file($id)) {
                my $fullname = "$self->{dir}/$file";
                xunlink($fullname);
                DEBUG "Lost file $file for $id chunk removed";
            }
            next;
        }
        $file = "$self->{dir}/$file";
        WARN "Unknown file $file";
        xunlink($file);
    }
    closedir $dh or die "Can't close '$self->{dir}': $!";

    for (@clients) {
        $_->clean_ids(@removed_ids);
    }
    for (@clients) {
        $_->gc();
    }
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

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

