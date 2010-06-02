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
use Stream::Log;
use Stream::Formatter::LinedStorable;

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
        $self->{appender} = $self->log;
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

=item B<< log() >>

Get underlying log storage object.

This method is for internal usage only.

=cut
sub log {
    my $self = shift;
    return Stream::Formatter::LinedStorable->wrap(
        Stream::Log->new($self->dir."/log")
    );
}

=back

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

