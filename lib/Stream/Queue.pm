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

use Yandex::Logger;

use base qw(Stream::Storage);

use Params::Validate;
use File::Temp;
use File::Path qw(remove_tree);
use File::Spec;
use Set::Object;
use IO::Handle;
use Storable qw(store_fd fd_retrieve);
use Carp;
use Yandex::Persistent;

use Stream::Queue::In;

=item B<< new({ dir => $dir }) >>

Constructor. C<$dir> must be writable and empty on first invocation.

=cut
sub new {
    my $class = shift;
    my $self = validate(@_, {
        dir => 1,
        format => { default => 'storable' },
    });
    unless ($self->{format} eq 'storable') {
        croak "Only 'storable' format is supported";
    }
    # TODO - check that dir is writable
    # TODO - create dir?
    unless (-d $self->{dir}) {
        mkdir $self->{dir} or croak "Can't create dir $self->{dir}: $!";
    }
    unless (-d "$self->{dir}/clients") {
        mkdir "$self->{dir}/clients" or croak "Can't create dir $self->{dir}/clients: $!";
    }
    return bless $self => $class;
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
    unless (exists $self->{tmp}) {
        $self->{tmp} = File::Temp->new(DIR => $self->{dir});
    }
    store_fd(\$item, $self->{tmp}) or croak "Can't write to '$self->{filename}'";
}

=item B<< commit() >>

Commit temporary file with written items into queue.

Each C<commit()> invocation creates new chunk; each chunk can be read in parallel with others, so try to keep chunks not too large and not too small.

=cut
sub commit {
    my $self = shift;
    unless (exists $self->{tmp}) {
        DEBUG "Nothing to commit";
        return;
    }
    $self->{tmp}->flush;

    my $new_chunk_name = do {
        my $status = Yandex::Persistent->new("$self->{dir}/meta");
        $status->{id} ||= 0;
        $status->{id}++;
        "$self->{dir}/$status->{id}.chunk";
    };
    INFO "Commiting $new_chunk_name";
    rename($self->{tmp}->filename, $new_chunk_name) or die "Failed to commit chunk $self->{tmp}: $!";
    $self->{tmp}->unlink_on_destroy(0);
    delete $self->{tmp};
}

=item B<< register_client($client_name) >>

Register new client with this queue.

Once registered, client must read queue regularly; otherwise queue will become overfill after some time.

=cut
sub register_client {
    my $self = shift;
    my ($client) = validate_pos(@_, { regex => qr/^[\w-]+$/ });
    if ($self->has_client($client)) {
        return; # already registered
    }
    INFO "Registering $client at $self->{dir}";
    mkdir "$self->{dir}/clients/$client" or croak "Can't create dir $self->{dir}/clients/$client: $!";
}

=item B<< unregister_client($client_name) >>

Unregister client named C<$client_name>.

=cut
sub unregister_client {
    my $self = shift;
    my ($client) = validate_pos(@_, { regex => qr/^[\w-]+$/ });
    unless ($self->has_client($client)) {
        WARN "No such client '$client', can't unregister";
        return;
    }
    remove_tree("$self->{dir}/clients/$client");
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

sub _clients {
    my $self = shift;
    my @client_names = map { File::Spec->abs2rel( $_, "$self->{dir}/clients" ) } grep { -d $_ } glob "$self->{dir}/clients/*";
    return map { Stream::Queue::In->new({ storage => $self, client => $_ }) } @client_names;
}

=item B<< clean() >>

Remove all unused chunks and clean clients' statuses.

This method is called authomatically on each clients commits, so usually you shouldn't call it manually.

=cut
sub clean {
    my $self = shift;
    my @clients = $self->_clients;
    my $done_ids;
    for (@clients) {
        my $client_done_ids = Set::Object->new($_->get_done_ids);
        if ($done_ids) {
            $done_ids = $client_done_ids * $done_ids; # intersection
        }
        else {
            $done_ids = $client_done_ids;
        }
    }
    unless ($done_ids->size) {
        DEBUG "Nothing to clean";
        return;
    }
    INFO "Done ids: ".join(', ', $done_ids->elements);
    for ($done_ids->elements) {
        my $chunk = "$self->{dir}/$_.chunk";
        unless (-e $chunk) {
            WARN "Chunk '$chunk' not found";
            next;
        }
        unlink $chunk or croak "Can't unlink '$chunk': $!";
    }
    for (@clients) {
        $_->clean_ids($done_ids->elements);
    }
}

sub stream {
    my $self = shift;
    my ($client) = validate_pos(@_, { regex => qr/^[\w-]+$/ });

    unless ($self->has_client($client)) {
        $self->register_client($client); # TODO - optionally forbid automatic registration?
    }
    return Stream::Queue::In->new({
        storage => $self,
        client => $client,
    });
}

=back

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

