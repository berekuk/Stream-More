package Stream::Queue::In;

use strict;
use warnings;

=head1 NAME

Stream::Queue::In - input stream for Stream::Queue

=head1 SYNOPSIS

    $in = $queue->stream('client_name');
    $in->read;
    $in->read;
    $in->commit;

=head1 METHODS

=over

=cut

use base qw(Stream::In);
use Params::Validate;

use Carp;
use IO::Handle;
use Yandex::Persistent 1.3.0;
use Yandex::Logger;
use Yandex::Lockf 3.0.0;
use Yandex::X;
use Stream::Queue::Chunk;

=item C<< new({ storage => $queue, client => $client }) >>

Constructor.

Usually you should construct object of this class using C<stream()> method from C<Stream::Queue>.

=cut
sub new {
    my $class = shift;
    my $self = validate(@_, {
        storage => { isa => 'Stream::Queue' },
        client => { regex => qr/^[\w-]+$/ },
    });
    unless ($self->{storage}->has_client($self->{client})) {
        croak "$self->{client} is not registered at ".$self->{storage}->dir;
    }
    $self->{dir} = $self->{storage}->dir."/clients/$self->{client}";
    $self->{prev_chunks} = {};
    $self->{lock} = lockf("$self->{dir}/lock", { shared => 1 });
    bless $self => $class;
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

sub _next_id {
    my $self = shift;
    my $status = $self->meta;
    $status->{id} ||= 0;
    $status->{id}++;
    $status->commit;
    return $status->{id};
}

sub _chunk2id {
    my $self = shift;
    my ($file) = validate_pos(@_, 1);
    my ($id) = $file =~ /(\d+)\.chunk$/ or die "Wrong file $file";
    return $id;
}

sub _new_chunk {
    my $self = shift;
    my $in = $self->{storage}->log->stream(Stream::Log::Cursor->new({
        PosFile => "$self->{dir}/pos",
    }));
    my $new_id = $self->_next_id;
    my $chunk_name = "$self->{dir}/$new_id.chunk";
    my $chunk_data = $in->read_chunk(100) or return;
    Stream::Queue::Chunk->new($self->{dir}, $new_id, $chunk_data);
    $in->commit;
    return $new_id;
}

sub _next_chunk {
    my $self = shift;

    my $try_chunk = sub {
        my $chunk_name = shift;
        my $id = $self->_chunk2id($chunk_name);
        if ($self->{prev_chunks}{$id}) {
            return; # chunk already processed in this process
        }

        my $chunk = Stream::Queue::Chunk->load($self->{dir}, $id) or return;

        DEBUG "[$self->{client}] Reading $chunk_name";
        $self->{chunk} = $chunk;
        return 1;
    };

    my @chunk_files = glob $self->{dir}."/*.chunk";

    @chunk_files =
        map { $_->[1] }
        sort { $a->[0] <=> $b->[0] }
        map { [ $self->_chunk2id($_), $_ ] }
        @chunk_files; # resort by ids

    for my $chunk (@chunk_files) {
        $try_chunk->($chunk) and return 1;
    }

    # no chunks left
    my $new_chunk_id = $self->_new_chunk or return;
    $try_chunk->("$self->{dir}/$new_chunk_id.chunk") and return 1;

    return;
}

=item C<< read() >>

Read new item from queue.

This method chooses next unlocked chunk and reads it from position saved from last invocation in persistent file C<$queue_dir/clients/$client_name/$chunk_id.status>.

=cut
sub read {
    my $self = shift;

    while () {
        unless ($self->{chunk}) {
            $self->_next_chunk or return undef;
        }
        my $item = $self->{chunk}->read;
        unless (defined $item) {
            # chunk is over
            $self->{prev_chunks}{ $self->{chunk}->id } = $self->{chunk};
            delete $self->{chunk};
            next;
        }
        return $item;
    }
}


=item C<< commit() >>

Save positions from all read chunks; cleanup queue.

=cut
sub commit {
    my $self = shift;

    INFO "Commiting $self->{dir}";

    if ($self->{chunk}) {
        $self->{chunk}->commit;
        delete $self->{chunk};
    }

    $_->remove for values %{ $self->{prev_chunks} };
    $self->{prev_chunks} = {};

    return;
}

=item C<< gc() >>

Cleanup lost files.

=cut
sub gc {
    my $self = shift;
    my $lock = lockf($self->{lock}->file);
    opendir my $dh, $self->{dir} or die "Can't open '$self->{dir}': $!";
    while (my $file = readdir $dh) {
        next if $file eq '.';
        next if $file eq '..';
        next if $file =~ /(^meta|^pos$|^lock$)/;
        my $unlink = sub {
            xunlink("$self->{dir}/$file");
        };
        my $id;
        if (($id) = $file =~ /^(\d+)\.lock$/) {
            unless (-e "$self->{dir}/$id.status") {
                $unlink->();
                DEBUG "[$self->{client}] Lost lock file $file removed";
            }
            next;
        }
        elsif (($id) = $file =~ /^(\d+)\.status$/) {
            unless (-e $self->{storage}->dir."/$id.chunk") {
                $unlink->();
                DEBUG "[$self->{client}] Lost file $file for $id chunk removed";
            }
            next; # TODO - remove files with old ids
        }
        elsif ($file =~ /^(\d+)\.chunk.new$/) {
            my $age = time - (stat($file))[10];
            unless ($age < 600) {
                $unlink->();
                DEBUG "[$self->{client}] Temp file $file removed";
            }
        }
        $unlink->();
        WARN "[$self->{client}] Unknown file $file removed";
    }
    closedir $dh or die "Can't close '$self->{dir}': $!";
}

=back

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

