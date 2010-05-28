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
use Storable qw(fd_retrieve);
use IO::Handle;
use File::Spec;
use Yandex::Persistent 1.3.0;
use Yandex::Logger;
use Yandex::X;
use Yandex::Lockf 3.0.0;

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
    bless $self => $class;

    if (-e "$self->{dir}/status") {
        $self->_convert_from_old_status;
    }
    return $self;

}

sub _convert_from_old_status {
    my $self = shift;
    my $old_status = Yandex::Persistent->new("$self->{dir}/status", { auto_commit => 0 });
    for my $id (keys %{ $old_status->{pos} }) {
        my $pos = $old_status->{pos}{$id};
        my $chunk_status = Yandex::Persistent->new("$self->{dir}/$id.status", { format => 'json', auto_commit => 0 });
        $chunk_status->{pos} = $pos;
        $chunk_status->commit;
    }
    xunlink($old_status);
    undef $old_status; # status file will not be rewritten because auto_commit is disabled
    xunlink("$old_status.lock");
}

=item C<< chunk_status($id) >>

Get chunk info by id.

Returns hashref C<< { id => $id, status => $persistent } >>.

=cut
sub chunk_status {
    my $self = shift;
    my ($id) = validate_pos(@_, 1);
    my $status = Yandex::Persistent->new("$self->{dir}/$id.status", { format => 'json', auto_commit => 0 }, { blocking => 0 }) or return;
    return {
        id => $id,
        status => $status,
    };
}

sub _chunk2id {
    my $self = shift;
    my ($file) = validate_pos(@_, 1);
    $file = File::Spec->abs2rel($file, $self->{storage}->dir);
    my ($id) = $file =~ /^(\d+)\.chunk$/ or die "Wrong file $file";
    return $id;
}

sub _next_chunk {
    my $self = shift;

    # TODO - keep buffer of next chunks, just in case there are a LOT of chunks and glob() call is too expensive
    my @chunk_files = glob $self->{storage}->dir."/*.chunk";

    @chunk_files =
        map { $_->[1] }
        sort { $a->[0] <=> $b->[0] }
        map { [ $self->_chunk2id($_), $_ ] }
        @chunk_files; # resort by ids

    for my $chunk (@chunk_files) {
        my $id = $self->_chunk2id($chunk);
        my $chunk_status = $self->{storage}->chunk_status($id, { read_only => 1 });
        my $client_chunk_status = $self->chunk_status($id) or next;

        unless ($chunk_status->{size}) {
            next; # chunk is empty
        }
        if ($client_chunk_status->{status}{pos} and $client_chunk_status->{status}{pos} >= $chunk_status->{size}) {
            next; # chunk already processed in some other process
        }
        if ($self->{prev_chunks}{$id}) {
            next; # chunk already processing in this process
        }

        DEBUG "[$self->{client}] Reading $chunk";
        $self->{current_chunk} = $client_chunk_status;

        $self->{fh} = xopen('<', $chunk);
        if (my $pos = $client_chunk_status->{status}{pos}) {
            seek $self->{fh}, $pos, 0 or die "Can't seek to $pos in $chunk";
        }
        return 1;
    }
    # no chunks left
    return;
}

=item C<< read() >>

Read new item from queue.

This method chooses next unlocked chunk and reads it from position saved from last invocation in persistent file C<$queue_dir/clients/$client_name/$chunk_id.status>.

=cut
sub read {
    my $self = shift;

    if ($self->{fh} and $self->{fh}->eof) {
        # chunk is over, keep its lock (we don't want to commit yet) and switch to next chunk if possible
        DEBUG "[$self->{client}] Chunk is over";
        my $pos = tell $self->{fh};
        if ($pos < 0) {
            die "tell failed";
        }
        $self->{current_chunk}{status}{pos} = $pos;
        $self->{prev_chunks}{ delete $self->{current_chunk}{id} } = $self->{current_chunk};
        delete $self->{fh};
    }

    unless ($self->{fh}) {
        $self->_next_chunk or return;
    }
    my $item = fd_retrieve($self->{fh});
    unless ($item) {
        return;
    }
    return $$item;
}


=item C<< commit() >>

Save positions from all read chunks; cleanup queue.

=cut
sub commit {
    my $self = shift;

    INFO "Commiting $self->{dir}";
    my @ids = keys %{ $self->{prev_chunks} };

    for my $id (@ids) {
        my $prev_chunk = $self->{prev_chunks}{$id};
        $prev_chunk->{status}->commit;
    }
    if ($self->{fh}) {
        my $id = $self->{current_chunk}{id};
        $self->{current_chunk}{status}{pos} = tell $self->{fh};
        $self->{current_chunk}{status}->commit;
    }

    $self->{prev_chunks} = {};
    $self->{current_chunk} = {};
    delete $self->{fh};

    $self->{storage}->clean_ids(@ids);
    return;
}

=item C<< clean_ids(@ids) >>

Remove all references to specified ids from client dir.

You shouldn't call this method from anywhere except C<Stream::Queue>, which uses it for cleanup. API is still very unstable.

=cut
sub clean_ids {
    my $self = shift;
    my @ids = @_;
    for (@ids) {
        my $status = $self->chunk_status($_) or next;
        DEBUG "[$self->{client}] Cleaning status for chunk $_";
        $status->{status}->delete();
    }
}

=item C<< gc() >>

Cleanup lost files.

=cut
sub gc {
    my $self = shift;
    opendir my $dh, $self->{dir} or die "Can't open '$self->{dir}': $!";
    while (my $file = readdir $dh) {
        next if $file eq '.';
        next if $file eq '..';
        my $unlink = sub {
            xunlink("$self->{dir}/$file");
        };
        my $id;
        if (($id) = $file =~ /^(\d+)\.status\.lock$/) {
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

