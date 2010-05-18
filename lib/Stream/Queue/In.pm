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
use Yandex::Persistent;
use Yandex::Logger;
use Yandex::X;
use Yandex::Lockf;

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
    $self->{prev_locks} = [];
    $self->{prev_ids} = {};
    return bless $self => $class;
}

sub _status {
    my $self = shift;
    return Yandex::Persistent->new("$self->{dir}/status");
}

sub _id2lock {
    my $self = shift;
    my ($id) = validate_pos(@_, 1);
    return "$self->{dir}/$id.chunk_lock";
}

sub _lock2id {
    my $self = shift;
    my ($file) = validate_pos(@_, 1);
    my ($id) = $file =~ /(\d+)\.chunk_lock$/ or die "Wrong lock file $file";
    return $id;
}

sub _id2chunk {
    my $self = shift;
    my ($id) = validate_pos(@_, 1);
    return $self->{storage}->dir."/$id.chunk";
}

sub _chunk2id {
    my $self = shift;
    my ($file) = validate_pos(@_, 1);
    $file = File::Spec->abs2rel($file, $self->{storage}->dir);
    my ($id) = $file =~ /^(\d+)\.chunk$/ or die "Wrong file $file";
    return $id;
}

=item C<< read() >>

Read new item from queue.

This method chooses next unlocked chunk and reads it from position saved from last invocation in persistent file C<$queue_dir/clients/$client_name/status>.

=cut
sub read {
    my $self = shift;

    if ($self->{fh} and $self->{fh}->eof) {
        # chunk is over, keep its lock (we don't want to commit yet) and switch to next chunk if possible
        DEBUG "Chunk is over";
        $self->{prev_ids}{ $self->_lock2id($self->{lock}->name) } = 1;
        push @{$self->{prev_locks}}, delete $self->{lock};
        delete $self->{fh};
    }

    unless ($self->{fh}) {
        my $status = $self->_status;

        # TODO - keep buffer of next chunks, just in case there are a LOT of chunks and glob() call is too expensive
        my @chunk_files = glob $self->{storage}->dir."/*.chunk";

        @chunk_files =
            map { $_->[1] }
            sort { $a->[0] <=> $b->[0] }
            map { [ $self->_chunk2id($_), $_ ] }
            @chunk_files; # resort by ids

        my @filtered_chunk_files;
        for (@chunk_files) {
            my $id = $self->_chunk2id($_);
            if (defined $status->{pos}{$id} and $status->{pos}{$id} eq 'done') {
                next; # chunk already processed in some other process
            }
            if ($self->{prev_ids}{$id}) {
                next; # chunk already processing in this process
            }
            push @filtered_chunk_files, $_;
        }
        @chunk_files = @filtered_chunk_files;

        my $lock = lockf_any([
            map { $self->_id2lock($self->_chunk2id($_)) } @chunk_files
        ], 1);
        unless ($lock) {
            # no chunks left
            return;
        }
        my $id = $self->_lock2id($lock->name);
        $self->{lock} = $lock;
        my $chunk = $self->_id2chunk($id);
        DEBUG "Reading $chunk";
        $self->{fh} = xopen('<', $chunk);
        if (my $pos = $status->{pos}{$id}) {
            seek $self->{fh}, $pos, 0 or die "Can't seek to $pos in $chunk";
        }
    }
    my $item = fd_retrieve($self->{fh});
    unless ($item) {
        return;
    }
    return $$item;
}


=item C<< commit() >>

Save positions from all read chunks in status file; cleanup queue.

=cut
sub commit {
    my $self = shift;

    INFO "Commiting $self->{dir}";

    my $status = $self->_status;
    for my $lock (@{ $self->{prev_locks} }) {
        my $id = $self->_lock2id($lock->name);
        $status->{pos}{$id} = 'done';
    }
    if ($self->{fh}) {
        my $id = $self->_lock2id($self->{lock}->name);
        if ($self->{fh}->eof) {
            $status->{pos}{$id} = 'done';
        }
        else {
            $status->{pos}{$id} = tell $self->{fh};
        }
    }
    $status->commit;
    $self->{prev_locks} = [];
    $self->{prev_ids} = {};
    delete $self->{fh};
    delete $self->{lock};
    undef $status;
    $self->{storage}->clean;
}

=item C<< get_done_ids() >>

Get ids of all completed chunks.

You shouldn't call this method from anywhere except C<Stream::Queue>, which uses it for cleanup. API is still very unstable.

=cut
sub get_done_ids {
    my $self = shift;
    my $status = $self->_status;
    return grep { $status->{pos}{$_} eq 'done' } keys %{ $status->{pos} };
}

=item C<< clean_ids(@ids) >>

Remove all references to specified ids from status file.

You shouldn't call this method from anywhere except C<Stream::Queue>, which uses it for cleanup. API is still very unstable.

=cut
sub clean_ids {
    my $self = shift;
    my @ids = @_;
    my $status = $self->_status;
    for (@ids) {
        delete $status->{pos}{$_};
        my $lockfile = $self->_id2lock($_);
        if (-e $lockfile) {
            unlink $lockfile or warn "Can't remove old lock file: $!";
        }
    }
}

=back

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

