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
use Stream::Formatter::LinedStorable;
use Stream::File::Cursor;

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
    return $self;

}

=item B<< meta() >>

Get metadata object.

It's implemented as simple persistent, so it also works as global exclusive queue lock.

=cut
sub meta {
    my $self = shift;
    return Yandex::Persistent->new("$self->{dir}/meta", { auto_commit => 0, format => 'json' });
}

=item B<< lock() >>

Get shared lock. All rw operations should take it, so gc would not interfere with them.

=cut
sub lock {
    my $self = shift;
    return lockf("$self->{dir}/lock", { shared => 1 });
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

    my $lock = $self->lock;

    my @chunks_info = $self->{storage}->chunks_info;

    my $data;
    my @ins;
    my $chunk_size = 100;
    for my $info (@chunks_info) {
        my $in = Stream::Formatter::LinedStorable->wrap(Stream::File->new($info->{file}))->stream(
            Stream::File::Cursor->new("$self->{dir}/$info->{id}.pos")
        );
        my $portion_size = $chunk_size;
        $portion_size -= @$data if $data;
        my $portion_data = $in->read_chunk($portion_size) or next;
        push @ins, $in;
        push @$data, @$portion_data;
        last if @$data >= $chunk_size;
    }
    return unless $data;
    my $new_id = $self->_next_id;
    Stream::Queue::Chunk->new($self->{dir}, $new_id, $data);
    $_->commit for @ins;
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

        # this line can create lock file for already removed chunk, which will be removed only at next gc()
        # TODO - think how we can fix it
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
    my $lock = $self->lock;
    $lock->unshare;
    opendir my $dh, $self->{dir} or die "Can't open '$self->{dir}': $!";
    while (my $file = readdir $dh) {
        next if $file eq '.';
        next if $file eq '..';
        next if $file =~ /(^meta|^lock$)/;
        next if $file =~ /^\d+\.chunk$/;
        my $unlink = sub {
            xunlink("$self->{dir}/$file");
        };
        my $id;
        if (($id) = $file =~ /^(\d+)\.lock$/) {
            unless (-e "$self->{dir}/$id.status") {
                $unlink->();
                DEBUG "[$self->{client}] Lost lock file $file removed";
            }
        }
        elsif (($id) = $file =~ /^(\d+)\.status$/) {
            unless (-e "$self->{dir}/$id.chunk") {
                $unlink->();
                DEBUG "[$self->{client}] Lost status file $file for $id client chunk removed";
            }
        }
        elsif (($id) = $file =~ /^(\d+)\.status.lock$/) {
            unless (-e "$self->{dir}/$id.chunk") {
                $unlink->();
                DEBUG "[$self->{client}] Lost status lock file $file for $id client chunk removed";
            }
        }
        elsif (($id) = $file =~ /^(\d+)\.pos$/) {
            unless (-e $self->{storage}->dir."/$id.chunk") {
                $unlink->();
                DEBUG "[$self->{client}] Lost pos file $file for $id chunk removed";
            }
        }
        elsif (($id) = $file =~ /^(\d+)\.pos.lock$/) {
            unless (-e $self->{storage}->dir."/$id.chunk") {
                $unlink->();
                DEBUG "[$self->{client}] Lost pos lock file $file for $id chunk removed";
            }
        }
        elsif ($file =~ /^(\d+)\.chunk.new$/) {
            my $age = time - (stat($file))[10];
            unless ($age < 600) {
                $unlink->();
                DEBUG "[$self->{client}] Temp file $file removed";
            }
        }
        else {
            WARN "[$self->{client}] Unknown file $file";
        }
    }
    closedir $dh or die "Can't close '$self->{dir}': $!";
}

=item C<< pos($id) >>

Get position in bytes for given chunk id.

=cut
sub pos {
    my ($self, $id) = @_;
    my $cursor_file = "$self->{dir}/$id.pos";
    unless (-e $cursor_file) {
        return;
    }
    my $cursor = Stream::File::Cursor->new($cursor_file);
    return $cursor->position;
}

=back

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

