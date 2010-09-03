package Stream::In::DiskBuffer;

use strict;
use warnings;

use parent qw(Stream::In);

use namespace::autoclean;

use Params::Validate qw(:all);

use Carp;
use Yandex::Logger;
use Yandex::Lockf;
use Yandex::Persistent;
use Yandex::X;
use Stream::In::DiskBuffer::Chunk;

=head1 NAME

Stream::In::DiskBuffer - parallelize any input stream using on-disk buffer

=head1 SYNOPSIS

    $buffered_in = Stream::In::DiskBuffer->new($in, $dir);

=head1 DESCRIPTION

Some storages don't support parallel reading using one client name.

This class solves this problem by caching small backlog in multiple on-disk files, allowing to read them in parallel fashion.

=head1 METHODS

=over

=item B<< new($in, $dir) >>
=item B<< new($in, $dir, $options) >>

Constructor.

C<$in> can be any L<Stream::In> object.

C<$dir> is a path to local dir. It will be created automatically if at least its parent dir exists (you should have appropriate rights to do this, of course).

C<$options> is an optional hash with options:

=over

=item I<format>

Buffer chunks format. Default is C<storable>.

=back

=cut
sub new {
    my $class = shift;
    my ($in, $dir, @options) = validate_pos(@_, { isa => 'Stream::In' }, 1, { type => HASHREF, optional => 1 });
    my $self = validate(@options, { format => { default => 'storable' } });
    unless ($self->{format} eq 'storable') {
        croak "Only 'storable' format is supported";
    }
    $self->{in} = $in;
    $self->{dir} = $dir;

    $self->{prev_chunks} = {};
    $self->{uncommited} = 0;

    return bless $self => $class;
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

    my $chunk_size = $self->{uncommited} + 1;
    my $data = $self->{in}->read_chunk($chunk_size);
    return unless $data;
    return unless @$data;
    my $new_id = $self->_next_id;
    Stream::In::DiskBuffer::Chunk->new($self->{dir}, $new_id, $data);
    $self->{in}->commit;
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
        my $chunk = Stream::In::DiskBuffer::Chunk->load($self->{dir}, $id) or return;

        DEBUG "Reading $chunk_name";
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
        $self->{uncommited}++;
        return $item;
    }
}


=item C<< commit() >>

Save positions from all read chunks; cleanup queue.

=cut
sub commit {
    my $self = shift;

    DEBUG "Commiting $self->{dir}";

    if ($self->{chunk}) {
        $self->{chunk}->commit;
        delete $self->{chunk};
    }

    $_->remove for values %{ $self->{prev_chunks} };
    $self->{prev_chunks} = {};
    $self->{uncommited} = 0;

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
                DEBUG "Lost lock file $file removed";
            }
        }
        elsif (($id) = $file =~ /^(\d+)\.status$/) {
            unless (-e "$self->{dir}/$id.chunk") {
                $unlink->();
                DEBUG "Lost status file $file for $id client chunk removed";
            }
        }
        elsif (($id) = $file =~ /^(\d+)\.status.lock$/) {
            unless (-e "$self->{dir}/$id.chunk") {
                $unlink->();
                DEBUG "Lost status lock file $file for $id client chunk removed";
            }
        }
        elsif ($file =~ /^(\d+)\.chunk.new$/) {
            my $age = time - (stat($file))[10];
            unless ($age < 600) {
                $unlink->();
                DEBUG "Temp file $file removed";
            }
        }
        else {
            WARN "Unknown file $file";
        }
    }
    closedir $dh or die "Can't close '$self->{dir}': $!";
}

sub in {
    my $self = shift;
    return $self->{in};
}

=back

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;
