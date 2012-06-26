package Stream::In::DiskBuffer;

# ABSTRACT: parallelize any input stream using on-disk buffer

use strict;
use warnings;

use parent qw(
    Stream::In
);

use namespace::autoclean;

use Params::Validate qw(:all);

use Carp;
use Yandex::Logger;
use Yandex::Lockf 3.0;
use Yandex::Persistent;
use Yandex::X;
use Stream::In::DiskBuffer::Chunk;

use Scalar::Util qw(blessed);

=head1 SYNOPSIS

    $buffered_in = Stream::In::DiskBuffer->new($in, $dir);

=head1 DESCRIPTION

Some storages don't support parallel reading using one client name.

This class solves this problem by caching small backlog in multiple on-disk files, allowing them to be processed in parallel.

=head1 METHODS

=over

=item B<< new($in, $dir) >>

=item B<< new($in, $dir, $options) >>

Constructor.

C<$in> can be L<Stream::In> object or coderef which returns L<Stream::In> object.

Second variant is useful if underlying input stream caches stream position; in this case, it's necessary to recreate input stream every time when diskbuffer caches new portion of data, so that every item is read from underlying stream only once. For example, you definitely should write C<< Stream::In::DiskBuffer->new(sub { Stream::Log::In->new(...) }, $dir) >> and not C<< Stream::In::DiskBuffer->new(Stream::Log::In->new(...), $dir) >>.

C<$dir> is a path to local dir. It will be created automatically if at least its parent dir exists (you should have appropriate rights to do this, of course).

C<$options> is an optional hash with options:

=over

=item I<format>

Buffer chunks format. Default is C<storable>.

=item I<gc_period>

Period in seconds to run garbage collecting.

=item I<read_lock>

Take a short read lock while reading data from the underlying input stream.

This option is true by default. You can turn it off if you're sure that your input stream is locking itself. (It usually should lock on read and then unlock on commit to cooperate correctly with diskbuffer.)

=back

=cut
sub new {
    my $class = shift;
    my ($in, $dir, @options) = validate_pos(@_, { type => CODEREF | OBJECT }, { type => SCALAR }, { type => HASHREF, optional => 1 });

    my $self = validate(@options, {
        format => { default => 'storable', type => SCALAR },
        gc_period => { default => 300, type => SCALAR, regex => qr/^\d+$/ },
        read_only => { default => 0, type => BOOLEAN },
        read_lock => { default => 1, type => BOOLEAN },
    });
    $self->{dir} = $dir;

    if (blessed($in)) {
        $self->{in} = sub { $in };
    }
    else {
        $self->{in} = $in;
    }

    $self->{prev_chunks} = {};
    $self->{uncommited} = 0;
    bless $self => $class;

    unless (-d $dir) {
        $self->_check_ro();
        mkdir($dir);
        die "mkdir failed: $!" unless -d $dir;
    }

    unless ($self->{read_only}) {
        $self->try_gc;
    }

    return $self;
}

sub _check_ro {
    my $self = shift;
    local $Carp::CarpLevel = 1;
    croak "Stream is read only" if $self->{read_only};
}

=item B<< meta() >>

Get metadata object.

It's implemented as simple persistent, so it also works as global exclusive queue lock.

=cut
sub meta {
    my $self = shift;
    $self->_check_ro(); # read_only meta is not currently of any use
    return Yandex::Persistent->new("$self->{dir}/meta", { auto_commit => 0, format => 'json' });
}

sub _next_id {
    my $self = shift;
    $self->_check_ro();
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
    $self->_check_ro();
    my $chunk_size = $self->{uncommited} + 1;

    my $read_lock;
    $read_lock = lockf("$self->{dir}/read_lock") if $self->{read_lock};

    my $in = $self->in;

    my $data = $in->read_chunk($chunk_size);
    return unless $data;
    return unless @$data;

    my $new_id = $self->_next_id;
    Stream::In::DiskBuffer::Chunk->new($self->{dir}, $new_id, $data, { format => $self->{format} });
    $in->commit;

    return $new_id;
}

sub _load_chunk {
    my ($self, $id, $overrides) = @_;
    $overrides ||= {};

    # this line can create lock file for already removed chunk, which will be removed only at next gc()
    # TODO - think how we can fix it
    return Stream::In::DiskBuffer::Chunk->load($self->{dir}, $id, { read_only => $self->{read_only}, format => $self->{format}, %$overrides });
}

sub _next_chunk {
    my $self = shift;

    my $try_chunk = sub {
        my $chunk_name = shift;
        my $id = $self->_chunk2id($chunk_name);
        if ($self->{prev_chunks}{$id}) {
            return; # chunk already processed in this process
        }

        my $chunk = $self->_load_chunk($id) or return;

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
    unless ($self->{read_only}) {
        my $new_chunk_id = $self->_new_chunk or return;
        $try_chunk->("$self->{dir}/$new_chunk_id.chunk") and return 1;
    } 
    return; 
}

=item B<< read() >>

Read new item from queue.

This method chooses next unlocked chunk and reads it from position saved from last invocation in persistent file C<$queue_dir/clients/$client_name/$chunk_id.status>.

=cut
sub read_chunk {
    my ($self, $data_size) = @_;

    my $remaining = $data_size;
    my $result;
    while () {
        unless ($self->{chunk}) {
            unless ($self->_next_chunk()) {
                if ($self->{read_only} and not $self->{chunk_in}) {
                    $self->{chunk} = $self->in; # ah yeah! streams... yummy!
                    $self->{chunk_in} = 1;
                } else {
                    last;
                }
            }
        }
        my $data = $self->{chunk}->read_chunk($remaining);
        unless (defined $data) {
            # chunk is over
            unless ($self->{chunk_in}) {
                $self->{prev_chunks}{ $self->{chunk}->id } = $self->{chunk};
                delete $self->{chunk};
                next;
            } else {
                last;
            }
        }
        $self->{uncommited} += @$data;
        $result ||= [];
        push @$result, @$data;
        $remaining -= @$data;
        last if $remaining <= 0;
    }
    return $result;
}

sub read {
    my $self = shift;
    my $chunk = $self->read_chunk(1);
    return unless $chunk;
    return $chunk->[0];
}


=item B<< commit() >>

Save positions from all read chunks; cleanup queue.

=cut
sub commit {
    my $self = shift;
    $self->_check_ro();
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

=item B<< try_gc() >>

Do garbage collecting if it's necessary.

=cut
sub try_gc {
    my $self = shift;
    if ($self->{read_only} or $self->{gc_timestamp_cached} and time < $self->{gc_timestamp_cached} + $self->{gc_period}) {
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

=item B<< gc() >>

Cleanup lost files.

=cut
sub gc {
    my ($self, $meta) = @_;
    $self->_check_ro();
    $meta ||= $self->meta;

    opendir my $dh, $self->{dir} or die "Can't open '$self->{dir}': $!";
    while (my $file = readdir $dh) {
        next if $file eq '.';
        next if $file eq '..';
        next if $file =~ /^meta/;
        next if $file =~ /^read_lock$/;
        next if $file =~ /^\d+\.chunk$/;
        my $unlink = sub {
            xunlink("$self->{dir}/$file");
        };

        if (my ($id) = $file =~ /^(\d+)\.(?: lock | status | status\.lock )$/x) {
            unless (-e "$self->{dir}/$id.chunk") {
                $unlink->();
                DEBUG "Lost file $file removed";
            }
            next;
        }

        if ($file =~ /^(\d+)\.chunk.new$/ or $file =~ /^yandex\.tmp\./) {
            my $age = time - (stat($file))[10];
            unless ($age < 600) {
                $unlink->();
                DEBUG "Temp file $file removed";
                next;
            }
        }
        WARN "Removing unknown file $file";
        $unlink->();
    }
    closedir $dh or die "Can't close '$self->{dir}': $!";
}

=item B<< in() >>

Get underlying input stream.

=cut
sub in {
    my $self = shift;
    return $self->{in}->();
}

=item B<< buffer_lag() >>

Get lag of this buffer (this is slightly less than total buffer size, so this method is called B<buffer_lag()> instead of B<buffer_size()> for a reason).

=cut
sub buffer_lag {

    my $self = shift;
    my $lag = 0;

    my @chunk_files = glob $self->{dir}."/*.chunk";
    for (@chunk_files) {
        my $id = $self->_chunk2id($_);
        next if $self->{prev_chunks}{$id};
        if ($self->{chunk} and not $self->{chunk_in} and $self->{chunk}->id eq $id) {
            $lag += $self->{chunk}->lag();
        } else {
            my $chunk = $self->_load_chunk($id, { read_only => 1 }) or next; # always read_only
            $lag += $chunk->lag();
        }
    }
    return $lag;
}

=item B<< lag() >>

Get total lag of this buffer and underlying stream.

=cut
sub lag {
    my $self = shift;
    my $in = $self->in;
    die "underlying input stream doesn't implement Lag role" unless $in->DOES('Stream::In::Role::Lag');
    return $in->lag + $self->buffer_lag;
}

sub DOES {
    my ($self, $role) = @_;
    if ($role eq 'Stream::In::Role::Lag') {
        return $self->in->DOES($role);
    }
    return $self->SUPER::DOES($role);
}

=back

=cut

1;
