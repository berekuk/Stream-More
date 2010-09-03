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

use parent qw(Stream::In);
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
    $self->{dir} = $self->{storage}->dir."/clients/$self->{client}/pos";
    $self->{prev_chunks} = {};
    $self->{uncommited} = 0;
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

sub read_chunk {
    my $self = shift;
    my $chunk_size = shift;

    my $lock = $self->lock;
    my @chunks_info = $self->{storage}->chunks_info;

    my $data;
    my @ins;
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
    if ($data and @$data) {
        # returning non-empty chunk, keeping this input stream locked until "commit" will be called
        $self->{uncommited} = {
            lock => $lock,
            in => [@ins],
        };
    }
    return $data;
}

=item C<< read() >>

Read new item from queue.

=cut
sub read {
    my $self = shift;
    my $chunk = $self->read_chunk(1);
    return unless $chunk;
    return unless @$chunk > 0;
    croak "invalid chunk" unless @$chunk == 1;
    return @$chunk;
}

sub commit {
    my $self = shift;
    return unless $self->{uncommited};
    for (@{ $self->{uncommited}{in} }) {
        $_->commit;
    }
    delete $self->{uncommited};
    return ();
}

=item C<< pos($id) >>

Get position in bytes for given chunk id.

=cut
sub pos {
    my ($self, $id) = @_;
    my $cursor_file = "$self->{dir}/$id.pos";
    unless (-e $cursor_file) {
        return 0;
    }
    my $cursor = Stream::File::Cursor->new($cursor_file);
    return $cursor->position;
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
        else {
            WARN "[$self->{client}] Unknown file $file";
        }
    }
    closedir $dh or die "Can't close '$self->{dir}': $!";
}

=back

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

