package Stream::Queue::BigChunkDir;

use strict;
use warnings;

use Yandex::Persistent;
use Yandex::Logger;
use Yandex::Lockf;
use Params::Validate qw(:all);

use Stream::Formatter::LinedStorable;
use Stream::File;

use Stream::Queue::BigChunkFile;

=head1 NAME

Stream::Queue::BigChunkDir - dir for keeping queue chunks

=head1 DESCRIPTION

This class is for keeping main queue chunks.

Client-specific chunks are not maintained by this class.

=over

=item B<< new($params) >>

Constructor.

=cut
sub new {
    my $class = shift;
    my $params = validate(@_, {
        dir => { type => SCALAR },
        max_chunk_size => { type => SCALAR, regex => qr/^\d+$/ },
        max_chunk_count => { type => SCALAR, regex => qr/^\d+$/ },
    });
    return bless $params => $class;
}

=item B<< meta() >>

Get metadata object.

It's implemented as simple persistent, so it also works as global dir lock.

=cut
sub meta {
    my $self = shift;
    return Yandex::Persistent->new("$self->{dir}/meta", { auto_commit => 0, format => 'json' });
}

=item B<< meta_ro() >>

Get read-only version of metadata object.

=cut
sub meta_ro {
    my $self = shift;
    my $file = "$self->{dir}/meta";
    unless (-e $file) {
        my $meta = $self->meta;
        $meta->{id} = 1;
        $meta->commit;
    }
    return Yandex::Persistent->new($file, { read_only => 1, format => 'json' });
}

=item B<< lock() >>

Global lock.

Returns C<Yandex::Lockf> instance.

=cut
sub lock {
    my $self = shift;
    return lockf("$self->{dir}/out.lock");
}

=item B<< out() >>

Returns output stream directed into some new or existing chunk, depending on whether last chunk in queue is already full.

=cut
sub out {
    my $self = shift;
    my $lock = lockf("$self->{dir}/out.lock", { shared => 1 }); # this lock guarantees that chunk will not be removed

    my $meta = $self->meta_ro;

    my ($id, $file);
    my $trial = 0;
    while (1) {
        # we'll try twice:
        # first time without global lock
        # second time with globally locked meta object, in case first chunk is too big and somebody already created second chunk
        $trial++;
        $id = $meta->{id};
        $file = "$self->{dir}/$id.chunk";
        unless (-e $file and (-s $file) and (-s $file) > $self->{max_chunk_size}) {
            last; # chunk looks ok
        }

        # chunk is too big, time for new chunk
        $meta = $self->meta if $trial == 1; # upgrading to locked meta after first trial
        if ($id == $meta->{id}) {
            # we are the first one noticed that chunk is full
            $id = ++$meta->{id};
            $meta->commit;
            INFO "New chunk $id";
            $file = "$self->{dir}/$id.chunk";
        }
        else {
            # somebody already created new chunk
            if ($trial > 1) {
                die "Internal error, meta updated while locked";
            }
            next;
        }
    }

    my @chunk_files = glob "$self->{dir}/*.chunk";
    if (@chunk_files >= $self->{max_chunk_count}) {
        die "Chunk count exceeded";
    }

    my $out = Stream::Queue::BigChunkFile->new($self->{dir}, $id);
    return Stream::Formatter::LinedStorable->wrap($out);
}

=item B<< next_out() >>

Move chunk counter to the next chunk.

=cut
sub next_out {
    my $self = shift;
    my $meta = $self->meta();
    $meta->{id}++;
    $meta->commit();
}

=item B<< chunks_info() >>

Get info about all chunks in dir.

=cut
sub chunks_info {
    my $self = shift;
    my @files =  glob "$self->{dir}/*.chunk";
    my @result;
    for my $file (@files) {
        $file =~ /(\d+)\.chunk$/ or die "Invalid filename '$file'";
        my $id = $1;
        push @result, { id => $id, file => $file, lock_file => "$self->{dir}/$id.append_lock" };
    }
    @result = sort { $a->{id} <=> $b->{id} } @result;
    return @result;
}

=back

=cut

1;
