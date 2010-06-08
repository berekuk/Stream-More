package Stream::Queue::BigChunkDir;

use strict;
use warnings;

use Stream::Formatter::LinedStorable;
use Stream::File;
use Yandex::Persistent;
use Yandex::Logger;
use Params::Validate qw(:all);

=head1 NAME

Stream::Queue::BigChunkDir - dir for keeping queue chunks

=head1 DESCRIPTION

This class is for keeping main queue chunks.

Client-specific chunks are not maintained by this class (yet).

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

sub out {
    my $self = shift;
    {
        package Stream::Queue::BigChunkFile;
        use parent qw(Stream::File);
        use Yandex::Lockf 3.0;
        sub new {
            my ($class, $dir, $id) = @_;
            my $append_lock = lockf("$dir/$id.append_lock", { shared => 1, blocking => 0 });
            die "Can't take append lock" if not $append_lock; # this exception should never happen - we already have meta lock at this moment
            my $self = $class->SUPER::new("$dir/$id.chunk");
            $self->{chunk_append_lock} = $append_lock;
            return $self;
        }
    }
    my $meta = $self->meta;
    my $id = $meta->{id};
    unless (defined $id) {
        $id = 1;
        $meta->{id} = 1;
        $meta->commit;
    }
    my $file = "$self->{dir}/$id.chunk";
    if (-e $file) {
        my $size = -s $file; # file still exists, because we locked dir with meta
        if ($size > $self->{max_chunk_size}) {
            my @chunk_files = glob "$self->{dir}/*.chunk";
            if (@chunk_files >= $self->{max_chunk_count}) {
                die "Chunk count exceeded";
            }
            $id = ++$meta->{id};
            $meta->commit;
            INFO "New chunk $id";
            $file = "$self->{dir}/$id.chunk";
        }
    }
    my $out = Stream::Queue::BigChunkFile->new($self->{dir}, $id);
    return Stream::Formatter::LinedStorable->wrap($out);
}

sub chunks_info {
    my $self = shift;
    my @files =  glob "$self->{dir}/*.chunk";
    my @result;
    for my $file (@files) {
        $file =~ /(\d+)\.chunk$/ or die "Invalid filename '$file'";
        my $id = $1;
        push @result, { id => $id, file => $file, lock_file => "$self->{dir}/$id.append_lock" };
    }
    return @result;
}

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

