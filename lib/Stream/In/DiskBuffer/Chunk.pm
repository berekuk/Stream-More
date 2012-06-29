package Stream::In::DiskBuffer::Chunk;

# ABSTRACT: represents one disk buffer chunk

=head1 DESCRIPTION

This module is used internally by L<Stream::In::DiskBuffer>.

It mostly conforms to C<Stream::In> API, except that you have to pre-initialize it manually using C<load> method. It's a C<Stream::File::In> decorator.

All chunks are immutable after they're created.

Creating an instance of this class doesn't create any files. You have to call C<create> first to fill it.

=cut

use namespace::autoclean;
use Moose;
with
    'Stream::Moose::In',
    'Stream::Moose::In::Lag',
;
use autodie qw(rename unlink);

=head1 METHODS

=over

=cut

use Yandex::Logger;
use Yandex::Lockf 3.0.0;
use Stream::Formatter::LinedStorable;
use Stream::Formatter::JSON;
use Stream::File::Cursor;
use Stream::File;
use Params::Validate qw(:all);
use Try::Tiny;
use File::Temp;

has 'dir' => (
    is => 'ro',
    isa => 'Str',
    required => 1,
);

has 'id' => (
    is => 'ro',
    isa => 'Int',
    required => 1,
);

has 'read_only' => (
    is => 'ro',
    isa => 'Bool',
    default => 0,
);

#FIXME: move formatters to catalog!
my %format2wrapper = (
    json => Stream::Formatter::JSON->new,
    storable => Stream::Formatter::LinedStorable->new,
);
has 'format' => (
    is => 'ro',
    isa => 'Str', # FIXME - Stream::Formatter? coerce?
);
has 'format_obj' => (
    is => 'ro',
    lazy => 1,
    default => sub {
        my $self = shift;
        my $format = $self->format;
        return unless $format;
        return if $format eq 'plain';
        my $obj = $format2wrapper{$format};
        confess "Unknown format '$format'" unless $obj;
        return $obj;
    },
);

has '_lock' => (
    is => 'ro',
    lazy => 1,
    default => sub {
        my $self = shift;
        return if $self->read_only;
        return lockf($self->_prefix.".lock", { blocking => 0 });
    },
);
has '_in' => (
    is => 'rw', # init lazily, once, in load()
);

sub _prefix {
    my $self = shift;
    return $self->dir.'/'.$self->id;
}

=item B<create($data)>

Create the new chunk and fill it with given arrayref of data atomically.

Exception will happen if chunk already exists.

=cut
sub create {
    my $self = shift;
    my ($data) = validate_pos(@_, { type => ARRAYREF });

    my $file = $self->_prefix.".chunk";
    my $new_file = "".File::Temp->new(DIR => $self->dir, TEMPLATE => 'tmpchunkXXXXXX', UNLINK => 0);

    if ($self->_in) {
        die "Can't recreate chunk, $self is already initialized";
    }
    if (-e $file) {
        die "Can't recreate chunk, $file already exists";
    }
    my $storage = Stream::File->new($new_file);
    $storage = $self->format_obj->wrap($storage) if $self->format_obj;
    $storage->write_chunk($data);
    $storage->commit;

    rename $new_file => $file; # autodie takes care of errors
}

=item B<< load($dir, $id) >>

Initialize input stream. You have to call manually it before reading.

=cut
sub load {
    my $self = shift;

    return 1 if $self->_in; # already loaded

    my $prefix = $self->_prefix;
    my $file = "$prefix.chunk";
    return unless -e $file; # this check is unnecessary, but it reduces number of fanthom lock files
    my $lock = $self->_lock;
    return unless $lock or $self->read_only;
    return unless -e $file;

    # it's still possible that file will disappear (if we're in r/o mode and didn't acquire the lock)
    my $storage = Stream::File->new($file);
    $storage = $self->format_obj->wrap($storage) if $self->format_obj;

    my $new = $self->read_only ? "new_ro" : "new";

    my $in;
    try {
        $in = $storage->in(Stream::File::Cursor->$new("$prefix.status"));
    }
    catch {
        if (-e $file) {
            die $_;
        }
        elsif ($lock) {
            die "Internal error: failed to create $file but lock is acquired";
        }
    };
    return unless $in; # probably disappeared

    $self->_in($in);
    return 1; # ok, loaded
}

sub _check_ro {
    my $self = shift;
    confess "Stream is read only" if $self->read_only;
}

sub read {
    my $self = shift;
    return $self->_in->read;
}

sub read_chunk {
    my $self = shift;
    return $self->_in->read_chunk(@_);
}

sub commit {
    my $self = shift;
    $self->_check_ro();
    return $self->_in->commit;
}

sub lag {
    my $self = shift;
    unless ($self->load) {
        return 0; # probably locked in some other process, or maybe chunk is already deleted
    }
    return $self->_in->lag;
}

=item B<< cleanup() >>

Remove all chunk-related files if and only if the chunk is fully processed.

=cut
sub cleanup {
    my $self = shift;
    my $prefix = $self->_prefix;
    return if -e "$prefix.chunk";
    $self->_lock or return; # even if chunk doesn't exist, we'll force the lock
    $self->remove;
}

=item B<< remove() >>

Remove chunk and all related files.

=cut
sub remove {
    my $self = shift;
    $self->_check_ro();

    my $prefix = $self->_prefix;
    for (map { "$prefix.$_" } qw/ chunk status status.lock lock /) {
        unlink $_ if -e $_;
    }
}

=back

=cut

__PACKAGE__->meta->make_immutable;
