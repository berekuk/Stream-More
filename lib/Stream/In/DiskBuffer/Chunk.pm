package Stream::In::DiskBuffer::Chunk;

# ABSTRACT: represents one disk buffer chunk

use strict;
use warnings;

use namespace::autoclean;
use parent qw(
    Stream::In
    Stream::In::Role::Lag
);

=head1 METHODS

=over

=cut

use Yandex::X;
use Yandex::Logger;
use Yandex::Lockf 3.0.0;
use Stream::Formatter::LinedStorable;
use Stream::Formatter::JSON;
use Stream::File::Cursor;
use Stream::File 0.9.4; # from this version, Stream::File::In implements 'lag'
use Params::Validate qw(:all);
use Carp;

#FIXME: move formatters to catalog!
my %format2wrapper = (
    json => Stream::Formatter::JSON->new,
    storable => Stream::Formatter::LinedStorable->new,
    plain => undef,
);

sub new {
    my ($class, $dir, $id, $data, @opts) = validate_pos(@_, 1, { type => SCALAR }, { type => SCALAR, regex => qr/^\d+$/ }, { type => ARRAYREF }, { type => HASHREF, optional => 1 });
    my $opts = validate(@opts, {
        format => { default => 'storable' },
    });

    my $file = "$dir/$id.chunk";
    my $wrapper = $format2wrapper{$opts->{format}};

    my $new_file = "$file.new";
    if (-e $new_file) {
        WARN "removing unexpected temporary file $new_file";
        xunlink($new_file);
    }
    my $storage = Stream::File->new($new_file);
    $storage = $wrapper->wrap($storage) if $wrapper;
    $storage->write_chunk($data);
    $storage->commit;
    xrename($new_file => $file);

    return $class->load($dir, $id, $opts);
}

=item B<< load($dir, $id) >>

Construct chunk object corresponding to existing chunk.

=cut
sub load {
    my ($class, $dir, $id, @opts) = validate_pos(@_, 1, { type => SCALAR }, { type => SCALAR, regex => qr/^\d+$/ }, { optional => 1, type => HASHREF });
    my $opts = validate(@opts, {
        read_only => { default => 0 },
        format => { default => 'storable' },
    });

    return unless -e "$dir/$id.chunk"; # this check is unnecessary, but it reduces number of fanthom lock files
    my $lock;
    unless ($opts->{read_only}) { 
        $lock = lockf("$dir/$id.lock", { blocking => 0 }) or return;
    };
    return unless -e "$dir/$id.chunk";

    my $wrapper = $format2wrapper{$opts->{format}};
    my $storage = Stream::File->new("$dir/$id.chunk");
    $storage = $wrapper->wrap($storage) if $wrapper;

    my $new = $opts->{read_only} ? "new_ro" : "new";
    my $in = $storage->stream(Stream::File::Cursor->$new("$dir/$id.status"));
    
    return bless {
        in => $in,
        read_only => $opts->{read_only},
        lock => $lock,
        id => $id,
        dir => $dir,
    } => $class;
}

sub _check_ro {
    my $self = shift;
    $Carp::CarpLevel = 1;
    croak "Stream is read only" if $self->{read_only};
}

sub read {
    my $self = shift;
    return $self->{in}->read;
}

sub read_chunk {
    my $self = shift;
    return $self->{in}->read_chunk(@_);
}

sub commit {
    my $self = shift;
    $self->_check_ro();
    return $self->{in}->commit;
}

=item B<< id() >>

Get chunk id.

=cut
sub id {
    my $self = shift;
    return $self->{id};
}

sub lag {
    my $self = shift;
    return $self->{in}->lag;
}

=item B<< remove() >>

Remove chunk and all related files.

=cut
sub remove {
    my $self = shift;
    $self->_check_ro();
    my $prefix = "$self->{dir}/$self->{id}";
    xunlink("$prefix.chunk");
    xunlink("$prefix.status");
    xunlink("$prefix.status.lock") if -e "$prefix.status.lock";
    xunlink("$prefix.lock");
}

=back

=cut

1;
