package Stream::Queue::BigChunkFile;

# ABSTRACT: helper class for Stream::Queue

use strict;
use warnings;

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

1;
