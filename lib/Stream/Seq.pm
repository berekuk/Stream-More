package Stream::Seq;

use strict;
use warnings;

# adaptation of PPB::Join modules

use base qw(Stream);

sub new {
    my ($class, $seq) = @_;
    return bless {seq => $seq} => $class;
}

sub read {
    $_[0]->{seq}->shift;
}
1;

