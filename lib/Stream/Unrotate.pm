package Stream::Unrotate;

use strict;
use warnings;

use Yandex::Version '{{DEBIAN_VERSION}}';

=head1 NAME

Stream::Unrotate - simple wrapper for Yandex::Unrotate

=cut

use Yandex::Unrotate;
use base qw(Yandex::Unrotate);
use base qw(Stream::Stream);

sub read {
    return shift->readline;
}

=head1 SEE ALSO

If should consider Stream::Log->new($logfile)->stream($posfile) instead, if you want to get all Catalog benefits.

=cut

1;

