package Stream::DB::Inserter;

use strict;
use warnings;

# ABSTRACT: simple stream wrapper around PPB::DB::Inserter

=head1 SYNOPSIS

    use Stream::DB::Inserter;

    $inserter = new Stream::DB::Inserter({
        DB => connectdb('foaf'),
        Insert => 'INSERT agents(link, foaf_link)',
        Values => '?,?',
        Tail => 'ON DUPLICATE KEY UPDATE link = VALUES(link)', # optional
        PortionSize => 1000,    # optional
    });
    $inserter->write(['http://a', 'http://a/foaf']);
    ...
    $inserter->commit();

=cut

use Yandex::Version '{{DEBIAN_VERSION}}';

use parent qw(Stream::Out);
use PPB::DB::Inserter;

sub new {
    my $class = shift;
    my $inserter = PPB::DB::Inserter->new(@_);
    return bless { inserter => $inserter } => $class;
}

sub write {
    my ($self, $row) = @_;
    $self->{inserter}->insert(@$row);
}

sub commit {
    my $self = shift;
    $self->{inserter}->finish;
}

1;
