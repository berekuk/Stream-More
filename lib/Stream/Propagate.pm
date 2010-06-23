package Stream::Propagate;

use strict;
use warnings;

=head1 NAME

Stream::Propagate - output stram for propagating data into stream-accept http service

=head1 SYNOPSIS

    $out = Stream::Propagate->new({
        name => 'blah',
        host => 'example.com',
    });
    $out->write('abc');
    $out->write({ x => 5, y => 6 });
    $out->commit;

=head1 DESCRIPTION

This is a client class for new HTTP stream acceptor service called C<stream-accept>.

=head1 METHODS

=over

=cut

use parent qw(Stream::Out);
use Params::Validate qw(:all);
use LWP::UserAgent;
use HTTP::Request::Common;
use URI;

use Stream::Formatter::JSON;
use Stream::Formatter::LinedStorable;
use Stream::Filter qw(filter);

use Carp;

my %format2filter = (
    json => Stream::Formatter::JSON->new->write_filter,
    storable => Stream::Formatter::LinedStorable->new->write_filter,
    plain => filter(sub { return shift }),
);

=item B<< new($parameters) >>

Constructor. Parameters:

=over

=item I<name>

Name of storage in catalog on remote host.

=item I<endpoint>

HTTP URI string, for example, C<http://example.com:1248>.

=item I<host>

Shortcut for I<endpoint> option. Hostname without C<http://> prefix and port (default port in 1248).

=item I<format>

Serialization format.

Possible values are C<json> (default), C<storable> and C<plain>. Only C<storable> format can serialize perl objects, but it's unsafe to do so since private fields can be different on different hosts, so it's not a default (and please think twice before turning it on).

=back

=cut
sub new {
    my $class = shift;
    my $self = validate(@_, {
        name => { type => SCALAR },
        format => { default => 'json', regex => qr/^storable|json|plain$/ },
        endpoint => { type => SCALAR, optional => 1 },
        host => { type => SCALAR, optional => 1 },
    });

    unless (defined $self->{endpoint} or $self->{host}) {
        croak "One of 'endpoint' or 'host' options should be specified";
    }
    if (defined $self->{endpoint} and $self->{host}) {
        croak "Only one of 'endpoint' and 'host' options should be specified";
    }
    if (defined $self->{host}) {
        $self->{endpoint} = "http://$self->{host}:1248";
    }

    $self->{ua} = LWP::UserAgent->new;
    $self->{filter} = $format2filter{$self->{format}} or die "invalid format '$self->{format}'";
    return bless $self => $class;
}

=item B<< write() >>

Write new item in buffer.

It will not be propagated until next C<commit()> call.

=cut
sub write {
    my ($self, $item) = @_;
    if ($self->{format} eq 'plain') {
        croak "Only strings can be written in plain format" if ref $item;
        unless ($item =~ /\n$/) {
            croak "line '$item' don't end with \\n";
        }
    }
    push @{ $self->{buffer} }, $self->{filter}->write($item);
}

=item B<< commit() >>

POST all items from buffer to remote host.

=cut
sub commit {
    my $self = shift;
    push @{ $self->{buffer} }, $self->{filter}->commit;
    return unless $self->{buffer} and @{ $self->{buffer} };

    my $uri = URI->new($self->{endpoint});
    $uri->path('write');
    $uri->query_form(
        name => $self->{name},
        format => $self->{format},
    );
    my $response = $self->{ua}->request(POST $uri->as_string, Content => join '', @{ $self->{buffer} });
    unless ($response->is_success) {
        croak "Propagating into $self->{name} at $self->{endpoint} failed: ".$response->status_line;
    }
    delete $self->{buffer};
}

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

