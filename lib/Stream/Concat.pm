package Stream::Concat;

# ABSTRACT: composite storage helping to change underlying storage implementation

=head1 SYNOPSIS

    use Stream::Concat;

    my $storage = Stream::Concat->new($old_storage => $new_storage);
    $storage->write(...); # writes into new storage
    my $in = $storage->stream('abc'); # get concat input stream
    $in->read; # read from old storage until it's empty, then reads from new storage

=cut

use namespace::autoclean;

use Moose;

use Params::Validate;
use Stream::Concat::In;

use List::MoreUtils qw(uniq);

sub isa {
    return 1 if $_[1] eq __PACKAGE__;
    $_[0]->next::method if $_[0]->next::can;
} # ugly hack

has 'new_storage' => (
    is => 'ro',
    handles => 'Stream::Moose::Out',
    required => 1,
);

has 'old_storage' => (
    is => 'ro',
    required => 1,
);

=head1 METHODS

=over

=item B< BUILDARGS($old, $new) >

Constructor of this class accepts two arguments: old storage and new storage.

=cut
sub BUILDARGS {
    my $self = shift;
    my ($old_storage, $new_storage) = validate_pos(@_, { isa => 'Stream::Storage' }, { isa => 'Stream::Storage' });

    return {
        old_storage => $old_storage,
        new_storage => $new_storage,
    }
}

sub in {
    my $self = shift;

    my $old_in = $self->old_storage->stream(@_);
    my $new_in = $self->new_storage->stream(@_);
    return Stream::Concat::In->new($old_in, $new_in);
}

=item B<client_names>

=item B<register_client>

=item B<unregister_client>

These methods delagate their calls to I<both> underlying storages. C<client_names()> returns the list of unique names of clients of both storages too.

These methods work only if I<both> underlying storages implement C<Stream::Storage::Role::ClientList> role.

=cut
sub client_names {
    my $self = shift;
    return uniq($self->old_storage->client_names, $self->new_storage->client_names);
}

sub register_client {
    my $self = shift;
    $self->old_storage->register_client(@_);
    $self->new_storage->register_client(@_);
}

sub unregister_client {
    my $self = shift;
    $self->old_storage->unregister_client(@_);
    $self->new_storage->unregister_client(@_);
}

sub DOES {
    my $self = shift;
    my ($role) = @_;
    if ($role eq 'Stream::Storage::Role::ClientList') {
        if (
            $self->old_storage->DOES($role)
            and $self->new_storage->DOES($role)
        ) {
            return 1;
        }
        else {
            return 0;
        }
    }
    return $self->SUPER::DOES($role);
}

=back

=cut

with 'Stream::Moose::Storage';

=head1 SEE ALSO

C<Stream::Concat::In> - concatenate any number of input streams

=cut

__PACKAGE__->meta->make_immutable;
