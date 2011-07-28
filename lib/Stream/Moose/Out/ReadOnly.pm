package Stream::Moose::Out::ReadOnly;

use Moose::Role;
with 'Stream::Moose::Role::Owned';

has 'read_only' => (
    is => 'ro',
    isa => 'Bool',
    lazy_build => 1,
);

sub _build_read_only {
    my $self = shift;
    if ($self->owner_uid ne $>) {
        return 1;
    }
    return;
}

sub check_read_only {
    my $self = shift;
    confess "Output stream $self is read only" if $self->read_only;
}

before [qw( write write_chunk commit )] => sub {
    shift->check_read_only;
};

1;
