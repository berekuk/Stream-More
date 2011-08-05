package Stream::Moose::Role::AutoOwned;

use MooseX::Role::Parameterized;

parameter 'file_method' => (
    is => 'ro',
    required => 1,
);

role {
    my $p = shift;
    my $file_method = $p->file_method;
    requires $file_method;

    method '_build_owner_uid' => sub {
        my $self = shift;
        my $file = $self->$file_method;

        unless (-e $file) {
            return $>;
        }
        my $uid = (stat($file))[4];
        return $uid;
    };

    method '_build_owner' => sub {
        my $self = shift;
        return scalar getpwuid $self->owner_uid;
    };

    with 'Stream::Moose::Role::Owned';
};

1;
