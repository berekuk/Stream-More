package Stream::Moose::Role::AutoOwned;

# ABSTRACT: specialization of ::Role::Owned which detects owner by *nix file owner

use strict;
use warnings;

use Package::Variant
    importing => ['Moo::Role'],
    subs => [qw( requires with )];

sub make_variant {
    my ($class, $target_package, %params) = @_;

    $params{file_method} or die "'file_method' parameter expected";
    my $file_method = $params{file_method};

    requires $file_method;

    install '_build_owner_uid' => sub {
        my $self = shift;
        my $file = $self->$file_method;

        unless (-e $file) {
            return $>;
        }
        my $uid = (stat($file))[4];
        return $uid;
    };

    install '_build_owner' => sub {
        my $self = shift;
        return scalar getpwuid $self->owner_uid;
    };

    with 'Stream::Moose::Role::Owned';
};

1;
