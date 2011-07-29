package Stream::Moose::In::ReadOnly;

use Moose::Role;
with 'Stream::Moose::Role::ReadOnly';

before [qw( commit )] => sub {
    shift->check_read_only;
};

1;
