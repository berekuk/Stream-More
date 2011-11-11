package Stream::Moose::In::ReadOnly;

# ABSTRACT: specialization of ::Role::ReadOnly for input streams

use Moose::Role;
with 'Stream::Moose::Role::ReadOnly';

before [qw( commit )] => sub {
    shift->check_read_only;
};

1;
