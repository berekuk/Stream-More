package Stream::Moose::In::ReadOnly;

# ABSTRACT: specialization of ::Role::ReadOnly for input streams

use Moo::Role;
with 'Stream::Moose::Role::ReadOnly';

use namespace::clean;

before [qw( commit )] => sub {
    shift->check_read_only;
};

1;
