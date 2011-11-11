package Stream::Moose::Out::ReadOnly;

# ABSTRACT: specialization of ::Role::ReadOnly for output streams

use Moose::Role;
with 'Stream::Moose::Role::ReadOnly';

before [qw( write write_chunk commit )] => sub {
    shift->check_read_only;
};

1;
