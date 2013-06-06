package Stream::Moose::Out::ReadOnly;

# ABSTRACT: specialization of ::Role::ReadOnly for output streams

use Moo::Role;
with 'Stream::Moose::Role::ReadOnly';

for (qw( write write_chunk commit )) {
    before $_ => sub {
        shift->check_read_only;
    };
}

1;
