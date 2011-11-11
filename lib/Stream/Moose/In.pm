package Stream::Moose::In;

# ABSTRACT: role for stream storage classes

use Moose::Role;
with 'Stream::Moose::FakeIsa' => { extra => ['Stream::In'] };

requires 'read', 'read_chunk', 'commit';

1;
