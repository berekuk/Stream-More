package Stream::Moose::In;

use Moose::Role;
with 'Stream::Moose::FakeIsa' => { extra => ['Stream::In'] };

requires 'read', 'read_chunk', 'commit';

1;
