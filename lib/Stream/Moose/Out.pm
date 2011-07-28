package Stream::Moose::Out;

use Moose::Role;
with 'Stream::Moose::FakeIsa' => { extra => ['Stream::Out'] };

requires 'write', 'write_chunk', 'commit';

1;
