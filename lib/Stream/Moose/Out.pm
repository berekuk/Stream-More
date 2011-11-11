package Stream::Moose::Out;

# ABSTRACT: role for output stream classes

use Moose::Role;
with 'Stream::Moose::FakeIsa' => { extra => ['Stream::Out'] };

requires 'write', 'write_chunk', 'commit';

1;
