package Stream::Moose::In::Lag;

use Moose::Role;
with
    'Stream::Moose::In',
    'Stream::Moose::FakeIsa' => { extra => ['Stream::In::Role::Lag'] },
;

requires 'lag';

1;
