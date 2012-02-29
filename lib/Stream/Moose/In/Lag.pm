package Stream::Moose::In::Lag;

# ABSTRACT: lag role for input streams

use Moose::Role;
with 'Stream::Moose::In';

use Class::DOES::Moose;
extra_does 'Stream::In::Role::Lag';

requires 'lag';

1;
