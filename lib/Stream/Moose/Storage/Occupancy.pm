package Stream::Moose::Storage::Occupancy;

# ABSTRACT: role for storages that are able to measure their occupancy

use Moose::Role;
with 'Stream::Moose::Storage';

use Class::DOES::Moose;
extra_does 'Stream::Storage::Role::Occupancy';

requires 'occupancy';

1;
