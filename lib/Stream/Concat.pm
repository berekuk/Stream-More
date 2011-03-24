package Stream::Concat;

use strict;
use warnings;

=head1 NAME

Stream::Concat - composite storage helping to change underlying storage implementation

=head1 SYNOPSIS

    use Stream::Concat;

    my $storage = Stream::Concat->new($old_storage => $new_storage);
    $storage->write(...); # writes into new storage
    my $in = $storage->stream('abc'); # get concat input stream
    $in->read; # read from old storage until it's empty, then reads from new storage

=cut

use namespace::autoclean;

use parent qw(Stream::Storage);

use Params::Validate;
use Stream::Concat::In;

sub new {
    my $class = shift;
    my ($old_storage, $new_storage) = validate_pos(@_, { isa => 'Stream::Storage' }, { isa => 'Stream::Storage' });
    return bless {
        old => $old_storage,
        new => $new_storage,
    } => $class;
}

sub write {
    my $self = shift;
    $self->{new}->write(@_);
}

sub write_chunk {
    my $self = shift;
    $self->{new}->write_chunk(@_);
}

sub commit {
    my $self = shift;
    $self->{new}->commit;
}

sub stream {
    my $self = shift;
    my $old_in = $self->{old}->stream(@_);
    my $new_in = $self->{new}->stream(@_);
    return Stream::Concat::In->new($old_in, $new_in);
}

=head1 SEE ALSO

C<Stream::Concat::In> - concatenate any number of input streams

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

