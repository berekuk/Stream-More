package Stream::Queue::Appender;

use strict;
use warnings;

=head1 NAME

Stream::Queue::Appender - queue storage's writer

=cut

use Storable qw(store_fd);
use File::Temp;
use Carp;
use Yandex::X;
use Yandex::Logger;
use IO::Handle;

sub new {
    my ($class, $storage) = @_;
    return bless { storage => $storage } => $class;
}

sub write {
    my ($self, $item) = @_;
    unless (exists $self->{tmp}) {
        $self->{tmp} = File::Temp->new(DIR => $self->{storage}->dir);
    }
    store_fd(\$item, $self->{tmp}) or croak "Can't write to '$self->{filename}'";
}

sub _next_id {
    my $self = shift;
    my $status = $self->{storage}->meta;
    $status->{id} ||= 0;
    $status->{id}++;
    $status->commit;
    return $status->{id};
}

sub commit {
    my ($self) = @_;

    return unless exists $self->{tmp};

    my $storage = $self->{storage};
    my $new_id = $self->_next_id;

    my $chunk_status = $storage->chunk_status($new_id);
    $chunk_status->{size} = $self->{tmp}->tell;

    my $new_chunk_name = $storage->chunk_file($new_id);
    INFO "Commiting $new_chunk_name";
    xrename($self->{tmp}->filename => $new_chunk_name);
    $self->{tmp}->unlink_on_destroy(0);
    delete $self->{tmp};

    $chunk_status->commit;
}

=head1 AUTHOR

Vyacheslav Matjukhin <mmcleric@yandex-team.ru>

=cut

1;

