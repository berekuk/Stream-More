package Stream::Buffer::Persistent;

use namespace::autoclean;
use Moose;
with 'Stream::Buffer::Role';

use Params::Validate qw(:all);

use Yandex::Logger;
use Yandex::Persistent;

=head1 METHODS

=over

=item B<< new($params) >>
=item B<< new(%$params) >>

Constructor.

=over

=item I<dir>

A local directory to store uncommited data. The buffer is implemented as a set of L<Yandex::Persistent> files. This parameter is mandatory.

=item I<max_chunk_size>

Maximum number of items to be stored in a single SQLite database. 1000 by default. Set to 0 to disable the chunk size control.

=item I<max_chunk_count>

Maximum number of SQLite databases to create. 100 by default. Multiple databases are required to provide a concurrent access to the buffer.

=back

=cut

my $counter = 0;

sub _persistent {
    my ($file) = @_;
    return Yandex::Persistent->new($file, { auto_commit => 0 }, { blocking => 0, remove => 1 });
}

has 'dir' => (
    is => 'ro',
    required => 1,
);

has 'max_chunk_size' => (
    is => 'ro',
    isa => 'Int',
    default => 1000,
);

has 'max_chunk_count' => (
    is => 'ro',
    isa => 'Int',
    default => 100,
);

sub BUILD {
    my $self = shift;
    $self->_create_buffer();
}

sub DEMOLISH {
    local $@;
    my $self = shift;
    $self->{_dbh}->delete if $self->{_dbh} and defined $self->{_db_size} and $self->{_db_size} == 0;
}

sub _find_buffer {
    my $self = shift;

    my @files = sort glob("$self->{dir}/*.state");
    $self->{_chunk_count} = scalar(@files);

    for my $file (@files) {
        my $db = _persistent($file);
        next unless $db;
        DEBUG "$file: found and locked";
        return ($db, $file);
    }

    return;
}

sub _create_buffer {
    my $self = shift;

    my ($dbh, $file) = $self->_find_buffer();

    unless ($file) {
        die "Chunk limit exceeded: $self->{dir}" if $self->{max_chunk_count} and $self->{_chunk_count} >= $self->{max_chunk_count};
        while () {
            $file = "$self->{dir}/" . time . ".$$." . $counter++ . ".state";
            $dbh = _persistent($file);
            unless ($dbh) { # newly created file was locked by someone else
                DEBUG "$file: created but stolen";
                next;
            }
            DEBUG "$file: created and locked";
            last;
        }
    }

    $self->{_dbh} = $dbh;
    $self->{_file} = $file;

    $self->{_dbh}{data} ||= {};

    {
        my $data = $self->{_dbh}{data};
        $self->{_buffer} = [
            map { [ $_ => $data->{$_} ] } sort { $a <=> $b } keys %$data
        ];
    }

    $self->{_id} = $self->{_buffer}->[-1]->[0] + 1 if @{$self->{_buffer}};
    $self->{_id} ||= 0;

    $self->{_db_size} = @{$self->{_buffer}};

    #use Data::Dumper;
    #warn Dumper($self);
}

sub _id {
    my $self = shift;
    return $self->{_id}++;
}

sub save {
    my $self = shift;
    my ($chunk) = @_;
    my $chunk_size = @$chunk;

    die "Chunk size exceeded: $self->{dir}: $self->{_file}" if $self->{max_chunk_size} and $self->{_db_size} + $chunk_size > $self->{max_chunk_size};

    for my $data (@$chunk) {
        my $id = $self->_id;
        push @{ $self->{_buffer} }, [$id => $data];

        #warn "save($id, $data)";
        $self->{_dbh}{data}{$id} = $data;
    }

    $self->{_dbh}->commit();

    $self->{_db_size} += @$chunk;
    return;
}

sub load {
    my $self = shift;
    my ($limit) = @_;
    $limit ||= 1;

    my $result = [];

    while () {

        unless (@{$self->{_buffer}}) {

            my ($db, $file) = $self->_find_buffer();
            last unless $db;

            my $buffer = [ values %{ $db->{data} } ];

            $self->save($buffer);
            #warn "removing $file";
            $db->delete;
            next;
        } else {

            push @$result, splice @{$self->{_buffer}}, 0, $limit - @$result;
        }

        last if $limit <= @$result;

    }

    return $result;
}

sub delete {
    my $self = shift;
    my ($ids) = @_; #TODO: no ids => delete everything already loaded

    for my $id (@$ids) {
        #warn "delete($id)";

        my $deleted = delete $self->{_dbh}{data}{$id};

        unless (defined $deleted) {
            die "commit: unknown id: $id";
        }
    }
    $self->{_dbh}->commit();
    $self->{_db_size} -= @$ids;
}


sub lag {
    my $self = shift;
    my $lag = 0;
    $lag += length $_->[1] for @{$self->{_buffer}}; #FIXME: sum from all chunks
    return $lag;
}

=back

=cut

__PACKAGE__->meta->make_immutable;
