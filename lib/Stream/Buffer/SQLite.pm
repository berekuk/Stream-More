package Stream::Buffer::SQLite;

# ABSTRACT: stream decorator to allow non sequential commits

use strict;
use warnings;

use parent qw(
    Stream::In
);

use namespace::autoclean;

use Params::Validate qw(:all);

use Yandex::Logger;
use DBI;
use DBD::SQLite;

=head1 SYNOPSIS

    $buf = Stream::Buffer::SQLite->new({ dir => $dir, max_chunk_size => ..., max_chunk_count => ... });
    $buf->save([$data1, $data2]);
    [[$id1 => $data1], [$id2 => $data2], [$id3 => $data3]] = $buf->load(3);
    [[$id4 => $data4]] = $buf->load();
    $buf->delete([$id1, $id3]);
    $lag = $buf->lag();

=head1 DESCRIPTION

MQ internals!
No multiprocessing support.
No read/only support.

=head1 METHODS

=over

=item B<< new($options) >>

Constructor.

C<$options> is an optional hash with options:

=over

=item I<dir>

A local directory to store uncommited data. The buffer is implemented as a set of SQLite databases.

=item I<max_chunk_size>

Maximum number of items to be stored in a single SQLite database. 1000 by default. Set to 0 to disable the chunk size control.

=item I<max_chunk_count>

Maximum number of SQLite databases to create. Multiple databases are required to provide a concurrent access to the buffer.

=back

=cut
sub new {
    my $class = shift;
    my $self = validate(@_, { 
        dir => 1,
        max_chunk_size => { default => 1000 },
        max_chunk_count => { default => 100 },
    });

    bless $self => $class;
    $self->{_db_file} = "$self->{dir}/buffer.sqlite"; #TODO: glob("*.sqlite") and pick unlocked

    $self->_init_db();

    return $self;
}

sub _init_db {
    my $self = shift;
    $self->{_dbh} = DBI->connect("dbi:SQLite:dbname=$self->{_db_file}", "", "", {
        AutoCommit => 0,
        RaiseError => 1,
        PrintError => 0,
    });
    $self->_upgrade();

    $self->{_buffer} = $self->{_dbh}->selectall_arrayref(qq{
        select id, data from buffer order by id
    }); #FIXME: load lazy?

    $self->{_id} = $self->{_buffer}->[-1]->[0] + 1 if @{$self->{_buffer}};
    $self->{_id} ||= 0;

    $self->{_db_size} = @{$self->{_buffer}}; 
}

sub _get_version ($) {
    my ($self) = @_;
    my $version = 0;
    eval { ($version) = $self->{_dbh}->selectrow_array("select version from version") };
    die if $@ and $@ !~ /no such table/;
    return $version;
}

sub _upgrade_0_to_1 ($) {
    my ($self) = @_;
    my $dbh = $self->{_dbh};

    $dbh->do("create table version (version int)");
    $dbh->do("insert into version (version) values (1)");

    $dbh->do(qq{
        create table buffer (
            id bigint primary key,
            data blob
        )}); # TODO: locked -> bigint?
    $dbh->commit();
}

sub _upgrade ($) {
    my ($self) = @_;
    my $version =$self->_get_version();
    $self->_upgrade_0_to_1() if $version == 0;
}

sub _id {
    my $self = shift;
    return $self->{_id}++;
}

sub save {
    my $self = shift;
    my ($chunk) = @_;
    my $limit = @$chunk;

    die "buffer exhausted: $self->{_db_size} + $limit > $self->{max_chunk_size}" if $self->{max_chunk_size} and $self->{_db_size} + $limit > $self->{max_chunk_size};

    for my $data (@$chunk) {

        my $id = $self->_id;
        push @{$self->{_buffer}}, [$id => $data];
        $self->{_dbh}->do(qq{
            insert into buffer (id, data) values (?, ?)
        }, undef, $id, $data); #TODO: prepare/execute?

    }

    $self->{_dbh}->commit; # fsync?

    $self->{_db_size} += @$chunk;
}

sub load {
    my $self = shift;
    my ($limit) = @_;
    $limit ||= 1;

    return [splice @{$self->{_buffer}}, 0, $limit];
}

sub delete {
    my $self = shift;
    my ($ids) = @_; #TODO: no ids => delete everything already loaded

    for my $id (@$ids) {
    
        unless ($self->{_dbh}->do(qq{
            delete from buffer where id = ?
        }, undef, $id)) {
            die "commit: unknown id: $id";
        }
    }
    $self->{_dbh}->commit;
    $self->{_db_size} -= @$ids;
}

sub lag {
    my $self = shift;
    my $lag = 0;
    $lag += length $_->[1] for @{$self->{_buffer}}; #FIXME: sum from all chunks
    return $lag;
}

1;
