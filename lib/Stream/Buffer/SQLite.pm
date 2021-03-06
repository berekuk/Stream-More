package Stream::Buffer::SQLite;

# ABSTRACT: ulitily class to store uncommited data in a local sqlite buffers

use Moo;
with 'Stream::Buffer::Role';

use Yandex::Logger;
use DBI;
use DBD::SQLite;
use Yandex::Lockf 3.0.0;

use Types::Standard qw(Int);

use namespace::clean;

=head1 SYNOPSIS

    $buf = Stream::Buffer::SQLite->new({ dir => $dir, max_chunk_size => ..., max_chunk_count => ... });
    $buf->save([$data1, $data2]);
    [[$id1 => $data1], [$id2 => $data2], [$id3 => $data3]] = $buf->load(3);
    [[$id4 => $data4]] = $buf->load();
    $buf->delete([$id1, $id3]);
    $lag = $buf->lag();

=head1 DESCRIPTION

No read/only support.

=head1 METHODS

=over

=item B<< new($params) >>
=item B<< new(%$params) >>

Constructor.

=over

=item I<dir>

A local directory to store uncommited data. The buffer is implemented as a set of SQLite databases. This parameter is mandatory.

=item I<max_chunk_size>

Maximum number of items to be stored in a single SQLite database. 1000 by default. Set to 0 to disable the chunk size control.

=item I<max_chunk_count>

Maximum number of SQLite databases to create. 100 by default. Multiple databases are required to provide a concurrent access to the buffer.

=back

=cut

my $counter = 0;

sub _lockf {
    my ($file, $opts) = @_;
    while () {
        my $lockf = lockf($file, $opts);
        return $lockf unless $lockf;
        unless (-e $file) {
            DEBUG "$file: locked but removed";
            next;
        }
        unless ((stat $lockf->{_fh})[1] eq (stat $file)[1]) {
            DEBUG "$file: locked but removed and created back";
            next;
        }
        return $lockf;
    }
}

has 'dir' => (
    is => 'ro',
    required => 1,
);

has 'max_chunk_size' => (
    is => 'ro',
    isa => Int,
    default => sub { 1000 },
);

has 'max_chunk_count' => (
    is => 'ro',
    isa => Int,
    default => sub { 100 },
);

sub BUILD {
    my $self = shift;
    $self->_create_buffer();
}

sub DEMOLISH {
    local $@;
    my $self = shift;
    $self->{_dbh}->rollback() if $self->{_dbh};
    unlink $self->{_db_file} if $self->{_db_file} and defined $self->{_db_size} and $self->{_db_size} == 0;
}

sub _find_buffer {
    my $self = shift;

    my @files = sort glob("$self->{dir}/*.sqlite");
    $self->{_chunk_count} = scalar(@files);

    for my $file (@files) {
        my $lockf = _lockf($file, { blocking => 0 });
        next unless $lockf;
        DEBUG "$file: found and locked";
        return ($lockf, $file);
    }

    return;
}

sub _create_buffer {
    my $self = shift;

    my ($lockf, $file) = $self->_find_buffer();

    unless ($file) {
        die "Chunk limit exceeded: $self->{dir}" if $self->{max_chunk_count} and $self->{_chunk_count} >= $self->{max_chunk_count};
        while () {
            $file = "$self->{dir}/" . time . ".$$." . $counter++ . ".sqlite";
            $lockf = _lockf($file, { blocking => 0 });
            unless ($lockf) { # newly created file was locked by someone else
                DEBUG "$file: created but stolen";
                next;
            }
            DEBUG "$file: created and locked";
            last;
        }
    }

    $self->{_lockf} = $lockf;
    $self->{_db_file} = $file;

    $self->{_dbh} = $self->_init_db($file);

    $self->{_buffer} = $self->{_dbh}->selectall_arrayref(qq{
        select id, data from buffer order by id
    });
    $self->{_dbh}->commit(); # commit after select, yep

    $self->{_id} = $self->{_buffer}->[-1]->[0] + 1 if @{$self->{_buffer}};
    $self->{_id} ||= 0;

    $self->{_db_size} = @{$self->{_buffer}};
}

sub _init_db {
    my ($self, $file) = @_;
    my $dbh = DBI->connect("dbi:SQLite:dbname=$file", "", "", {
        AutoCommit => 0,
        RaiseError => 1,
        PrintError => 0,
    });
    $dbh->do(qq{pragma synchronous = off});
    $self->_upgrade($dbh);
    return $dbh;
}

sub _get_version {
    my ($self, $dbh) = @_;
    my $version = 0;
    eval { ($version) = $dbh->selectrow_array("select version from version"); };
    die if $@ and $@ !~ /no such table/;
    return $version;
}

sub _upgrade_0_to_1 {
    my ($self, $dbh) = @_;

    $dbh->do("create table version (version int)");
    $dbh->do("insert into version (version) values (1)");

    $dbh->do(qq{
        create table buffer (
            id bigint primary key,
            data blob
        )}); # TODO: locked -> bigint?
    $dbh->commit();
}

sub _upgrade {
    my ($self, $dbh) = @_;
    my $version = $self->_get_version($dbh);
    $self->_upgrade_0_to_1($dbh) if $version == 0;
}

sub _id {
    my $self = shift;
    return $self->{_id}++;
}

sub save {
    my $self = shift;
    my ($chunk) = @_;
    my $chunk_size = @$chunk;

    die "Chunk size exceeded: $self->{dir}: $self->{_db_file}" if $self->{max_chunk_size} and $self->{_db_size} + $chunk_size > $self->{max_chunk_size};

    for my $data (@$chunk) {
        my $id = $self->_id;
        push @{ $self->{_buffer} }, [$id => $data];

        $self->{_dbh}->do(qq{
            insert into buffer (id, data) values (?, ?)
        }, undef, $id, $data); #TODO: prepare/execute?
    }

    $self->{_dbh}->commit(); # fsync?

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

            my ($lockf, $file) = $self->_find_buffer();
            last unless $file;

            my $dbh = $self->_init_db($file);

            my $buffer = $dbh->selectcol_arrayref(qq{
                select data from buffer
            });
            $dbh->commit();

            $self->save($buffer);
            undef $dbh;
            unlink $file or die "$file: unlink failed: $!";
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

        my $deleted = $self->{_dbh}->do(qq{
            delete from buffer where id = ?
        }, undef, $id);

        if ($deleted == 0) {
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

1;
