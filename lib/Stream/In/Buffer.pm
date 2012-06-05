package Stream::In::Buffer;

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

    $b_in = Stream::In::Buffer->new($in, { dir => $dir, size => $size });
    my ($id1, $data1) = @{$b_in->read()};
    my ($id2, $data2) = @{$b_in->read()};
    my ($id3, $data3) = @{$b_in->read()};
    ...
    $b_in->commit([$id1, $id3]);
    undef $b_in; # $data2 remains uncommited and would be returned again.

=head1 DESCRIPTION

MQ!
No multiprocessing support.
No read/only support.

=head1 METHODS

=over

=item B<< new($in, $options) >>

Constructor.

C<$options> is an optional hash with options:

=over

=item I<dir>

A local directory to store uncommited data. Right now the buffer is implemented as an SQLite database.

=item I<size>

Maximum number of items to be stored in a temporary buffer. You have to commit hence discard some of them to get new ones. Set this option to 0 to disable buffer size control. 

=back

=cut
sub new {
    my $class = shift;
    my ($in, @options) = validate_pos(@_, { type => CODEREF | OBJECT }, { type => HASHREF, optional => 1 });

    my $self = validate(@options, {
        dir => 1,
        size => { default => 0 },
    });
    bless $self => $class;
    $self->{in} = ref $in eq "CODE" ? $in->() : $in;
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

sub _load_from_in {
    my $self = shift;
    my ($limit, $result) = @_;

    die "buffer exhausted: $self->{_db_size} + $limit > $self->{size}" if $self->{size} and $self->{_db_size} + $limit > $self->{size};

    my $in = $self->{in};
    my $chunk = $in->read_chunk($limit); #TODO: load some more in advance?

    for my $data (@$chunk) {

        my $id = $self->_id;
        push @$result, [$id => $data];
        $self->{_dbh}->do(qq{
            insert into buffer (id, data) values (?, ?)
        }, undef, $id, $data); #TODO: prepare/execute?

    }

    $self->{_dbh}->commit; # fsync?
    $in->commit;
    
    $self->{_db_size} += @$chunk;
}

sub _load_from_cache {
    my $self = shift;
    my ($limit, $result) = @_;

    push @$result, splice @{$self->{_buffer}}, 0, $limit;
}

sub read_chunk {
    my $self = shift;
    my ($limit) = @_;

    my $result = [];

    $self->_load_from_cache($limit => $result);
    $limit -= @$result;
    return $result if $limit <= 0;

    $self->_load_from_in($limit => $result);

    return $result;
}

sub read {
    my $self = shift;
    my $chunk = $self->read_chunk(1);
    return unless $chunk;
    return $chunk->[0];
}

sub commit {
    my $self = shift;
    my $ids = \@_;

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
    my $lag = $self->{in}->lag();
    $lag += length $_->[1] for @{$self->{_buffer}}; 
    return $lag;
}

1;
