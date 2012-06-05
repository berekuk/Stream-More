package Stream::In::Buffer;

# ABSTRACT: stream decorator to allow non sequential commits

use strict;
use warnings;

use parent qw(
    Stream::In
);

use namespace::autoclean;

use Params::Validate qw(:all);

use Carp;
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

Maximum number of items to be stored in a temporary buffer. You have to commit hence discard some of them to get new ones.

=back

=cut
sub new {
    my $class = shift;
    my ($in, @options) = validate_pos(@_, { type => CODEREF | OBJECT }, { type => HASHREF, optional => 1 });

    my $self = validate(@options, {
        dir => 1,
        size => { default => 10000 },
    });
    bless $self => $class;
    $self->{in} = ref $in eq "CODE" ? $in->() : $in;
    $self->{_db_file} = "$self->{dir}/buffer.sqlite";
    my $time = time;
    our $guid = 0 unless defined $guid;
    $self->{_guid} = "$time.$$.".$guid++;

    $self->_init_db();

    return $self;
}

sub _init_db {
    my $self = shift;
    $self->{_dbh} = DBI->connect("dbi:SQLite:dbname=$self->{_db_file}", "", "", {
        AutoCommit => 0,
        RaiseError => 1,
        PrintError => 0,
    #   sqlite_handle_binary_nulls => 1,
    });
    $self->_upgrade();
    ($self->{_id}) = $self->{_dbh}->selectrow_array(qq{
        select max(id)+1 from buffer
    });
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
            lock text not null default '',
            data blob
        )}); # TODO: locked -> bigint?
    $dbh->do(qq{
        create index lock on buffer (lock)
    });
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

    my $size = $self->{size};
    my ($buffered) = $self->{_dbh}->selectrow_array(qq{
        select count(1) from buffer
    }); #FIXME: track internally

    die "buffer exhausted: $buffered + $limit > $self->{size}" if $buffered + $limit > $self->{size};

    my $in = $self->{in};
    # my $chunk = $in->read_chunk($size - $buffered); # Eager prefetching. Not the best idea.
    my $chunk = $in->read_chunk($limit);

    for my $data (@$chunk) {
       
        my $id = $self->_id;
        push @$result, [$id => $data];
        $self->{_dbh}->do(qq{
            insert into buffer (id, lock ,data) values (?, ?, ?)
        }, undef, $id, $self->{_guid}, $data);

    }

    $self->{_dbh}->commit;
    $in->commit;
    return;
}

sub _load_from_db {
    my $self = shift;
    my ($limit, $result) = @_;

    for (@{$self->{_dbh}->selectall_arrayref(qq{
        select id, data from buffer where lock != ? limit ?
    }, undef, $self->{_guid}, $limit - @$result)}) {

        my ($id, $data) = @$_;
        push @$result, [$id => $data];
        $self->{_dbh}->do(qq{
            update buffer set lock = ? where id = ?
        }, undef, $self->{_guid}, $id);
    }

    $self->{_dbh}->commit;
    return;
}

# TODO: could load from $self->{_buffer} but still have to update locks in db anyway
#sub _load_from_mem {
#    my $self = shift;
#    my ($limit) = @_;
#    ...
#}

sub read_chunk {
    my $self = shift;
    my ($limit) = @_;

    my $result = [];

    $self->_load_from_db($limit => $result);
    $limit -= @$result;
    return if $limit <= 0;

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

    for my $id (@_) {
        $self->{_dbh}->do(qq{
            delete from buffer where id = ?
        }, undef, $id);
    }
    $self->{_dbh}->commit;
}

sub lag {
    my $self = shift;
    my $in_lag = $self->{in}->lag();
    my ($db_lag) = $self->{_dbh}->selectrow_array(qq{
        select sum(length(data)) from buffer
    }); #FIXME: track internally?
    return $in_lag + $db_lag;
}

sub DESTROY {
    my $self = shift;
    # preparing for multiprocessing support
    $self->{_dbh}->do(qq{
        update buffer set lock = '' where lock = ?
    }, undef, $self->{_guid});
    $self->{_dbh}->commit;
    # TODO: gc - unset locks that are not flocked
}

1;
