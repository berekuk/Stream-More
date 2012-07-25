package Stream::Buffer::File;

use namespace::autoclean;

use Moose;
with 'Stream::Buffer::Role';

use Params::Validate qw(:all);

use Yandex::Logger;
use Yandex::X qw(xopen xclose xunlink);
use Yandex::Lockf 3.0.0;

=head1 DESCRIPTION

This is a simple files-based buffer implementation.

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
    isa => 'Int',
    default => 1000,
);

has 'max_log_size' => (
    is => 'ro',
    isa => 'Int',
    default => 10000,
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
    xunlink $self->{_file} if $self->{_file} and defined $self->{_state} and scalar(keys %{$self->{_state}}) == 0;
}

sub _find_buffer {
    my $self = shift;

    my @files = sort glob("$self->{dir}/*.log");
    $self->{_chunk_count} = scalar(@files);

    for my $file (@files) {
        my $lockf = _lockf($file, { blocking => 0 });
        next unless $lockf;
        DEBUG "$file: found and locked";
        return ($lockf, $file);
    }
    return;
}

sub _dump_file {
    my $self = shift;
    my ($file, $buffer, $state) = @_;
    
    my $fh = xopen '<', $file;
    my $log_size = 0;
    while (my $l = <$fh>) {
        chomp $l;
        if ($l =~ m{^(\d+)\t([\+\-])\t(.+)$}) {
            my ($id, $type, $cur_data) = ($1, $2, $3);
            if ($type eq "+") {
                die "There is already an element with same id, file $file, line $l" if $state->{$id};
                $state->{$id} = $cur_data;
            }
            else {
                die "There is no element with id $id, file $file, line $l" unless $state->{$id};
                delete $state->{$id};
            }
        }
        else {
            die "Broken log $file, line $l";
        }
        ++$log_size;
    }

    @$buffer = map { [ $_ => $state->{$_} ] } sort {$a <=> $b} keys %$state;
    xclose $fh;
    return $log_size; 
};

sub _create_buffer {
    my $self = shift;

    my ($lockf, $file) = $self->_find_buffer();

    unless ($file) {
        die "Chunk limit exceeded: $self->{dir}" if $self->{max_chunk_count} and $self->{_chunk_count} >= $self->{max_chunk_count};
        while () {
            $file = "$self->{dir}/" . time . ".$$." . $counter++ . ".log";
            $lockf = _lockf($file, { blocking => 0 });
            unless ($lockf) {
                DEBUG "$file: created but stolen";
                next;
            }
            DEBUG "$file: created and locked";
            last;
        }
    }

    $self->{_lockf} = $lockf;
    $self->{_file} = $file;

    $self->{_buffer} = [];
    $self->{_state} = {};
    $self->{_log_size} = $self->_dump_file($file, $self->{_buffer}, $self->{_state});

    $self->{_id} = $self->{_buffer}->[-1]->[0] + 1 if @{$self->{_buffer}};
    $self->{_id} ||= 0;

    $self->{_items_size} = scalar(@{$self->{_buffer}});
}

sub _flush_buffer {
    my $self = shift;

    my $old_lock = $self->{_lockf};
    my $old_file = $self->{_file};
        
    DEBUG "Log size exceeded, creating new log file";
    my ($file, $lockf);
    while () {
        $file = "$self->{dir}/" . time . ".$$." . $counter++ . ".log";
        $lockf = _lockf($file, { blocking => 0 });
        unless ($lockf) {
            DEBUG "$file: created but stolen";
            next;
        }
        DEBUG "$file: created and locked";
        last;
    }
    
    $self->{_lockf} = $lockf;
    $self->{_file} = $file;

    my $state = $self->{_state};
    my @log_data = map { [ $_ => $state->{$_} ] } sort {$a <=> $b} keys %$state;
    
    my $fh = xopen '>', $file;
    for my $item (@log_data) {
        print $fh $item->[0] . "\t+\t" . $item->[1] . "\n";
    }
    xclose $fh;

    $self->{_log_size} = scalar(@log_data);

    xunlink $old_file;
    undef $old_lock;
}

sub _id {
    my $self = shift;
    return $self->{_id}++;
}

sub save {
    my $self = shift;
    my ($chunk) = @_;
    my $chunk_size = @$chunk;

    die "Chunk size exceeded: $self->{_file}" if $self->{max_chunk_size} and $self->{_items_size} + $chunk_size > $self->{max_chunk_size};

    $self->_flush_buffer if $self->{_log_size} + $chunk_size > $self->{max_log_size};
    die "Chunk size is good, but still log size if big, very very strange" if $self->{_log_size} + $chunk_size > $self->{max_log_size};

    my $state = $self->{_state};
    my $fh = xopen '>>', $self->{_file};
    for my $data (@$chunk) {
        my $id = $self->_id;
        die "There is already an element with id $id" if $state->{$id};
        $state->{$id} = $data;
        push @{$self->{_buffer}}, [$id => $data];
        print $fh $id . "\t+\t" . $data . "\n";
    }
    xclose $fh;

    $self->{_log_size} += @$chunk;
    $self->{_items_size} += @$chunk;
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

            my $buffer = [];
            my $temp = {};
            $self->_dump_file($file, $buffer, $temp);

            my @data_to_save = map { $_->[1] } @$buffer;

            $self->save(\@data_to_save);
            xunlink $file;
            next;
        }
        else {
            push @$result, splice @{$self->{_buffer}}, 0, $limit - @$result;
        }

        last if $limit <= @$result;
    }

    return $result;
}

sub delete {
    my $self = shift;
    my ($ids) = @_;

    $self->_flush_buffer if scalar(@$ids) + $self->{_log_size} > $self->{max_log_size};
    
    my $state = $self->{_state};
    my $fh = xopen '>>', $self->{_file};
    for my $id (@$ids) {
        die "There is unknown id $id" unless $state->{$id};
        delete $state->{$id};
        print $fh "$id\t-\tundef\n";
    }
    xclose $fh;
    $self->{_items_size} -= @$ids;
    $self->{_log_size} += @$ids;
}

sub lag {
    my $self = shift;
    my $lag = 0;
    $lag += length $_->[1] for @{$self->{_buffer}};
    return $lag;
}

__PACKAGE__->meta->make_immutable;
