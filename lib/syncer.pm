package syncer;

sub new {
    my $s = shift;
    my %a = @_;
    bless \%a, $s;
}

sub sync {
    die "implemented by derived class";
}

sub stats {
    shift->{stats};
}

sub gcis {
    return shift->{gcis};
}

sub logger {
    return shift->gcis->logger;
}

1;
