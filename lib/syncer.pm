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

1;
