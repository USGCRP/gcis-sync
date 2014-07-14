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

my $_logger;
sub logger {
    my $arg = shift;
    return $_logger unless @_;
    $_logger = shift;
    warn "unknown logger" unless $_logger->isa("Mojo::Log");
    return $_logger;
}


1;
