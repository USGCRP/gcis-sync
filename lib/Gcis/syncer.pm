package Gcis::syncer;
use Gcis::syncer::util qw/:log/;
use v5.14;

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

sub audit_note {
    return shift->{audit_note};
}

my $_logger;
sub logger {
    my $arg = shift;
    return $_logger unless @_;
    $_logger = shift;
    warn "unknown logger" unless $_logger->isa("Mojo::Log");
    return $_logger;
}

sub lookup_or_create_gcid {
    my $s = shift;
    my %args = @_;
    my ($lexicon,$context,$term,$gcid) =
        @args{qw/lexicon context term gcid/};
    
    debug "looking for /lexicon/$lexicon/find/$context/$term";
    $s->gcis->ua->max_redirects(0);
    my $existing = $s->gcis->get("/lexicon/$lexicon/find/$context/$term");
    $s->gcis->ua->max_redirects(5);
    if ($existing) {
        my $gcid = $existing->{gcid};
        return $gcid;
    }
    debug "Making a new gcid $lexicon, $context, $term -> $gcid";

    # Make a new one.
    $gcid =~ m[^/] or die "invalid gcid $gcid";

    return $gcid if $args{dry_run};
    $s->gcis->post("/lexicon/$lexicon/term/new",
        { context => $context, term => $term, gcid => $gcid }) or die $s->gcis->error;
    return $gcid;
}


1;
