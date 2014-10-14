package Gcis::syncer;
use Gcis::syncer::util qw/:log/;
use Mojo::Base qw/-base/;
use Path::Class qw/dir file/;
use YAML::XS qw/Dump/;
use v5.14;

has 'stats';
has 'gcis';
has 'audit_note';
has 'base_path' => sub { die "no base extraction path defined"; };

sub sync {
    die "implemented by derived class";
}

my $_logger;
sub logger {
    my $arg = shift;
    return $_logger unless @_;
    $_logger = shift;
    warn "unknown logger" unless $_logger->isa("Mojo::Log");
    return $_logger;
}

sub lookup_gcid {
    my $s = shift;
    my ($lexicon, $context, $term) = @_;
    $s->gcis->ua->max_redirects(0);
    my $existing = $s->gcis->get("/lexicon/$lexicon/find/$context/$term");
    $s->gcis->ua->max_redirects(5);
    return unless $existing;
    return $existing->{gcid};
}

sub lookup_or_create_gcid {
    my $s = shift;
    my %args = @_;
    my ($lexicon,$context,$term,$gcid,$restrict) =
        @args{qw/lexicon context term gcid restrict/};
    
    # debug "looking for /lexicon/$lexicon/find/$context/$term";
    $s->gcis->ua->max_redirects(0);
    my $existing = $s->gcis->get("/lexicon/$lexicon/find/$context/$term");
    $s->gcis->ua->max_redirects(5);
    if ($existing) {
        my $gcid = $existing->{gcid};
        die "found invalid gcid" if $gcid =~ / /;
        return if $restrict && $gcid !~ /$restrict/;
        return $gcid;
    }

    # Make a new one.
    $gcid =~ m[^/] or die "invalid gcid $gcid";

    return if $restrict && $gcid !~ /$restrict/;
    return $gcid if $args{dry_run};

    debug "Making a new term $lexicon, $context, $term -> $gcid";
    $s->gcis->post("/lexicon/$lexicon/term/new",
        { context => $context, term => $term, gcid => $gcid }) or die $s->gcis->error;
    return $gcid;
}

sub write_resource {
    my $s = shift;
    my %a = @_;
    my $dir        = $a{dir}        or die "missing dir";
    my $identifier = $a{identifier} or die "missing identifier";
    my $contents   = $a{contents}   or die "missing contents";

    my $top = dir($s->base_path);
    -d $top or die "cannot open $top";
    my $sub = $top->subdir($dir);
    -d $sub or $sub->mkpath;
    my $file = $sub->file("$identifier.yaml");

    my $new = Dump($contents);

    if (-e $file) {
        my $existing = scalar $file->slurp;
        return 1 if $existing eq $new;
    }
    debug "writing to $file";
    $file->spew($new);
    return 1;
}

1;
