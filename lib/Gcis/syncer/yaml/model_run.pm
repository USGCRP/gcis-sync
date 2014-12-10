package Gcis::syncer::yaml::model_run;
use base 'Gcis::syncer::yaml';
use Gcis::syncer::util qw/:log/;
use v5.14;

sub _munge_record {
    my $s = shift;
    my $record = shift;
    die "missing range_start" unless $record->{range_start};

    if ($record->{model_identifier} =~ m[/lexicon/([^/]+)/([^/]+)/(.*)$]) {
        my $gcid = $s->lookup_gcid($1,$2,$3);
        unless ($gcid) {
            error "No gcid found for $record->{model_identifier}";
            return;
        }
        my ($new_identifier) = $gcid =~ m[/model/(.*)$];
        unless ($new_identifier) {
            error "could not find model id in $gcid";
            return;
        }
        $record->{model_identifier} = $new_identifier;
    }
    $record->{sequence} //= 1;
    my $url = '/'. join "/", "model_run", @$record{qw/model_identifier
         scenario_identifier range_start range_end spatial_resolution time_resolution sequence/};
    debug "getting $url";
    if (my $existing = $s->gcis->get($url)) {
        debug "found existing record $existing->{identifier}";
        $record->{identifier} = $existing->{identifier};
    }

    return $record;
}

sub _extract_gcid {
    my $s = shift;
    my $file = shift;
    my $record = shift;
    return "/model_run/$record->{identifier}" if $record->{identifier};
    return undef;
}

1;
