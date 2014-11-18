package Gcis::syncer::yaml::model_run;
use base 'Gcis::syncer::yaml';
use Gcis::syncer::util qw/:log/;
use v5.14;

sub _munge_record {
    my $s = shift;
    my $record = shift;
    my $identifier = $record->{model_identifier};

    return $record unless $identifier =~ m[/lexicon/([^/]+)/([^/]+)/(.*)$];
    my $gcid = $s->lookup_gcid($1,$2,$3);
    unless ($gcid) {
        error "No gcid found for $identifier";
        return;
    }
    my ($new_identifier) = $gcid =~ m[/model/(.*)$];
    unless ($new_identifier) {
        error "could not find model id in $gcid";
        return;
    }
    $record->{model_identifier} = $new_identifier;
    return $record;
}

1;
