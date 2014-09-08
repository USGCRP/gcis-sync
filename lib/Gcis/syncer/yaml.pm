package Gcis::syncer::yaml;
use base 'Gcis::syncer';

use Smart::Comments;
use Mojo::UserAgent;
use Gcis::syncer::util qw/:log pretty_id/;
use Data::Dumper;
use List::MoreUtils qw/mesh/;
use YAML::XS qw/Load/;
use Path::Class qw/file dir/;
use FindBin;
use v5.14;

sub sync {
    my $s = shift;
    my %a = @_;
    my $limit   = $a{limit};
    my $dry_run = $a{dry_run};
    my $gcid    = $a{gcid};
    my $c       = $s->{gcis} or die "no client";

    my $dir = $FindBin::Bin.'/yaml';
    -d $dir or die "cannot find $dir";
    debug "$dir";
    dir($dir)->recurse( callback => sub {
        my $file = shift;
        return if $file->is_dir();
        return unless $file->basename =~ /\.yaml$/;
        $s->_ingest_file($file, $gcid, $dry_run);
    } );
}

# record :
#    identifier : foo
#    field : value
#    fields and values should match the API (which matches the database)
sub _ingest_record {
  my $s          = shift;
  my $gcid       = shift;
  my $create_url = shift;
  my $record     = shift;

  my $url = $create_url;
  if (my $existing = $s->gcis->get($gcid)) {
    $url = $existing->{uri};
  }
  $s->gcis->post($url, $record) or return error $s->gcis->error;
  return 1;
}

# prov :
#    uri : /dataset/prov/nasa-podaac-integrated-multi-mission-ocean-altimeter-data-for-climate-research
#    entries :
#    - rel    : prov:wasDerivedFrom
#      parent : /lexicon/podaac/find/dataset/MERGED_TP_J1_OSTM_OST_CYCLES_V2
#    - rel    : prov:wasDerivedFrom
#      parent : /lexicon/podaac/find/dataset/MERGED_TP_J1_OSTM_OST_ALL_V2
#    - rel    : prov:wasDerivedFrom
#      parent : /lexicon/podaac/find/dataset/MERGED_TP_J1_OSTM_OST_GMSL_ASCII_V2
sub _ingest_prov {
    my $s = shift;
    my $gcid = shift;
    my $prov = shift or return;
    info "updating prov : $prov->{uri}";
    for my $entry (@{ $prov->{entries} }) {
        my $parent = $entry->{parent};
        if ($parent =~ m[^/lexicon]) {
            $s->gcis->ua->max_redirects(0);
            $parent = $s->gcis->get($parent)->{gcid};
            $s->gcis->ua->max_redirects(5);
        }
        die "missing rel" unless $entry->{rel};
        $s->gcis->post( $prov->{uri}, {
                parent_uri => $parent,
                parent_rel => $entry->{rel} } ) or error $s->gcis->error;
    }
}

# files :
#    location : https://eoportal.org/documents/163813/212645/TOPEX_Auto8
#    landing_page : https://eoportal.org/web/eoportal/satellite-missions/t/topex-poseidon
sub _ingest_files {
    my $s = shift;
    my $gcid = shift;
    my $files = shift or return;
    info "ingesting files";
    my $url = $gcid;
    $url =~ s[/([^/]+)$][/files/$1];
    $files = [ $files ] if ref $files eq 'HASH';
    for my $file (@$files) {
        debug "post to $url";
        $s->gcis->post($url => {
            file_url => $file->{location},
            landing_page => $file->{landing_page}
        }) or return error $s->gcis->error;
    }
    return 1;
}

#contributors :
#    funding_agency :
#        - /organization/national-aeronautics-space-administration
#        - /organization/centre-national-d-etudes-spatiales
sub _ingest_contributors {
    my $s = shift;
    my $gcid = shift;
    my $contributors = shift or return;

}

sub _ingest_file {
    my $s         = shift;
    my $file      = shift;
    my $only_gcid = shift;
    my $dry_run   = shift;
    my $data = Load(scalar $file->slurp);
    my $gcid = $data->{gcid} or return error "missing gcid in $file";
    return if $only_gcid && $gcid ne $only_gcid;
    return if $dry_run;
    debug "file : ".$file->basename;
    debug "gcid : ".$gcid;
    $s->_ingest_record($gcid, $data->{create} => $data->{record}) or return 0;
    $s->_ingest_prov($gcid => $data->{prov});
    $s->_ingest_files($gcid => $data->{files});
    $s->_ingest_contributors($gcid => $data->{contributors});
    # TODO the same for instruments if this is a /platform
}

1;

