package Gcis::syncer::yaml;
use base 'Gcis::syncer';

use Smart::Comments -ENV;
use Mojo::UserAgent;
use Gcis::syncer::util qw/:log pretty_id/;
use Data::Dumper;
use List::MoreUtils qw/mesh/;
use YAML::XS qw/Load/;
use Path::Class qw/file dir/;
use FindBin;
use v5.14;

# Base sync class takes care of these yaml keys :
my %base_handles = (
  create       => 1,
  gcid         => 1,
  exterms      => 1,
  record       => 1,
  files        => 1,
  contributors => 1,
);

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
        my $child_class = $file->dir->basename;
        my $plugin = join '::', __PACKAGE__, $child_class;
        eval "use $plugin";
        if ($@) {
            warn $@ unless $@ =~ /^Can't locate.*$child_class.pm/;
            bless $s, __PACKAGE__;
        } else {
            bless $s, $plugin;
        }
        debug "file : $file";
        $s->_ingest_file($file, $gcid, $dry_run);
    } );
}

# record :
#    identifier : foo
#    field : value
#    fields and values should match the API (which matches the database)
sub _ingest_record {
  my $s          = shift;
  my $gcid       = shift; # may be tbd
  my $create_url = shift or die "Missing create url";
  my $record     = shift or die "Missing record";

  my $url = $create_url;
  if ($gcid and (my $existing = $s->gcis->get($gcid))) {
    unless (ref $existing eq 'HASH') {
        error "Existing $gcid is not a hashref".Dumper($existing);
        return;
    }
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
            my $res = $s->gcis->get($parent);
            $s->gcis->ua->max_redirects(5);
            unless ($res) {
                error "Cannot resolve $parent";
                next;
            }
            $parent = $res->{gcid};
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
    my $dest = $gcid;
    $dest =~ s[/([^/]+)$][/contributors/$1] or die "cannot form contributors URL from GCID $gcid";
    for my $role (keys %$contributors) {
        my $these = $contributors->{$role};
        $these = [ $these ] unless ref $these eq 'ARRAY';
        for my $uri (@$these) {
            $uri =~ m[/organization/(.*)$] or die "only /organization contributors so far ($uri)";
            my $organization_identifier = $1;
            debug "adding $role $organization_identifier";
            $s->gcis->post( $dest => {
                organization_identifier => $organization_identifier,
                role                    => $role
              }) or error ($s->gcis->error // "failed to add $organization_identifier");
        }
    }
    return 1;
}


#exterms :
#    - /podaac/Platform/TOPEX/POSIDEON
#    - /ceos/Mission/Topex-Poseidon
#    - /gcmd/platform/e5eb6afb-5d3e-4767-ad08-5293c5b2d88b
sub _ingest_exterms {
    my $s = shift;
    my $gcid = shift;
    my $exterms = shift or return;
    $exterms = [ $exterms ] unless ref $exterms eq 'ARRAY';
    for my $exterm (@$exterms) {
        debug "mapping $exterm to $gcid";
        my ($lexicon,$context,$term) = $exterm =~ m[^/([^/]+)     # lexicon
                                                     /([^/]+)     # context
                                                     /(.*)        # term
                                                     $]x or error "bad exterm $exterm";
        info "adding term '$term', context '$context' -> $gcid";
        $s->gcis->post("/lexicon/$lexicon/term/new" => {
                term => $term, context => $context, gcid => $gcid
            });
    }
}

sub _infer_gcid {
    my $s = shift;
    my $file = shift;
    my $name = $file->basename;
    my $dir = $file->dir->basename;
    $name =~ s/\.yaml$//;
    my $gcid = "/". join '/', $dir, $name;
    return $gcid;
}

sub _munge_record {
    my $s = shift;
    my $record = shift;
    # overload this in derived classes
    return $record;
}

sub _determine_create_endpoint {
    my $s = shift;
    my $file = shift;
    my $dir = $file->dir->basename;
    # e.g. POST to /model creates a model.
    return "/$dir";
}

sub _extract_gcid {
    my $s = shift;
    my ($file,$record) = @_;
    return '/' . $file->dir->basename . '/' . $record->{identifier};
    # override to provide a way to extract a gcid from a file/record
    return undef;
}

sub _exclude_file {
    # return true if we should exclude this file based on the gcid regex.
    my $s = shift;
    my $file = shift;
    my $gcid_regex = shift;
    my $record = shift;
    return 0 unless $gcid_regex;
    my $test_gcid = '/' . $file->dir->basename .'/' . ($record->{identifier} // $file->basename);
    # yes exclude, if we don't match.
    debug "testing gcid $test_gcid against $gcid_regex";
    return 1 if $test_gcid !~ /^$gcid_regex/;
    return 0;
}
sub _ingest_file {
    my $s         = shift;
    my $file      = shift;
    my $gcid_regex = shift;
    my $dry_run   = shift;
    my $data = eval { Load(scalar $file->slurp) };
    if ($@) {
        error "$file : $@";
        return;
    }
    return if $dry_run;
    my $create_endpoint = $data->{create} || $s->_determine_create_endpoint($file);
    #
    # Accepted formats :
    #
    #  (1)
    #      field1 : value1
    #      field2 : value2
    #
    #  (2)
    #     record :
    #          field1 : value1
    #          field2 : value2
    #
    #  (3)
    #     record :
    #        - field1 : value1
    #          field2 : value
    #
    #
    my $records =
          !exists($data->{record})        ? [ $data           ]  # (1)
        : ref($data->{record}) ne 'ARRAY' ? [ $data->{record} ]  # (2)
        : $data->{record};                                       # (3)
    die "not an array ref : ".Dumper($data) unless ref($records) eq 'ARRAY';
    my $default = $data->{_record_default} || {};
    my $gcid;
    for my $record (@$records) {
        my %merged = ( %$default, %$record );
        next if $s->_exclude_file($file, $gcid_regex, $record);
        my $munged = $s->_munge_record(\%merged) or next;
        $gcid = $data->{gcid} // $s->_extract_gcid($file,$munged);
        next if $gcid && ($gcid_regex && $gcid !~ /^$gcid_regex/);
        debug "file : ".$file;
        debug "gcid : ".($gcid // "unknown");
        $s->_ingest_record($gcid, $create_endpoint => $munged ) or return 0;
    }
    return unless $gcid;
    return if $gcid_regex && $gcid !~ /^$gcid_regex/;
    $s->_ingest_prov($gcid => $data->{prov});
    $s->_ingest_files($gcid => $data->{files});
    $s->_ingest_contributors($gcid => $data->{contributors});
    $s->_ingest_exterms($gcid => $data->{exterms});
}


1;

