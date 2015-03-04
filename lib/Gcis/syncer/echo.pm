package Gcis::syncer::echo;
use Gcis::Client;
use base 'Gcis::syncer';
use Gcis::syncer::util qw/:log iso_date pretty_id/;
use Smart::Comments;
use JSON::XS;
use Mojo::UserAgent;
use IO::Uncompress::Unzip qw/unzip $UnzipError/;
use Data::Dumper;
use v5.14;

our $src = qw[https://api.echo.nasa.gov/catalog-rest/echo_catalog/datasets.echo10];
our $map = {
      identifier  => sub {
          my $dom = shift;
          my $archive_center = $dom->at('ArchiveCenter') or return;
          $archive_center = lc $archive_center->text;
          my $native_id = lc $dom->at('Collection > ShortName')->text;
          $archive_center .= 'daac' unless $archive_center =~ /daac$/;
          $native_id =~ s/^$archive_center-//;
          return "nasa-".(lc $archive_center)."-$native_id";
      },
      name        => "Collection > DataSetId",
      description => "Collection > Description",
      native_id   => "Collection > ShortName",
      url         => [
                       "OnlineResources > OnlineResource > URL",
                       "OnlineAccessURLs > OnlineAccessURL > URL",
                       sub { warn "no url for ".shift->attr('echo_dataset_id'); return undef; },
                      ],
      release_dt => "Collection > InsertTime",
      lat_min    => "Collection > Spatial > HorizontalSpatialDomain > Geometry > BoundingRectangle > SouthBoundingCoordinate",
      lat_max    => "Collection > Spatial > HorizontalSpatialDomain > Geometry > BoundingRectangle > NorthBoundingCoordinate",
      lon_min    => "Collection > Spatial > HorizontalSpatialDomain > Geometry > BoundingRectangle > WestBoundingCoordinate",
      lon_max    => "Collection > Spatial > HorizontalSpatialDomain > Geometry > BoundingRectangle > EastBoundingCoordinate",
      start_time => [
                     "Collection > Temporal > RangeDateTime > BeginningDateTime",
                     "Collection > Temporal > RangeDateTime > SingleDateTime",
                    ],
      end_time   => [
                     "Collection > Temporal > RangeDateTime > EndingDateTime",
                     "Collection > Temporal > RangeDateTime > SingleDateTime",
                    ],
      doi => sub {
          my $dom = shift;
          my $identifier = $dom->at("Collection > ShortName")->text;
          die "no shortname in ".$dom->to_string unless $identifier;
          $identifier =~ /^doi:(.*)$/ and return $1;
          for my $url ($dom->find("OnlineResource > URL")->each) {
              $url->text =~ m[dx.doi.org/(.*)$] and return $1;
          }
          return undef;
      },
};

our $daacs = {
    nsidc => '/organization/national-snow-ice-data-center',
};

sub sync {
    my $s = shift;
    my %a = @_;
    my $limit   = $a{limit};
    my $dry_run = $a{dry_run};
    my $gcid    = $a{gcid};
    my $c = $s->{gcis} or die "no client";
    return if ($gcid && $gcid !~ /^\/dataset\/nasa-echo-/);
    my %stats;

    my $per_page    = 500;
    my $more        = 1;
    my $start_index = 1;
    my $ua          = Mojo::UserAgent->new();
    my $url         = Mojo::URL->new($src)->query(page_size => $per_page);
    my $page        = 1;
    my $count       = 0;

    REQUEST :
    while ($more) {
        $more = 0;
        debug "getting $url (page $page)";
        my $tx = $ua->get($url->query([ page_num => $page++ ]));
        my $res = $tx->success or die "$url : ".Dumper($tx->error);
        for my $entry ($res->dom->find('result')->each) {  ### Processing===[%]       done
            $more = 1;
            my $daac = $entry->at('ArchiveCenter') or do {
                info "no archivecenter for ".$entry->at('ShortName')->text;
                next;
            };
            $daac = lc $daac->text;
            next unless $daac eq 'nsidc';
            debug "archive center : $daac";
            last REQUEST if $limit && $count++ > $limit;
            my %gcis_info = $s->_extract_gcis($entry);
            next if $gcid && $gcid ne "/dataset/$gcis_info{identifier}";

            # insert or update
            my $existing = $c->get("/dataset/$gcis_info{identifier}");
            my $url = $existing ? "/dataset/$gcis_info{identifier}" : "/dataset";
            $stats{ ($existing ? "updated" : "created") }++;
            debug "xml data : ".$entry->to_string;
            debug "gcis data : ".Dumper(\%gcis_info);
            if ($dry_run) {
                info "ready to POST to $url";
                next;
            }
            # TODO skip if unchanged
            $c->post($url => \%gcis_info) or do {
                error $c->error;
                die "bailing out, error : ".$c->error;
            };
            if (my $archive = $daacs->{$daac}) {
                my $id = $gcis_info{identifier} or die "no id in ".Dumper(\%gcis_info);
                $s->_assign_contributors( "/dataset/$id", \%gcis_info, $archive, $dry_run);
            } else {
                die "contributor not found : $daac";
            }
        }
    }

    $s->{stats} = \%stats;
    return;
}

sub extract {
    my $e = shift;
    my $dom = shift;

    ref($e) eq 'CODE' and return $e->($dom);
    !ref($e) and return $dom->at($e);
    ref($e) eq 'ARRAY' and do {
        for (@$e) {
            my $val = extract($_, $dom);
            $val = $val->text if ref($val) eq 'Mojo::DOM';
            return $val if $val && $val !~ /^\s+$/;
        }
    };
    return undef;
}

sub fmt {
    my ($val,$field) = @_;
    return $val unless defined $val && length($val);
    $val = $val->text if ref($val) eq 'Mojo::DOM';
    return iso_date($val) if $field && $field =~ /_(dt|time)$/;
    return $val;
}

sub _extract_gcis {
    state $count;
    my $s = shift;
    my $dom = shift;
    our $map;

    my %new = map {
             $_ => fmt(extract($map->{$_}, $dom), $_)
         } keys %$map;
    debug "platform : ".fmt(extract("Platforms > Platforms > ShortName", $dom));
    debug "Instrument : ".fmt(extract("Instrument > ShortName", $dom));
    debug "sensor : ".fmt(extract("Sensor > ShortName", $dom));
    $count++;
    debug "extracting entry $count : $new{identifier} : $new{native_id}";
    return %new;
}

sub _assign_contributors {
    my $s = shift;
    my ($gcid, $info, $data_archive, $dry_run ) = @_;
    die "no gcid" unless $gcid;
    return if $dry_run;
    my $contribs_url = $gcid =~ s[dataset/][dataset/contributors/]r;
    $s->gcis->post($contribs_url => {
                organization_identifier => $data_archive,
                role => 'data_archive'
        }) or error $s->gcis->error;
}

