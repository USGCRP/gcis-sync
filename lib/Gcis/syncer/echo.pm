package Gcis::syncer::echo;
use Gcis::Client;
use base 'Gcis::syncer';
use Gcis::syncer::util qw/:log iso_date/;
use Smart::Comments;
use JSON::XS;
use Mojo::UserAgent;
use IO::Uncompress::Unzip qw/unzip $UnzipError/;
use Data::Dumper;
use v5.14;

our $src = qw[https://api.echo.nasa.gov/catalog-rest/echo_catalog/datasets.echo10];
our $map = {
      identifier  => sub { "nasa-echo-" . (lc shift->attr('echo_dataset_id')); },
      name        => "collection > datasetid",
      description => "collection > description",
      native_id   => "collection > shortname",
      url         => [
                       "result > collection > onlineaccessurls > onlineaccessurl > url",
                       sub { "http://reverb.echo.nasa.gov/reverb?selected=".shift->attr('echo_dataset_id'); }
                   ],
      release_dt => "collection > inserttime",
      lat_min    => "collection > spatial > horizontalspatialdomain > geometry > boundingrectangle > southboundingcoordinate",
      lat_max    => "collection > spatial > horizontalspatialdomain > geometry > boundingrectangle > northboundingcoordinate",
      lon_min    => "collection > spatial > horizontalspatialdomain > geometry > boundingrectangle > westboundingcoordinate",
      lon_max    => "collection > spatial > horizontalspatialdomain > geometry > boundingrectangle > eastboundingcoordinate",
      start_time => [
                     "collection > temporal > rangedatetime > beginningdatetime",
                     "collection > temporal > rangedatetime > singledatetime",
                    ],
      end_time   => [
                     "collection > temporal > rangedatetime > endingdatetime",
                     "collection > temporal > rangedatetime > singledatetime",
                    ],
      doi => sub {
          my $dom = shift;
          my $identifier = $dom->at("collection > shortname")->text;
          $identifier =~ /^doi:(.*)$/ and return $1;
          for my $url ($dom->find("onlineresource > url")->each) {
              $url->text =~ m[dx.doi.org/(.*)$] and return $1;
          }
          return undef;
      },
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
        my $tx = $ua->get($url->query([ page_num => $page++ ]));
        my $res = $tx->success or die $tx->error;
        for my $entry ($res->dom->find('result')->each) {  ### Processing===[%]       done
            last REQUEST if $limit && $count++ > $limit;
            my %gcis_info = $s->_extract_gcis($entry);
            $more = 1;
            next if $gcid && $gcid ne "/dataset/$gcis_info{identifier}";

            # insert or update
            my $existing = $c->get("/dataset/$gcis_info{identifier}");
            my $url = $existing ? "/dataset/$gcis_info{identifier}" : "/dataset";
            $stats{ ($existing ? "updated" : "created") }++;
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
            return $val if $val;
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
    debug "platform : ".fmt(extract("platforms > platform > shortname", $dom));
    debug "Instrument : ".fmt(extract("instrument > shortname", $dom));
    debug "sensor : ".fmt(extract("sensor > shortname", $dom));
    $count++;
    debug "extracting entry $count : $new{identifier} : $new{native_id}";
    return %new;
}


