package Gcis::syncer::podaac;
use base 'Gcis::syncer';

use Gcis::Client;
use Gcis::syncer::util qw/:log iso_date pretty_id/;
use Smart::Comments;
use Mojo::UserAgent;
use Data::Dumper;

use v5.14;
our $src = "http://podaac.jpl.nasa.gov/ws/search/dataset/";
our $meta_src = Mojo::URL->new("http://podaac.jpl.nasa.gov/ws/search/dataset/")
                ->query(format=>'atom', full => 'true', pretty => "true");
                # shortName => MERGED_TP_J1_OSTM_OST_ALL_V2, pretty => true,
my $ua  = Mojo::UserAgent->new();

our $map = {
    identifier  =>  sub { my $dom = shift; my $id = lc $dom->id->text; 
                          return "nasa-podaac-$id" unless $id =~ /^podaac/;
                          return "nasa-$id";
                        },
    name        =>  sub { my $dom = shift; $dom->title->text;         },
    description =>  sub { my $dom = shift; join "\n", $dom->at('content')->text;  },
    native_id   =>  sub { my $dom = shift; $dom->shortName->text;     },
    url         =>  sub { my $dom = shift; $dom->at('link[title="Dataset Information"]')->attr('href'); },
    lon_min     =>  sub { my $dom = shift; my $w = $dom->at('where') or return undef;
                                           [ split / /, $w->Envelope->lowerCorner->text ]->[0]; },
    lat_min     =>  sub { my $dom = shift; my $w = $dom->at('where') or return undef;
                                           [ split / /, $w->Envelope->lowerCorner->text ]->[1]; },
    lon_max     =>  sub { my $dom = shift; my $w = $dom->at('where') or return undef;
                                           [ split / /, $w->Envelope->upperCorner->text ]->[0]; },
    lat_max     =>  sub { my $dom = shift; my $w = $dom->at('where') or return undef;
                                           [ split / /, $w->Envelope->upperCorner->text ]->[1]; },
    start_time  =>  sub { my $start = shift->at('start') or return undef;  return iso_date($start->text) },
    end_time    =>  sub { my $end   = shift->at('end')   or return undef;  return iso_date($end->text)   },
    release_dt  =>  sub { iso_date(shift->updated->text);                                                },
};

sub sync {
    my $s = shift;
    my %a = @_;
    my $limit   = $a{limit};
    my $dry_run = $a{dry_run};
    my $gcid    = $a{gcid};
    my $c       = $s->{gcis} or die "no client";
    return if ($gcid && $gcid !~ /^\/dataset\/nasa-podaac-/);
    my %stats;

    my $per_page    = 400;
    my $more        = 1;
    my $start_index = 1;
    my $url         = Mojo::URL->new($src)->query(format => "atom", itemsPerPage => $per_page);
    my $count       = 1;

    REQUEST :
    while ($more) {
        $more = 0;
        my $tx = $ua->get($url->query([ startIndex => $start_index ]));
        my $res = $tx->success or die $tx->error->{message};
        for my $entry ($res->dom->find('entry')->each) {  ### Processing===[%]       done
            last REQUEST if $limit && $count > $limit;
            $more = 1;
            my %gcis_info = $s->_extract_gcis($entry);

            # Store mappings to both shortName and id
            my $dataset_gcid = $s->lookup_or_create_gcid(
                lexicon => 'podaac', context => 'dataset', term => $gcis_info{native_id},
                gcid => "/dataset/$gcis_info{identifier}", dry_run => $dry_run,
            );
            my $alternate_id = $entry->id->text;
            $s->lookup_or_create_gcid(
                lexicon => 'podaac', context => 'datasetId', term => $alternate_id,
                gcid => $dataset_gcid, dry_run => $dry_run,
            );

            next if $gcid && $gcid ne $dataset_gcid;
            debug "entry #$count : $dataset_gcid";
            $count++;

            # insert or update
            my $existing = $c->get($dataset_gcid);
            my $url;
            $url = "/dataset" unless $existing;
            $stats{ ($existing ? "updated" : "created") }++;
            # TODO skip if unchanged
            debug "sending to $url";
            #debug Dumper(\%gcis_info);
            unless ($dry_run) {
                $c->post($url => \%gcis_info) or do {
                    error $c->error;
                    warn "error : ".$c->error;
                };
            }
            my $meta = $s->_retrieve_dataset_meta($gcis_info{native_id});
            $s->_assign_instrument_instances(\%gcis_info, $meta,$dry_run) if $meta;
        }
        $start_index += $per_page;
    }

    $s->{stats} = \%stats;
    return;
}

sub _retrieve_dataset_meta {
    my $s = shift;
    my $id = shift;
    my $url = Mojo::URL->new($meta_src)->clone->query([shortName => $id, format => 'atom']);
    debug "getting $url";
    my $tx = $ua->get($url);
    my $res = $tx->success or do {
        error $tx->error->{message};
        return;
    };
    return $res->dom;
}

sub _extract_gcis {
    my $s = shift;
    my $dom = shift;
    our $map;

    my %new = map { $_ => $map->{$_}->( $dom ) } keys %$map;
    debug "extracting $new{identifier} : $new{native_id}";
    return %new;
}

sub _assign_instrument_instances {
  my $s    = shift;
  my $gcis = shift;
  my $meta = shift;
  my $dry_run = shift;
  my @sources;
  $meta->find('podaac\:datasetSource')->each( sub {
      my $e = shift;
      push @sources, {
            platform   => $e->sourceShortName->text,
            instrument => $e->sensorShortName->text };
    });
  for my $source (@sources) {
      debug "Dataset $gcis->{identifier} : $source->{platform} $source->{instrument}";
      my ($platform_identifier, $instrument_identifier) = map lc, @$source{qw/platform instrument/};
      # TODO: use lexicons
      my $instance = $s->gcis->get("/platform/$platform_identifier/instrument/$instrument_identifier") or do {
          info "Did not find /platform/$platform_identifier/instrument/$instrument_identifier";
          next;
      };

      next if $dry_run;
      info "Assigning instrument and platform";
      $s->gcis->post("/dataset/rel/$gcis->{identifier}" => {
              add_instrument_measurement => {
                  platform_identifier => $platform_identifier,
                  instrument_identifier => $instrument_identifier
              }
          }
      ) or die $s->gcis->error;
  }
}

