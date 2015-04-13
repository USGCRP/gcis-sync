package Gcis::syncer::podaac;
use base 'Gcis::syncer';

use Gcis::Client;
use Gcis::syncer::util qw/:log iso_date pretty_id/;
use Smart::Comments -ENV;
use Mojo::UserAgent;
use Data::Dumper;
use DateTime;
use Path::Class qw/file/;

use v5.14;
our $src = "http://podaac.jpl.nasa.gov/ws/search/dataset/";
our $meta_src = Mojo::URL->new("http://podaac.jpl.nasa.gov/ws/search/dataset/")
                ->query(format=>'atom', full => 'true', pretty => "true");
                # shortName => MERGED_TP_J1_OSTM_OST_ALL_V2, pretty => true,
my $ua  = Mojo::UserAgent->new();

our $data_archive = '/organization/physical-oceanography-distributed-active-archive-center';

our $map = {
    identifier  =>  sub { my $dom = shift; my $id = lc $dom->at('shortName')->text; 
                          $id =~ tr[ .][-];
                          return "nasa-podaac-$id" unless $id =~ /^podaac/;
                          return "nasa-$id";
                        },
    name        =>  sub { my $dom = shift; $dom->at('title')->text;         },
    description =>  sub { my $dom = shift; join "\n", $dom->at('content')->text;  },
    description_attribution => sub { shift->at('link[title="Dataset Information"]')->attr('href');  },
    native_id   =>  sub { my $dom = shift; $dom->at('shortName')->text;     },
    url         =>  sub { my $dom = shift; $dom->at('link[title="Dataset Information"]')->attr('href'); },
    lon_min     =>  sub { my $dom = shift; my $w = $dom->at('where') or return undef;
                                           [ split / /, $w->at('Envelope lowerCorner')->text ]->[0]; },
    lat_min     =>  sub { my $dom = shift; my $w = $dom->at('where') or return undef;
                                           [ split / /, $w->at('Envelope lowerCorner')->text ]->[1]; },
    lon_max     =>  sub { my $dom = shift; my $w = $dom->at('where') or return undef;
                                           [ split / /, $w->at('Envelope upperCorner')->text ]->[0]; },
    lat_max     =>  sub { my $dom = shift; my $w = $dom->at('where') or return undef;
                                           [ split / /, $w->at('Envelope upperCorner')->text ]->[1]; },
    start_time  =>  sub { my $start = shift->at('start') or return undef;  return iso_date($start->text) },
    end_time    =>  sub { my $end   = shift->at('end')   or return undef;  return iso_date($end->text)   },
    release_dt  =>  sub { iso_date(shift->at('updated')->text);                                          },
};

sub sync {
    my $s = shift;
    my %a = @_;
    my $limit   = $a{limit};
    my $dry_run = $a{dry_run};
    my $gcid_regex = $a{gcid};
    my $c       = $s->{gcis} or die "no client";
    my $from_file = $a{from_file};
    my %stats;

    my $per_page    = 400;
    my $more        = 1;
    my $start_index = 1;
    my $url         = Mojo::URL->new($src)->query(format => "atom", itemsPerPage => $per_page);
    my $count       = 1;

    REQUEST :
    while ($more) {
        $more = 0;
        my $dom;
        if ($from_file) {
            $dom = Mojo::DOM->new(scalar file($from_file)->slurp);
        } else {
            my $tx = $ua->get($url->query([ startIndex => $start_index ]));
            my $res = $tx->success or die "$url : ".$tx->error->{message};
            $dom = $res->dom;
        }

        for my $entry ($dom->find('entry')->each) {  ### Processing===[%]       done
            last REQUEST if $limit && $count > $limit;
            $more = 1 unless $from_file;
            my %gcis_info = $s->_extract_gcis($entry);

            # Store mappings to both shortName and id
            my $dataset_gcid = $s->lookup_or_create_gcid(
                  lexicon   => 'podaac',
                  context => 'dataset',
                  term    => $gcis_info{native_id},
                  gcid    => "/dataset/$gcis_info{identifier}",
                  dry_run => $dry_run,
                  restrict => $gcid_regex,
            );
            die "bad gcid $dataset_gcid" if $dataset_gcid =~ / /;
            next if $gcid_regex && $dataset_gcid !~ /$gcid_regex/;
            my $alternate_id = $entry->at("id")->text;
            $s->lookup_or_create_gcid(
                lexicon => 'podaac', context => 'datasetId', term => $alternate_id,
                gcid => $dataset_gcid, dry_run => $dry_run,
            );

            debug "entry #$count : $dataset_gcid";
            $count++;

            # insert or update
            my $existing = $c->get($dataset_gcid);
            my $url = $dataset_gcid;
            $url = "/dataset" unless $existing;
            $stats{ ($existing ? "updated" : "created") }++;
            # TODO skip if unchanged
            debug "sending to $url";
            #debug Dumper(\%gcis_info);
            unless ($dry_run) {
                $c->post($url => \%gcis_info) or do {
                    error "Error posting to $url : ".$c->error;
                    error "Gcis info : ".Dumper(\%gcis_info);
                };
            }
            my $meta = $s->_retrieve_dataset_meta($gcis_info{native_id}) or next;
            $s->_assign_instrument_instances(\%gcis_info, $meta,$dry_run);
            $s->_assign_files($dataset_gcid, $meta, $dry_run );
            $s->_assign_contributors($dataset_gcid, $meta, $dry_run );
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
        error "Error GETting from $url : ".$tx->error->{message};
        return;
    };
    return $res->dom;
}

sub _extract_gcis {
    my $s = shift;
    my $dom = shift;
    our $map;

    my %new = map { $_ => $map->{$_}->( $dom ) } keys %$map;
    # debug "extracting $new{identifier} : $new{native_id}";
    return %new;
}

sub _assign_instrument_instances {
  my $s    = shift;
  my $gcis = shift;
  my $meta = shift;
  my $dry_run = shift;
  my @sources;
  state %missing_platform;
  state %missing_instrument;

  $meta->find('podaac\:datasetSource')->each( sub {
      my $e = shift;
      push @sources, {
            platform   => $e->at('sourceShortName')->text,
            platform_long   => $e->at('sourceLongName')->text,
            platform_desc => $e->at('sourceDescription')->text,
            instrument => $e->at('sensorShortName')->text,
            instrument_long => $e->at('sensorLongName')->text,
            instrument_desc => $e->at('sensorDescription')->text,
        };
    });
  for my $source (@sources) {
      debug "Dataset $gcis->{identifier} : $source->{platform} $source->{instrument}";

      my ($platform_gcid, $instrument_gcid);
      if (my $found = $s->gcis->get("/lexicon/podaac/find/Platform/$source->{platform}")) {
          debug "Lookup succeeded, got ".$found->{uri};
          $platform_gcid = $found->{uri};
      } else {
          debug "tried to get /lexicon/podaac/find/Platform/$source->{platform}";
          debug "could not find platform $source->{platform} ";
          unless ($missing_platform{$source->{platform}}++) {
              error "Missing Platform '$source->{platform}' : $source->{platform_long} -- $source->{platform_desc}";
          }
      }
      if (my $found = $s->gcis->get("/lexicon/podaac/find/Sensor/$source->{instrument}")) {
          debug "Lookup succeeded, got ".$found->{uri};
          $instrument_gcid = $found->{uri};
      } else {
          debug "could not find instrument $source->{instrument} (on platform $source->{platform})";
          unless ($missing_instrument{$source->{instrument}}++) {
              error "Missing Sensor on $source->{platform} : '$source->{instrument}' : $source->{instrument_long} -- $source->{instrument_desc}";
          }
      }
      next unless $platform_gcid && $instrument_gcid;

      my $instrument_instance = $platform_gcid . $instrument_gcid;

      my $instance = $s->gcis->get($instrument_instance) or do {
          info "Did not find $instrument_instance";
          next;
      };

      next if $dry_run;
      debug "Assigning instrument and platform";
      my ($platform_identifier) = $platform_gcid =~ m[/platform/(.*)$];
      my ($instrument_identifier) = $instrument_gcid =~ m[/instrument/(.*)$];
      $s->gcis->post("/dataset/rel/$gcis->{identifier}" => {
              add_instrument_measurement => {
                  platform_identifier => $platform_identifier,
                  instrument_identifier => $instrument_identifier
              }
          }
      ) or die $s->gcis->error;
  }
}

sub _assign_files {
    my $s = shift;
    my ($gcid, $meta, $dry_run ) = @_;
    my $thumb = $meta->children('feed entry link')->grep(sub { $_[0]->attr('title') =~ /thumbnail/i; });
    return unless $thumb->first;
    my $file_url = $thumb->first->attr('href') or return;
    my $info = $meta->children('feed entry link')->grep(sub { $_[0]->attr('title') =~ /dataset information/i; });
    return unless $info->first;
    my $info_url = $info->first->attr('href');
    debug "adding file $file_url";
    $s->gcis->add_file_url($gcid => {
        file_url       => $file_url,
        landing_page   => $info_url,
    }) or return error $s->gcis->error;
    1;
}

sub _assign_contributors {
    my $s = shift;
    my ($gcid, $meta, $dry_run ) = @_;
    return if $dry_run;
    my $contribs_url = $gcid;
    $contribs_url =~ s[dataset/][dataset/contributors/];
    $s->gcis->post($contribs_url => {
                organization_identifier => $data_archive,
                role => 'data_archive'
        }) or error $s->gcis->error;
}
