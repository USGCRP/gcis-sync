package Gcis::syncer::podaac;
use base 'Gcis::syncer';

use Gcis::Client;
use Gcis::syncer::util qw/:log iso_date pretty_id/;
use Smart::Comments;
use Mojo::UserAgent;
use Data::Dumper;

use v5.14;
our $src = "http://podaac.jpl.nasa.gov/ws/search/dataset/";
our $meta_src = "http://podaac.jpl.nasa.gov/ws/metadata/dataset/"; # datasetId => PODAAC-TPRGD-NET10, format => gcmd
my $ua  = Mojo::UserAgent->new();

our $map = {
    identifier  =>  sub { my $dom = shift; "nasa-".(lc $dom->id->text); },
    name        =>  sub { my $dom = shift; $dom->title->text;         },
    description =>  sub { my $dom = shift; join "\n", $dom->at('content')->text, "short name : ".$dom->shortName->text;  },
    native_id   =>  sub { my $dom = shift; $dom->datasetId->text;     },
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

our $rel = [
    [
      platforms => sub {
        my $dataset = shift;
        my $dif = shift;
        for ($dif->find("Source_Name > Short_Name")->each) {
          my $platform_identifier = pretty_id($_->text);
              debug "dataset ".$dataset->{identifier}." has platform $platform_identifier";
        }
      }
    ],
    [
      sensors => sub {
        my $dataset = shift;
        my $dif = shift;
        for ($dif->find("Sensor_Name > Short_Name")->each) {
          my $sensor_identifier = pretty_id($_->text);
          debug "dataset ".$dataset->{identifier}." has sensor $sensor_identifier";
        }
      },
    ],
];

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
        my $res = $tx->success or die $tx->error;
        for my $entry ($res->dom->find('entry')->each) {  ### Processing===[%]       done
            last REQUEST if $limit && $count > $limit;
            $more = 1;
            my %gcis_info = $s->_extract_gcis($entry);
            next if $gcid && $gcid ne "/dataset/$gcis_info{identifier}";
            debug "entry #$count : $gcis_info{identifier}";
            $count++;

            # insert or update
            my $existing = $c->get("/dataset/$gcis_info{identifier}");
            my $url = $existing ? "/dataset/$gcis_info{identifier}" : "/dataset";
            $stats{ ($existing ? "updated" : "created") }++;
            # TODO skip if unchanged
            debug "sending ".Dumper(\%gcis_info);
            unless ($dry_run) {
                $c->post($url => \%gcis_info) or do {
                    error $c->error;
                    die "bailing out, error : ".$c->error;
                };
            }
            my $dif = $s->_retrieve_dataset_meta($gcis_info{native_id});
            for my $rel (@$rel) {
                my ($name,$code) = @$rel;
                debug "updating $name";
                $code->(\%gcis_info, $dif);
            }
        }
        $start_index += $per_page;
    }

    $s->{stats} = \%stats;
    return;
}

sub _retrieve_dataset_meta {
    my $s = shift;
    my $id = shift;
    my $url = Mojo::URL->new($meta_src)->query(datasetId => $id, format => 'gcmd');
    debug "getting $url";
    my $tx = $ua->get($url);
    my $res = $tx->success or die $tx->error;
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


