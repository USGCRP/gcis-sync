package Gcis::syncer::ornldaac;
use base 'Gcis::syncer';

use Gcis::Client;
use Gcis::syncer::util qw/:log iso_date pretty_id/;
use Smart::Comments;
use Mojo::UserAgent;
use Data::Dumper;
use DateTime;

use v5.14;
our $src = "http://mercury.ornl.gov/oai/provider";
our $records_per_request = 100;
our %params = (
  verb           => 'ListRecords',
  metadataPrefix => 'oai_dif',
  set            => sprintf('dleseodlsearch/./null/%d/%d','0',$records_per_request),
  #                          dleseodlsearch/[query]/[set]/[offset]/[length]
);

my $ua  = Mojo::UserAgent->new();

our $data_archive = '/organization/oak-ridge-national-laboratory-distributed-active-archive-center';

our $map = {
    identifier  =>  sub { my $dom = shift;
                          my $id = $dom->at('header identifier')->text; 
                          $id =~ s[oai:mercury\.ornl\.gov:][];
                          $id =~ s/_/-/;
                          die "bad id : $id" unless $id =~ m[^ornldaac-\d+$];
                          return "nasa-$id";
                        },
    native_id   =>  sub { shift->at('Entry_ID')->text; }, 
    name        =>  sub { shift->at('Entry_Title')->text;},
    description =>  sub { shift->at('Summary')->text;  },
    description_attribution => sub {
                           shift
                           ->find('Related_URL')
                           ->grep( sub { $_->at('Type') && $_->at('Type')->text =~ /view related information/i; })
                           ->map(at => 'URL')
                           ->map('text')->join(" ")->to_string;
                          },
    url => sub {
                           shift
                           ->find('Related_URL')
                           ->grep( sub { $_->at('Type') && $_->at('Type')->text =~ /get data/i; })
                           ->map(at => 'URL')
                           ->map('text')->join(" ")->to_string;
                          },
    doi         =>  sub { shift->find('Data_Set_Citation Other_Citation_Details')
                          ->map('text')
                          ->map(sub { s/doi://r })
                          ->join
                          ->to_string },
    lat_min      => sub { shift->at('Southernmost_Latitude')->text },
    lat_max      => sub { shift->at('Northernmost_Latitude')->text },
    lon_min      => sub { shift->at('Westernmost_Longitude')->text },
    lon_max      => sub { shift->at('Easternmost_Longitude')->text },
    start_time   => sub { shift->at('Temporal_Coverage Start_Date')->text },
    end_time     => sub { shift->at('Temporal_Coverage Stop_Date')->text },
    release_dt   => sub { iso_date(shift->at('Dataset_Release_Date')->text) },
    access_dt    => sub { DateTime->now->iso8601 },
};

sub sync {
    my $s = shift;
    my %a = @_;
    my $limit   = $a{limit};
    my $dry_run = $a{dry_run};
    my $gcid_regex = $a{gcid};
    my $c       = $s->{gcis} or die "no client";
    my %stats;
    debug "starting ornldaac";

    my $per_page    = 10;
    my $more        = 1;
    my $url         = Mojo::URL->new($src)->query(%params);
    my $count       = 0;

    while ($count < $limit) {
        ### percent done : sprintf('%02d',100 * $count/$limit)
        $more = 0;
        info "getting $url";
        my $tx = $ua->get($url->query([ %params,
                set => sprintf('dleseodlsearch/./null/%d/%d',$count,$records_per_request),
            ]));
        my $res = $tx->success or die "$url : ".$tx->error->{message};
        if (my $error = $res->dom->at('error')) {
            info "ornldaac error : ".$error->text;
            return;
        }
        #debug "got ".$res->to_string;
        for my $entry ($res->dom->find('record')->each) {
            last if $limit && ++$count > $limit;
            $more = 1;
            my %gcis_info = $s->_extract_gcis($entry);
            debug Dumper(\%gcis_info);

            my $oai_identifier = $entry->at('header identifier')->text;
            my $dataset_gcid = $s->lookup_or_create_gcid(
                  lexicon   => 'ornl',
                  context => 'dataset',
            # e.g. oai:mercury.ornl.gov:ornldaac_831
                  term    => $oai_identifier,
                  gcid    => "/dataset/$gcis_info{identifier}",
                  dry_run => $dry_run,
                  restrict => $gcid_regex,
            );
            die "bad gcid $dataset_gcid" if $dataset_gcid =~ / /;
            next if $gcid_regex && $dataset_gcid !~ /$gcid_regex/;


            debug "entry #$count : $dataset_gcid";

            # insert or update
            my $existing = $c->get($dataset_gcid);
            my $url = $dataset_gcid;
            $url = "/dataset" unless $existing;
            $stats{ ($existing ? "updated" : "created") }++;
            debug "sending to $url";
            unless ($dry_run) {
                $c->post($url => \%gcis_info) or do {
                    error "Error posting to $url : ".$c->error;
                    error "Gcis info : ".Dumper(\%gcis_info);
                };
                $s->_assign_contributors($dataset_gcid, \%gcis_info, $dry_run );
            }
            $s->_assign_instrument_instances(\%gcis_info, $entry, $dry_run );
        }
        last unless $more;
    }

    $s->{stats} = \%stats;
    return;
}
sub _extract_gcis {
    my $s = shift;
    my $dom = shift;
    our $map;

    my %new = map { $_ => $map->{$_}->( $dom ) } keys %$map;
    # debug "extracting $new{identifier} : $new{native_id}";
    return %new;
}

sub _assign_contributors {
    my $s = shift;
    my ($gcid, $info, $dry_run ) = @_;
    return if $dry_run;
    my $contribs_url = $gcid =~ s[dataset/][dataset/contributors/]r;
    $s->gcis->post($contribs_url => {
                organization_identifier => $data_archive,
                role => 'data_archive'
        }) or error $s->gcis->error;
}

sub _assign_instrument_instances {
    my $s = shift;
    state %source_seen;
    state %sensor_seen;

    my ($gcis_info, $dom, $dry_run) = @_;

    # Sample :
    #    <Sensor_Name>
    #        <Short_Name />
    #        <Long_Name>STILLING WELL </Long_Name>
    #    </Sensor_Name>
    #    <Source_Name>
    #        <Short_Name />
    #        <Long_Name>SURFACE WATER WEIR </Long_Name>
    #    </Source_Name>

    info $s->gcis->url."/dataset/$gcis_info->{identifier}";

    my @sensors = $dom->find('Sensor_Name Long_Name')->map('text')->each;
    my @sources = $dom->find('Source_Name Long_Name')->map('text')->each;
    unless (@sensors==@sources) {
        error "count mismatch for sensors and sources in $gcis_info->{identifier} : ".@sensors." vs ".@sources;
        return;
    }
    my @instances;
    my %seen;
    while (@sensors && (my ($i,$p) = (shift @sensors, shift @sources))) {
        next if $seen{$i}{$p}++;
        info "new source : $p" unless $source_seen{$p}++;
        info "new sensor : $i" unless $sensor_seen{$i}++;
        debug qq[{ source : "$p", sensor : "$i" }];
        push @instances, { sensor => $i, source => $p};
    }
    for my $instance (@instances) {
        my ($platform_gcid, $instrument_gcid);
        if (my $found = $s->gcis->get("/lexicon/ornl/find/Source/$instance->{source}")) {
            $platform_gcid = $found->{uri};
        } else {
            debug "no platform id for source : $instance->{source}";
        }
        if (my $found = $s->gcis->get("/lexicon/ornl/find/Sensor/$instance->{sensor}")) {
            $instrument_gcid = $found->{uri};
        } else {
            if ($platform_gcid) {
                info "no instrument id for sensor : $instance->{sensor} on ".$s->gcis->url.$platform_gcid;
            } else {
                debug "no instrument id for sensor : $instance->{sensor}";
            }
        }
        next unless $platform_gcid && $instrument_gcid;
        info "found sensor/source : $instance->{sensor}/$instance->{source}";
        my $instance_gcid = join '', $platform_gcid, $instrument_gcid;
        my $instance = $s->gcis->get($instance_gcid) or do {
            info "did not find instance $instance_gcid";
            next;
        };
        next if $dry_run;
        $s->gcis->post("/dataset/rel/$gcis_info->{identifier}" => {
                add_instrument_measurement => {
                    platform_identifier => $platform_gcid =~ s[/platform/][]r,
                    instrument_identifier => $instrument_gcid =~ s[/instrument/][]r,
                }
            } );

    }
}

