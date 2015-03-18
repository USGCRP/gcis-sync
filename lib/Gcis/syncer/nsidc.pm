package Gcis::syncer::nsidc;
use base 'Gcis::syncer';

use Gcis::Client;
use Gcis::syncer::util qw/:log iso_date pretty_id/;
use Smart::Comments -ENV;
use Mojo::UserAgent;
use Data::Dumper;
use DateTime;
use Path::Class qw/file/;

use v5.14;
our $src = "http://nsidc.org/api/dataset/2/oai?verb=ListRecords&metadataPrefix=dif";

my $ua  = Mojo::UserAgent->new()->inactivity_timeout(60 * 20);

our $data_archive = '/organization/national-snow-ice-data-center';

sub _txt($) {
    my $selector = shift;
    return sub {
        my $v = shift->at($selector) or return undef;
        return $v->text;
    }
}

our $map = {
    identifier  =>  sub { my $dom = shift;
                          my $id = lc $dom->at('Entry_ID')->text;
                          $id = "nsidc-$id" unless $id =~ /^nsidc/;
                          $id = "nasa-$id";
                          return $id;
                        },
    native_id   =>  sub { shift->at('Entry_ID')->text; }, 
    name        =>  sub { shift->at('Entry_Title')->text;},
    description =>  sub { shift->at('Summary > Abstract')->text;  },
    description_attribution => sub {
                           shift
                           ->find('Related_URL')
                           ->grep( sub { $_->at('Type') && $_->at('Type')->text =~ /view related information/i; })
                           ->map(at => 'URL')
                           ->map('text')->join(" ")->to_string;
                          },
    url          => _txt 'Data_Set_Citation > Online_Resource',
    doi          => _txt 'Data_Set_Citation > Dataset_DOI',
    lat_min      => _txt 'Southernmost_Latitude',
    lat_max      => _txt 'Northernmost_Latitude',
    lon_min      => _txt 'Westernmost_Longitude',
    lon_max      => _txt 'Easternmost_Longitude',
    start_time   => _txt 'Temporal_Coverage Start_Date',
    end_time     => _txt 'Temporal_Coverage Stop_Date',
    release_dt   => sub { iso_date(shift->at('Dataset_Release_Date')->text) },
};

sub sync {
    my $s = shift;
    my %a = @_;
    my $limit   = $a{limit} || 999999;
    my $dry_run = $a{dry_run};
    my $gcid_regex = $a{gcid};
    my $from_file = $a{from_file};
    my $c       = $s->{gcis} or die "no client";
    my %stats;
    my $count       = 0;
    my $dom;

    debug "starting nsidc";

    if ($from_file) {
        $dom = Mojo::DOM->new(scalar file($from_file)->slurp);
    } else {
        info "getting $src";
        my $tx = $ua->get($src);
        my $res = $tx->success or die "$src : ".$tx->error->{message};
        if (my $error = $res->dom->at('error')) {
            info "nsidc error : ".$error->text;
            return;
        }
        $dom = $res->dom;
    }

    for my $entry ($dom->find('record')->each) {
        last if $limit && $count >= $limit;
        my %gcis_info = $s->_extract_gcis($entry);
        next unless $gcis_info{doi};
        ++$count;
        # debug Dumper(\%gcis_info);

        my $oai_identifier = $entry->at('header identifier')->text;
        my $dataset_gcid = $s->lookup_or_create_gcid(
              lexicon   => 'nsidc',
              context => 'dataset',
              term    => $oai_identifier,
              gcid    => "/dataset/$gcis_info{identifier}",
              dry_run => $dry_run,
              restrict => $gcid_regex,
        );
        my $identifier = $gcis_info{identifier} or die "no identifier : ".Dumper(\%gcis_info);
        my $dataset_gcid = "/dataset/$gcis_info{identifier}";
        next if $gcid_regex && $dataset_gcid !~ /$gcid_regex/;
        debug "$dataset_gcid";

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

    # NSIDC provides multiple sources and sensors and no indication of which goes with which, so
    # we try all possible combinations to search for existing instrument instances.
    #
    #<Source_Name>
    #   <Short_Name>DMSP 5D-2/F11</Short_Name>
    #   <Long_Name>Defense Meteorological Satellite Program-F11</Long_Name>
    #</Source_Name>
    #<Sensor_Name>
    #   <Short_Name>SSMIS</Short_Name>
    #   <Long_Name>Special Sensor Microwave Imager/Sounder</Long_Name>
    #</Sensor_Name>

    info $s->gcis->url."/dataset/$gcis_info->{identifier}";

    my @sensors = $dom->find('Sensor_Name Short_Name')->map('text')->each;
    my @sources = $dom->find('Source_Name Short_Name')->map('text')->each;
    my @long_sensors = $dom->find('Sensor_Name Long_Name')->map('text')->each;
    my @long_sources = $dom->find('Source_Name Long_Name')->map('text')->each;

    return unless @sources && @sensors;

    debug "sources : ".(join ", ",@sources).( @long_sources ? "(".(join", ", @long_sources).")" : "");
    debug "sensors : ".(join ", ",@sensors).( @long_sensors ? "(".(join", ", @long_sensors).")" : "");

    # Look for any existing combinations, no way to tell what goes with what.

    my @instances;
    for my $i (@sensors) {
        my $instrument = $s->gcis->get("/lexicon/nsidc/find/Sensor/$i") or do {
            info "instrument not found : $i";
            next;
        };
        for my $p (@sources) {
            my $platform = $s->gcis->get("/lexicon/nsidc/find/Source/$p") or do {
                info "platform not found : $p";
                next;
            };
            my $instance = "/platform/$platform->{identifier}/instrument/$instrument->{identifier}";
            $s->gcis->get($instance) or do {
                info "did not find instance $instance";
                next;
            };
            next if $dry_run;
            $s->gcis->post("/dataset/rel/$gcis_info->{identifier}" => {
                    add_instrument_measurement => {
                        platform_identifier => $platform->{identifier} =~ s[/platform/][]r,
                        instrument_identifier => $instrument->{identifier}=~ s[/instrument/][]r,
                    }
                } );
        }
    }
}

