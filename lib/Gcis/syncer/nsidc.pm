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

# http://nsidc.org/api/dataset/metadata/g02135.dif
# and get the list of IDs from someplace

my $ua  = Mojo::UserAgent->new()->inactivity_timeout(60 * 10);

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
        debug Dumper(\%gcis_info);

        #my $oai_identifier = $entry->at('header identifier')->text;
        #my $dataset_gcid = $s->lookup_or_create_gcid(
        #      lexicon   => 'nsidc',
        #      context => 'dataset',
        #      term    => $oai_identifier,
        #      gcid    => "/dataset/$gcis_info{identifier}",
        #      dry_run => $dry_run,
        #      restrict => $gcid_regex,
        #);
        #die "bad gcid $dataset_gcid" if $dataset_gcid =~ / /;
        my $identifier = $gcis_info{identifier} or die "no identifier : ".Dumper(\%gcis_info);
        my $dataset_gcid = "/dataset/$gcis_info{identifier}";
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
        #$s->_assign_instrument_instances(\%gcis_info, $entry, $dry_run );
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
        if (my $found = $s->gcis->get("/lexicon/nsidc/find/Source/$instance->{source}")) {
            $platform_gcid = $found->{uri};
        } else {
            debug "no platform id for source : $instance->{source}";
        }
        if (my $found = $s->gcis->get("/lexicon/nsidc/find/Sensor/$instance->{sensor}")) {
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

