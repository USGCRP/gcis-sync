package Gcis::syncer::ceos;
use base 'Gcis::syncer';

use Smart::Comments;
use Mojo::UserAgent;
use Gcis::syncer::util qw/:log pretty_id/;
use Data::Dumper;
use List::MoreUtils qw/mesh/;
use Date::Parse qw/str2time/;
use DateTime;
use v5.14;

our $base_src = "http://database.eohandbook.com";
our $platform_src   = qq[$base_src/database/missiontable.aspx];
our $instrument_src = qq[$base_src/database/instrumenttable.aspx];
my $ua  = Mojo::UserAgent->new()->max_redirects(3);

our %defaultParams = (
  ddlAgency               => "All",
  tbMission               => "",
  ddlLaunchYearFiltertype => "All",
  tbInstruments           => "",
  ddlEOLYearFilterType    => "All",
  tbApplications          => "",
  ddlDisplayResults       => "10",
  ddlRepeatCycleFilter    => "All",
  btExportToExcel         => "Export+to+Excel",
  '__VIEWSTATE'           => undef, # set below
  '__EVENTVALIDATION'     => undef, # set below
  '__EVENTTARGET'         => "",
  '__EVENTARGUMENT'       => "",
  '__LASTFOCUS'           => "",
  '__VIEWSTATEENCRYPTED' => "",

);

sub _get_missions {
    my $s = shift;
    my @missions;

    debug "GET $platform_src";
    my $tx1 = $ua->get($platform_src);
    my $res1 = $tx1->res or die "fail: ".$tx1->error->{message};
    my %params = %defaultParams;
    $params{ddlMissionStatus} = "All";
    $params{__VIEWSTATE} = $res1->dom->find('#__VIEWSTATE')->attr('value');
    $params{__EVENTVALIDATION} = $res1->dom->find('#__EVENTVALIDATION')->attr('value');
    debug "POST to $platform_src";
    my $tx = $ua->post($platform_src => form => \%params);
    my $res = $tx->res or die "error posting to $platform_src: ".$tx->error->{message};
    die "failed to get xls file : ".$res->body unless $res->headers->content_type eq 'application/vnd.xls';

    my @header = map pretty_id($_->text), $res->dom->find('table > tr > th')->each;
    for my $row ($res->dom->find('table > tr')->each) {
        my @cells = map $_->text, $row->find('td')->each;
        next unless @cells;
        my %record = mesh @header, @cells;
        push @missions, \%record;
    }
    return @missions;
}

sub _get_instruments {
    my $s = shift;
    my @instruments;

    debug "GET $instrument_src";
    my $tx1 = $ua->get($instrument_src);
    my $res1 = $tx1->res or die "fail: ".$tx1->error->{message};
    my %params = %defaultParams;
    $params{'ddlMissionStatus' } = 'All';
    $params{'ddlInstrumentStatus' } = 'All';
    $params{__VIEWSTATE} = $res1->dom->find('#__VIEWSTATE')->attr('value');
    $params{__EVENTVALIDATION} = $res1->dom->find('#__EVENTVALIDATION')->attr('value');
    debug "POST to $instrument_src";
    my $tx = $ua->post($instrument_src => form => \%params);
    my $res = $tx->res or die "error posting to $instrument_src: ".$tx->error->{message};
    die "failed to get xls file : ".$res->body unless $res->headers->content_type eq 'application/vnd.xls';

    my @header = map pretty_id($_->text), $res->dom->find('table > tr > th')->each;
    for my $row ($res->dom->find('table > tr')->each) {
        my @cells = map $_->text, $row->find('td')->each;
        next unless @cells;
        my %record = mesh @header, @cells;
        push @instruments, \%record;
    }
    return @instruments;
}

sub sync {
    my $s = shift;
    my %a       = @_;
    my $limit   = $a{limit};
    my $dry_run = $a{dry_run};
    my $gcid    = $a{gcid};
    my $c       = $s->{gcis} or die "no client";

    # Missions (platforms)
    my @missions = $s->_get_missions;
    my @map;

    for my $ceos_record (@missions) {  ### Adding missions... [%]   done
        next unless $ceos_record->{'mission-status'} =~ /^(Mission complete|currently being flown)/i;
        my $platform = $s->_add_platform($ceos_record, $dry_run, $gcid) or next;
        info "platform $platform";
        # debug Dumper($ceos_record);
        push @map, {
            platform => $platform,
            ceos_instrument_ids => [ split /,\s*/, $ceos_record->{'instrument-ids'} ],
            ceos_agencies => [ split /,\s*/, $ceos_record->{'mission-agencies'}]
        }
    }

    # Instruments
    my @instruments = $s->_get_instruments;
    for my $ceos_record (@instruments) {   ### Adding instruments... [%]  done
        my $instrument = $s->_add_instrument($ceos_record, $dry_run, $gcid) or next;
        info "instrument $instrument";
    }

    # Join
    for my $entry (@map) {  ## Associating platforms, instruments, agencies... [%]   done
        my $instruments = $s->_associate_instruments($entry->{platform}, $entry->{ceos_instrument_ids}, $dry_run);
        my $agencies = $s->_associate_agencies($entry->{platform}, $entry->{ceos_agencies}, $dry_run);
    }
}

sub _add_instrument {
    my $s = shift;
    state %seen;
    my $ceos = shift;
    my $dry_run = shift;
    my $gcid_regex;
    return if $ceos->{'instrument-status'} =~ /(proposed|being developed)/i;
    # debug "ceos data : ".Dumper($ceos);

    my $name = $ceos->{'instrument-name-full'};
    $name = $ceos->{'instrument-name-short'} unless length($name) > 2 && $name =~ /\S/;

    # If the numeric ID is not there, make a unique GCID.
    my $gcid;
    if (my $existing = $s->lookup_gcid(ceos => instrumentID => $ceos->{'instrument-id'})) {
        $gcid = $existing;
    } else {
        my $candidate = pretty_id($name);
        my $base = $candidate;
        my $i = 2;
        while ($s->gcis->get("/instrument/$candidate")) {
            debug "/instrument/$candidate exists, trying another id";
            $candidate = "$base-$i";
            $i++;
        }
        $gcid = $s->lookup_or_create_gcid(
            lexicon => "ceos",
            context => "instrumentID",
            term    => $ceos->{'instrument-id'},
            gcid    => "/instrument/$candidate",
            dry_run => $dry_run,
        );
    }

    my $alt = $s->lookup_or_create_gcid(
        lexicon => "ceos",
        context => "Instrument",
        term    => $ceos->{'instrument-name-short'},
        gcid    => $gcid,
        dry_run => $dry_run,
    );
    return if $gcid_regex && $gcid !~ m[$gcid_regex];
    # Two shortnames may refer to one numeric identifier
    debug "multiple matches for ".$ceos->{'instrument-name-short'}." : $gcid, $alt" unless $alt eq $gcid;

    my ($id) = $gcid =~ m[/instrument/(.*)];
    if ($seen{$id}++) {
        info "Skipping duplicate instrument id $id" ;
    }

    my $url = "/instrument";
    if (my $existing = $s->gcis->get("/instrument/$id")) {
        $url = $existing->{uri};
        debug "exists : $url";
    }
    return $id if $dry_run;

    my %instrument = (
      identifier => $id,
      name       => $ceos->{'instrument-name-full'},
      description => $ceos->{'instrument-technology'}, 
      audit_note => $s->audit_note,
    );
    $s->gcis->post($url => \%instrument) or do {
        warning "Error posting to $url : ".$s->gcis->error;
        return $instrument{identifier};
    };
    return $instrument{identifier};
}

sub _add_platform {
    my $s = shift;
    state %seen;
    my $ceos = shift;
    my $dry_run = shift;
    my $gcid_regex = shift;
    #debug "ceos data : ".Dumper($ceos);
    #return if $ceos->{'mission-status'} =~ /N\/A/;

    my $name = $ceos->{'mission-name-full'};
    $name = $ceos->{'mission-name-short'} unless $name =~ /\S/;

    my $gcid = $s->lookup_or_create_gcid(
        lexicon => "ceos",
        context => "missionID",
        term => $ceos->{'mission-id'},
        gcid => "/platform/".pretty_id($name),
        dry_run => $dry_run,
    );
    my $alt = $s->lookup_or_create_gcid(
        lexicon => "ceos",
        gcid => $gcid,
        context => "Mission",
        term => $ceos->{'mission-name-short'},
        dry_run => $dry_run,
    );
    die "id mismatch ($gcid != $alt)" unless $gcid eq $alt;
    return if $gcid_regex && $gcid !~ m[$gcid_regex];

    my ($id) = $gcid =~ m[/platform/(.*)];
    if ($seen{$id}++) {
        info "skipping duplicate platform id $id";
        return;
    }

    my $url = "/platform";
    if (my $existing = $s->gcis->get("/platform/$id")) {
        $url = $existing->{uri};
        debug "exists : $url";
    }
    return $id if $dry_run;

    my %platform = (
      identifier => $id,
      name       => $name,
      url        => $ceos->{'mission-site'},
      audit_note => $s->audit_note,
      platform_type_identifier => 'spacecraft',
      start_date => _ceos_date($ceos->{'launch-date'}),
      end_date => _ceos_date($ceos->{'eol-date'}),
    );
    $s->gcis->post($url => \%platform) or do {
        warning "Error posting to $url : ".$s->gcis->error;
        return $platform{identifier};
    };
    return $platform{identifier};
}

sub _ceos_date {
    my $dt = shift or return undef;
    # Use Jan and 01 for approximate dates.
    $dt =~ /^\d{4}$/ and $dt="Jan $dt";
    $dt =~ /^(\w{3}) (\d{4})/ and $dt = "01 $1 $2";
    return DateTime->from_epoch(epoch => (str2time($dt)))->ymd;
}

sub audit_note {
    return join "\n",shift->SUPER::audit_note, $base_src;
}

sub _associate_instruments {
    my $s = shift;
    my $platform = shift;
    my $ceos_instrument_ids = shift;
    my $dry_run = shift;
    my @instruments;
    for my $ceos_instrument_id (@$ceos_instrument_ids) {
        my $gcid = $s->lookup_gcid( "ceos", "instrumentID", $ceos_instrument_id) or do {
            error "Could not find instrument id '$ceos_instrument_id' for $platform";
            next;
        };
        my ($identifier) = $gcid =~ m[/instrument/(.*)$];
        push @instruments, $identifier;
    }
    return \@instruments if $dry_run;
    for my $instrument (@instruments) {
        unless ($s->gcis->get("/instrument/$instrument")) {
            error "Instrument $instrument not found (platform $platform)";
            return;
        }

        $s->gcis->post("/platform/rel/$platform",
          {add => {instrument_identifier => $instrument}});
    }
    return \@instruments;
}

sub _associate_agencies {
    my $s = shift;
    my $platform = shift;
    my $ceos_agencies = shift;
    my $dry_run = shift;
    my @organizations = map {
            my $id = $s->lookup_gcid( "ceos", "Agency", $_) or error "missing agency id for $_";
            $id || ();
        } @$ceos_agencies;
    for my $org (@organizations) {
        debug "adding agency $org";
        my $contribs_url = "/platform/contributors/$platform";
        next if $dry_run;
        $s->gcis->post($contribs_url => {
                organization_identifier => $org,
                role => 'contributor'
        }) or error $s->gcis->error;
    }
    return \@organizations;
}

1;
