package Gcis::syncer::ceos;
use base 'Gcis::syncer';

use Smart::Comments;
use Mojo::UserAgent;
use Gcis::syncer::util qw/:log pretty_id/;
use Data::Dumper;
use List::MoreUtils qw/mesh/;
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
    my $all_missions = shift;
    my @instruments;

    debug "GET $instrument_src";
    my $tx1 = $ua->get($instrument_src);
    my $res1 = $tx1->res or die "fail: ".$tx1->error->{message};
    my %params = %defaultParams;
    if ($all_missions) {
        $params{'ddlMissionStatus' } = 'All';
    } else {
        $params{'ddlInstrumentStatus' } = 'All';
    }
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
        my $platform = $s->_add_platform($ceos_record, $dry_run) or next;
        info "platform $platform";
        push @map, {
            platform => $platform,
            ceos_instruments => $ceos_record->{'instruments'}
        }
    }

    # Instruments
    my @instruments = $s->_get_instruments;
    my %by_id = map { $_->{'instrument-name-short'} => 1 } @instruments;
    my @more = $s->_get_instruments(all_missions => 1);
    for (@more) {
        next if $by_id{$_->{'instrument-name-short'}};
        push @instruments, $_;
    }
    for my $ceos_record (@instruments) {   ### Adding instruments... [%]  done
        my $instrument = $s->_add_instrument($ceos_record, $dry_run) or next;
        info "instrument $instrument";
    }

    # Join
    for my $entry (@map) {  ## Associating platforms and instruments... [%]   done
        my $instruments = $s->_associate_instruments($entry->{platform}, $entry->{ceos_instruments}, $dry_run);
    }
}

sub _add_instrument {
    my $s = shift;
    state %seen;
    my $ceos = shift;
    my $dry_run = shift;
    #return if $ceos->{'instrument-status'} =~ /(proposed|being developed)/i;
    #debug "ceos data : ".Dumper($ceos);

    my $gcid = $s->lookup_or_create_gcid(
        lexicon => "ceos",
        context => "instrument",
        term    => $ceos->{'instrument-name-short'},
        gcid    => "/instrument/" . pretty_id($ceos->{'instrument-name-short'}),
        dry_run => $dry_run,
    );
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
    #debug "ceos data : ".Dumper($ceos);
    #return if $ceos->{'mission-status'} =~ /N\/A/;

    my $gcid = $s->lookup_or_create_gcid(
        lexicon => "ceos",
        context => "mission",
        term => $ceos->{'mission-name-short'},
        gcid => "/platform/".pretty_id($ceos->{'mission-name-short'}),
        dry_run => $dry_run,
    );
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
      name       => $ceos->{'mission-name-full'},
      url        => $ceos->{'mission-site'},
      audit_note => $s->audit_note,
    );
    $s->gcis->post($url => \%platform) or do {
        warning "Error posting to $url : ".$s->gcis->error;
        return $platform{identifier};
    };
    return $platform{identifier};
}


sub audit_note {
    return join "\n",shift->SUPER::audit_note, $base_src;
}

sub _associate_instruments {
    my $s = shift;
    my $platform = shift;
    my $ceos_instruments = shift;
    my $dry_run = shift;
    my @instruments;
    for my $ceos_instrument_id (split /\s*,\s*/, $ceos_instruments) {
        my $gcid = $s->lookup_or_create_gcid(
            lexicon => "ceos",
            context => "instrument",
            term    => $ceos_instrument_id,
            gcid    => "/instrument/" . pretty_id($ceos_instrument_id),
            dry_run => $dry_run,
        );
        my ($identifier) = $gcid =~ m[/instrument/(.*)$];
        push @instruments, $identifier;
    }
    return \@instruments if $dry_run;
    for my $instrument (@instruments) {
        unless ($s->gcis->get("/instrument/$instrument")) {
            error "Instrument $instrument not found (platform $platform, Instruments : $ceos_instruments)";
            return;
        }

        $s->gcis->post("/platform/rel/$platform",
          {add => {instrument_identifier => $instrument}});
    }
    return \@instruments;
}


1;
