package Gcis::syncer::ceos;
use base 'Gcis::syncer';

use Smart::Comments;
use Mojo::UserAgent;
use Gcis::syncer::util qw/:log pretty_id/;
use Data::Dumper;
use List::MoreUtils qw/mesh/;
use v5.14;

our $src = q[http://database.eohandbook.com/database/missiontable.aspx];
our %params = (
  ddlAgency               => "All",
  ddlMissionStatus        => "All",
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

my $ua  = Mojo::UserAgent->new()->max_redirects(3);

my %all_orgs;
sub sync {
    my $s = shift;
    my %a       = @_;
    my $limit   = $a{limit};
    my $dry_run = $a{dry_run};
    my $gcid    = $a{gcid};
    my $c       = $s->{gcis} or die "no client";

    # First get hidden fields.
    debug "GETting $src";
    my $tx1 = $ua->get($src);
    my $res1 = $tx1->res or die "fail: ".$tx1->error;
    $params{__VIEWSTATE} = $res1->dom->find('#__VIEWSTATE')->attr('value');
    $params{__EVENTVALIDATION} = $res1->dom->find('#__EVENTVALIDATION')->attr('value');
    
    # Now get the "excel spreadsheet".
    debug "POSTing to $src";
    my $tx = $ua->post($src => form => \%params);
    my $res = $tx->res or die $tx->error;
    die "failed to get xls file : ".$res->body unless $res->headers->content_type eq 'application/vnd.xls';

    # Payload is actually HTML.
    my @header = map pretty_id($_->text), $res->dom->find('table > tr > th')->each;
    for my $row ($res->dom->find('table > tr')->each) {
        my @cells = map $_->text, $row->find('td')->each;
        next unless @cells;
        my %record = mesh @header, @cells;
        my $platform = $s->_add_platform(\%record, $dry_run);
        my $agencies = $s->_add_agencies(\%record, $dry_run);
        #my $instruments = $s->_add_instruments(\%record, $dry_run);
        info "platform $platform";
        # info "   agencies : @$agencies";
        # info "   instrument : @$instruments";
    }
    say "all orgs : ".Dumper(\%all_orgs);
}

my %only_agencies = (
    'NSO' => 
);

sub _add_agencies {
    my $s = shift;
    my $ceos = shift;
    my $dry_run = shift;
    debug "agencies : ".$ceos->{'mission-agencies'};
    my $agencies = [ split qr[, ?], $ceos->{'mission-agencies'} ];
    my $site = $ceos->{'mission-site'};
    if ($site && length($site) > 1) {
        $all_orgs{$_}{$site} = 1 for @$agencies;
    }
    my $portal = $ceos->{'data-access-portal'};
    if ($portal && length($portal) > 1) {
        $all_orgs{$_}{$portal} = 1 for @$agencies;
    }

    $agencies = [ map pretty_id($_), @$agencies ];
    return $agencies if $dry_run;
    warn "TODO, ingest @$agencies";
    return $agencies;
}

sub _add_platform {
    my $s = shift;
    my $ceos = shift;
    my $dry_run = shift;
    my %platform = (
      identifier => pretty_id($ceos->{'mission-name-short'}),
      name       => $ceos->{'mission-name-full'},
      url        => $ceos->{'mission-site'},
    );
    my $url = "/platform";
    if (my $existing = $s->gcis->get("/platform/$platform{identifier}")) {
        $url = $existing->{uri};
        debug "exists : $url";
    }
    #debug "ceos data : ".Dumper($ceos);
    return $platform{identifier} if $dry_run;
    $s->gcis->post($url => \%platform) or die Dumper($s->gcis->error);
    return $platform{identifier};
}

sub _add_instruments {
    my $s = shift;
    my $ceos = shift;
    my $dry_run = shift;
    my $instruments = [ split /,/, $ceos->{'instruments'} ];
    $instruments = [ map pretty_id($_), @$instruments ];
    return $instruments if $dry_run;
    for my $instrument (@$instruments) {
        my $url = "/instrument";
        if (my $existing = $s->gcis->get("/instrument/$instruments")) {
            $url = $existing->{uri};
        }
        $s->gcis->post($url => { identifier => $instrument });
        #$s->gcis->post("/platform/$platform/instrument", { identifier => $instrument } );
    }
    return $instruments;
}


1;
