package Gcis::syncer::podaac;
use Gcis::Client;
use base 'Gcis::syncer';
use Gcis::syncer::util qw/:log iso_date/;
use Smart::Comments;
use Mojo::UserAgent;
use Data::Dumper;

use v5.14;
our $src = "http://podaac.jpl.nasa.gov/ws/search/dataset/";
our $map = {
    identifier  =>  sub { my $dom = shift; "nasa-".(lc $dom->id->text); },
    name        =>  sub { my $dom = shift; $dom->title->text;         },
    description =>  sub { my $dom = shift; $dom->at('content')->text; },
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
    my $c = $s->{gcis} or die "no client";
    return if ($gcid && $gcid !~ /^\/dataset\/nasa-podaac-/);
    my %stats;

    my $per_page    = 400;
    my $more        = 1;
    my $start_index = 1;
    my $ua          = Mojo::UserAgent->new();
    my $url         = Mojo::URL->new($src)->query(format => "atom", itemsPerPage => $per_page);

    while ($more) {
        $more = 0;
        my $tx = $ua->get($url->query([ startIndex => $start_index ]));
        my $res = $tx->success or die $tx->error;
        for my $entry ($res->dom->find('entry')->each) {
            my %gcis_info = $s->_extract_gcis($entry);
            $more = 1;
            next if $gcid && $gcid ne "/dataset/$gcis_info{identifier}";

            # insert or update
            my $existing = $c->get("/dataset/$gcis_info{identifier}");
            my $url = $existing ? "/dataset/$gcis_info{identifier}" : "/dataset";
            $stats{ ($existing ? "updated" : "created") }++;
            if ($dry_run) {
                info "ready to POST to $url";
                next;
            }
            # TODO skip if unchanged
            debug "sending ".Dumper(\%gcis_info);
            $c->post($url => \%gcis_info) or do {
                error $c->error;
                die "bailing out, error : ".$c->error;
            };
        }
        $start_index += $per_page;
    }

    $s->{stats} = \%stats;
    return;
}

sub _extract_gcis {
    state $count;
    my $s = shift;
    my $dom = shift;
    our $map;

    my %new = map { $_ => $map->{$_}->( $dom ) } keys %$map;
    $count++;
    debug "extracting entry $count : $new{identifier} : $new{native_id}";
    return %new;
}


