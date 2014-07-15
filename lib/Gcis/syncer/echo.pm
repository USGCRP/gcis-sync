package Gcis::syncer::echo;
use Gcis::Client;
use base 'Gcis::syncer';
use Gcis::syncer::logger;
use Smart::Comments;
use JSON::XS;
use Mojo::UserAgent;
use IO::Uncompress::Unzip qw/unzip $UnzipError/;
use DateTime;
use DateTime::Format::ISO8601;
use Data::Dumper;
use v5.14;

our $src = q[https://cdn.earthdata.nasa.gov/opendata/echo-publisher.opendata.zip];
our $map = {
 identifier   => sub { my $s = shift; "nasa-echo-".lc($s->{identifier});     },
 name         => sub { my $s = shift; $s->{title}                            },
 description  => sub { my $s = shift; $s->{description}                      },
 native_id    => sub { my $s = shift; $s->{identifier}                       },
 url          => sub { my $s = shift; $s->{accessURL} || $s->{landingPage}   },
 release_dt   => sub { my $s = shift; _fmt_date($s->{issued})                },
 lat_min => sub {
    # spatial is : west, south, east, north or : x,y
     for (shift->{spatial}) {
         /^(.*), (.*), (.*), (.*)$/ and return $1;
         /^(.*), (.*)$/ and return $1;
     }
     return;
 },
 lat_max => sub {
     for (shift->{spatial}) {
         /^(.*), (.*), (.*), (.*)$/ and return $3;
         /^(.*), (.*)$/ and return $1;
     }
     return;
 },
 lon_min => sub {
     for (shift->{spatial}) {
         /^(.*), (.*), (.*), (.*)$/ and return $2;
         /^(.*), (.*)$/ and return $2;
     }
     return;
 },
 lon_max => sub {
     for (shift->{spatial}) {
         /^(.*), (.*), (.*), (.*)$/ and return $4;
         /^(.*), (.*)$/ and return $2;
     }
     return;
 },
 start_time => sub {
     shift->{temporal} =~ m[^(.*)/(.*)$] or return;
     return _fmt_date($1);
 },
 end_time => sub {
     shift->{temporal} =~ m[^(.*)/(.*)$] or return;
     return _fmt_date($2);
 },
};

sub _fmt_date {
    my $dt = shift or return undef;
    my $dt = DateTime::Format::ISO8601->parse_datetime($dt) or return undef;
    return $dt->iso8601();
}

sub _get_opendata {
    my $c = shift;
    my $ua = Mojo::UserAgent->new();
    our $src;
    info "getting $src";
    my $tx = $ua->get($src);
    my $res = $tx->success or die $tx->error;
    my $zipped = $res->body;
    info "unzipping";
    unzip \$zipped => \(my $unzipped) or die "unzip failed $UnzipError";
    return $unzipped;
}

sub sync {
    my $s       = shift;
    my %a       = @_;
    my $limit   = $a{limit};
    my $dry_run = $a{dry_run};
    my $gcid    = $a{gcid};
    return if ($gcid && $gcid !~ /\/article\//);
    my $c = $s->{gcis} or die "no client";
    my %stats;

    my $opendata = $s->_get_opendata();
    my $data = JSON::XS->new->decode($opendata);
    info "echo entries : ".@$data;
    for my $entry (@$data) {
        my %gcis = map { $_ => scalar $map->{$_}->($entry)} keys %$map;
        my $existing = $c->get("/dataset/$gcis{identifier}");
        my $url = $existing ? "/dataset/$gcis{identifier}" : "/dataset";
        $stats{ ($existing ? "updated" : "created") }++;
        if ($dry_run) {
            info "ready to POST to $url";
            next;
        }
        # TODO skip if unchanged
        debug "sending ".Dumper(\%gcis);
        $c->post($url => \%gcis) or do {
            error $c->error;
            die "bailing out, error : ".$c->error;
        };
    }
    $s->{stats} = \%stats;
    return;
}

return 1;


