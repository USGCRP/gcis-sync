package Gcis::syncer::echo;
use Gcis::Client;
use base 'Gcis::syncer';
use Gcis::syncer::logger;
use Smart::Comments;
use JSON::XS;
use Mojo::UserAgent;
use IO::Uncompress::Unzip qw/unzip $UnzipError/;
use Data::Dumper;
use v5.14;

our $src = q[https://cdn.earthdata.nasa.gov/opendata/echo-publisher.opendata.zip];
our $map = {
 identifier   => sub { my $s = shift; "nasa-echo-".lc($s->{identifier});     },
 name         => sub { my $s = shift; $s->{title}                            },
 description  => sub { my $s = shift; $s->{description}                      },
 native_id    => sub { my $s = shift; $s->{identifier}                       },
 url          => sub { my $s = shift; $s->{accessURL} || $s->{landingPage}   },
 release_dt   => sub { my $s = shift; $s->{issued}                           },
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
     shift->{temporal} =~ m[^(.*)/(.*)$] and return $1;
     return;
 },
 end_time => sub {
     shift->{temporal} =~ m[^(.*)/(.*)$] and return $2;
     return;
 },
};

sub _get_opendata {
    my $ua = Mojo::UserAgent->new();
    our $src;
    my $tx = $ua->get($src);
    my $res = $tx->success or die $tx->error;
    my $zipped = $res->body;
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

    my $opendata = $s->_get_opendata();
    my $data = JSON::XS->new->decode($opendata);
    info "echo entries : ".@$data;
    for my $entry (@$data) {
        my %gcis = map { $_ => scalar $map->{$_}->($entry)} keys %$map;
        debug "got : ".Dumper(\%gcis);
    }
    return;
}

return 1;
