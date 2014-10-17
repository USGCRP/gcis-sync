#!/usr/bin/env perl

# eoportal.pl
#   Get some thumbnails and quotes from eoportal, and put them into GCIS.

# Sample usage :
#
# ./eoportal.pl                                                     # default is localhost:3000
# ./eoportal.pl https://data-stage.globalchange.gov                 # all platforms
# ./eoportal.pl https://data-stage.globalchange.gov /platform/aqua  # one platform
#

use Path::Class qw/file/;
use Mojo::DOM;
use Gcis::Client;
use Data::Dumper;
use v5.14;

my $ua = Mojo::UserAgent->new();

&main;

sub get_image_and_description {
    my $ceos_url = shift;
    my $eoportal_span = $ua->get($ceos_url)->res->dom->at("#lblEOPortal");
    my $eoportal_link = $eoportal_span->find('a')->first or return;
    $eoportal_link &&= $eoportal_link->attr('href');
    my $eo = $ua->get($eoportal_link)->res->dom;
    my $article = $eo->at('.journal-content-article') or return;
    my $image = $article->find('img')->first;
    $image &&= $image->attr('src');
    my $desc;
    my $index = 0;
    for ($article->find('p')->each()) {
        next unless $index++;
        $desc = $_;
        last if $desc->text && length($desc->text) > 10;
    }
    $desc &&= $desc->text;
    my $base = Mojo::URL->new($eoportal_link);
    $image &&=  Mojo::URL->new($image)->host($base->host)->scheme($base->scheme)->to_abs;
    $image &&= "$image";
    return ( $eoportal_link, $image, $desc );
}

sub main {
    my $gcis = Gcis::Client->connect(url => $ARGV[0] || 'http://localhost:3000');
    my $gcid = $ARGV[1];
    for my $platform ($gcis->get("/platform?all=1")) {
        sleep 1;
        next if $gcid && $platform->{uri} ne $gcid;
        say $platform->{uri};
        my $info = $gcis->get($platform->{uri});
        my ($ceos) = grep { $_->{context} eq 'missionID' && $_->{lexicon} eq 'ceos' } @{ $info->{aliases} };
        my ($eourl, $img_url, $desc) = get_image_and_description($ceos->{url});
        next unless $eourl;
        my $existing = $gcis->get_form($platform);
        $existing->{description} = $desc;
        $existing->{description_attribution} = $eourl;
        $gcis->post($platform->{uri}, $existing) or die $gcis->error;
        if ($img_url)  {
            my $file_url = $platform->{uri};
            $file_url =~ s[/([^/]+)$][/files/$1];
            $gcis->post($file_url => {
                file_url => $img_url,
                landing_page => $eourl
            }) or warn "error adding file $file_url : ".$gcis->error;
        }
    }
}



