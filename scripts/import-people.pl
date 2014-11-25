#!/usr/bin/env perl

use v5.20.1;
use Mojo::UserAgent;
use Gcis::Client;
use YAML::XS qw/Dump/;
use Text::Diff qw/diff/;
use Data::Dumper;

my $src_url = q[http://data-stage.globalchange.gov];
my $dst_url = q[https://data.gcis-dev-front.joss.ucar.edu];
my $what = q[/person];
my $verbose = 1;
my $dry_run = 1;
my $max_change = 100;

my $src = Gcis::Client->new(url => $src_url);

my $dst = $dry_run ? Gcis::Client->new(    url => $dst_url)
                   : Gcis::Client->connect(url => $dst_url);
say "dry_run" if $dry_run;

sub same {
    my ($x,$y) = @_;
    return !( Dump($x) cmp Dump($y) );
}

say "src : ".$src->url;
say "dst : ".$dst->url;
say "resource : $what";

my @src = $src->get("$what?all=1");
my @dst = $dst->get("$what?all=1");

delete $_->{href} for @src, @dst;

say "counts :";
say "         src : ".@src;
say "         dst : ".@dst;

# key on uri
my %src = map {$_->{uri} => $_} @src;
my %dst = map {$_->{uri} => $_} @dst;

say "identifiers :";
my @only_in_src = grep !exists($dst{$_}), keys %src;
my @only_in_dst = grep !exists($src{$_}), keys %dst;
my @common      = grep exists($dst{$_}), keys %src;
say "      common : ".@common;
say " only in src : ".@only_in_src;
say " only in dst : ".@only_in_dst;

say "content : ";
my @same      = grep same($src{$_},$dst{$_}), @common;
my @different = grep !same($src{$_},$dst{$_}), @common;
say "        same : ".@same;
say "   different : ".@different;

if ($verbose) {
    say "only in $src_url : ";
    say $_ for @only_in_src;
    say "only in $dst_url : ";
    say $_ for @only_in_dst;
    say "\ndifferences between resources in both places : ";
    for (@different) {
        say "uri : ".$_;
        say diff(\Dump($src{$_}), \Dump($dst{$_}));
    }
}

my %dst_names;
map($dst_names{$_->{last_name}}++, @dst);
if ($verbose) {
    say "last name duplicates in $dst_url: ";
    for my $name (keys %dst_names) {
         say " $name : $dst_names{$name}" if ($dst_names{$name} > 1);
    }
}

$dry_run ? say "people to add to $dst_url"
         : say "adding people to $dst_url";
my $n = 0;
for my $src_uri (@only_in_src) {
    my $name = $src{$src_uri}->{last_name};
    if ($dst_names{$name}) {
       say " same last name in dst : $name, $src_uri";
       next;
    }

    $n++;
    last if $n > $max_change;
    say " $name, $src_uri";
    $dst_names{$name}++;
    next if $dry_run;

    my $person = $src{$src_uri};
    delete($person->{uri});
    $dst->post($src_uri, $person) or error $dst->error;
}

say "\ndone";
