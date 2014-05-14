#!/usr/bin/env perl

use Getopt::Long qw/GetOptions/;
use Pod::Usage qw/pod2usage/;

use v5.14;
use lib './lib';
use lib $ENV{HOME}.'/gcis/gcis-pl-client/lib';
use Gcis::Client 0.03;

use syncer::article;
my @syncers = qw/article/;

binmode STDOUT, ':encoding(utf8)';

GetOptions(
  'dry_run|n'  => \(my $dry_run),
  'url=s'      => \(my $url),
  'log_file=s' => \(my $log_file = '/tmp/gcis-sync.log'),
  'limit=s'    => \(my $limit),
);

pod2usage(-msg => "missing url", -verbose => 1) unless $url;

&main;

sub main {
    my $s = shift;
    my $gcis = Gcis::Client->connect(url => $url);
    $gcis->logger(Mojo::Log->new(path => $log_file));
    say "url : ".$gcis->url;
    $gcis->logger->info("starting : ".$gcis->url);
    say "log : ".$log_file;
    my %stats;
    for my $which (@syncers) {
        my $class = "syncer::$which";
        my $obj = $class->new(gcis => $gcis);
        $obj->sync(
            dry_run => $dry_run,
            limit => $limit,
        );
        $stats{$which} = $obj->stats || {};
    }
    print "\n";
    for my $k (keys %stats) {
        next unless ref $stats{$k};
        my $line = join ' ', map "$_=$stats{$k}{$_}", keys %{ $stats{$k} };
        say "$k : $line ";
        $gcis->logger->info("stats : $k : $line");
    }
}

1;

=head1 NAME

sync.pl -- sync gcis with various srouces

=head1 OPTIONS

=item B<-url>

GCIS URL.

=item B<-dry_run|-n>

Dry run.

=item B<-log_file>

Log file (/tmp/gcis-sync.log)

=item B<-log_file>

Limit number of items of each type to sync (default all).

=cut

=cut

