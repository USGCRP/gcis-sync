#!/usr/bin/env perl

use Getopt::Long qw/GetOptions/;
use Pod::Usage qw/pod2usage/;

use Gcis::Client;
use YAML;
use Data::Dumper;
use exim;

use strict;
use v5.14;

# local $YAML::Indent = 2;

GetOptions(
  'url=s'       => \(my $url),
  'log_file=s'  => \(my $log_file = '/tmp/gcis-export.log'),
  'log_level=s' => \(my $log_level = "info"),
  'report=s'    => \(my $report),
);

pod2usage(-msg => "missing url", -verbose => 1) unless $url;

&main;

sub main {
    my $s = shift;
    my $e = exim->new($url);

    my $logger = Mojo::Log->new($log_file eq '-' ? () : (path => $log_file));
    $logger->level($log_level);
    $e->logger($logger);
    $e->logger_info("starting: ".$url);

    my $prefix = ($report =~ /^\/report\//) ? "" : "/report/"; 
    my $rep = $e->get("$prefix$report");

    $e->get_report($rep->{uri});
    $e->get_chapters('report');
    $e->get_figures('report');
    $e->get_images('figures');
    $e->get_tables('report');
    $e->get_findings('report');
    $e->get_references('report');
    $e->get_publications('references');
    $e->get_journals('publications');
    $e->get_activities('images');
    $e->get_datasets('activities');

    my @items = qw (
        report
        chapters
        figures
        images
        tables
        findings
        publications
        datasets
        );
    for my $item (@items) {
        $e->get_contributors($item);
        $e->get_files($item);
    }

    $e->export;
    $e->logger_info("done");
}

1;

=head1 NAME

export-report.pl -- export report from gcis (in yaml)

=head1 DESCRIPTION

export-report.pl exports an entire report from gcis with all of the dependent 
information.  All of the internal refrences are resolved.  It should be 
possible to load the resulting output to another gcis instance.

The output format is yaml.

=head1 SYNOPSIS

./export-report.pl [OPTIONS]

=head1 OPTIONS

=item B<--url>

GCIS URL.

=item B<--log_file>

Log file (/tmp/gcis-export.log).

=item B<--log_level>

Log level (see Mojo::Log)

=item B<--report>

Report unique identifier

=head1 EXAMPLES

    ./export-report.pl --url=https://datas-dev-front.joss.ucar.edu
         --report=indicator-annual-change-terrestrial-carbon-sequestration-contiguous-us
    ./export-report.pl --url=https://data-stage.globalchange.gov 
         --log_level=debug --report=/report/nca3

=cut
