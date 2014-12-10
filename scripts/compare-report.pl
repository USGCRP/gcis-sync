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
  'input=s'     => \(my $input),
  'map_file=s'  => \(my $map_file),
  'not_all'     => \(my $not_all),
);

pod2usage(-msg => "missing url", -verbose => 1) unless $url;

&main;

sub main {
    my $s = shift;
    my $a = exim->new();
    my $b = exim->new($url);
    $b->not_all if $not_all;
    my $c = exim->new();
    my $map;
    my $map = $map_file ? exim->new() : '';

    my $logger = Mojo::Log->new($log_file eq '-' ? () : (path => $log_file));
    $logger->level($log_level);
    $b->logger($logger);
    $b->logger_info("starting: ".$url);

    $a->load($input);
    if ($map) {
        $map->load($map_file);
        $map->set_up_map($a->{base}[0], $url);
    }

    $b->get_full_report($a->{report}[0]->{uri});
    $b->{base}[0] = $url;

    my @items = qw (
        report
        chapters
        figures
        images
        tables
        findings
        references
        publications
        journals
        activities
        datasets
        people
        organizations
        contributors
        files
        );
    for my $item (@items) {
        $c->compare($item, $a, $b, $map);
    }

    $c->dump;

    $b->logger_info("done");
}

1;

=head1 NAME

compare-report.pl -- compare report from source to destination

=head1 DESCRIPTION

compare-report.pl compares an entire report with all of the dependent 
information.  The report source is a yaml file (see export-report.txt).
The destination is a gcis instance.

The output comparison is yaml (on STDOUT).

If a mapping file is provided, the comparison is made after the redirect 
is done.

=head1 SYNOPSIS

./compare-report.pl [OPTIONS]

=head1 OPTIONS

=item B<--url>

GCIS URL.

=item B<--log_file>

Log file (/tmp/gcis-export.log).

=item B<--log_level>

Log level (see Mojo::Log)

=item B<--input>

Input (source) report (yaml file, defaults to STDIN).

=item B<--map_File>

Input mapping file (yaml file, defaults to NULL).

=item B<--not_all>

Set to only compare first set of items (opposite of "?all=1").

=head1 EXAMPLES

    ./export-report.pl --url=http://datas-dev-front.joss.ucar.edu
         --file=report.txt

=cut
