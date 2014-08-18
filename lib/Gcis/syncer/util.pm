package Gcis::syncer::util;
use DateTime;
use DateTime::Format::ISO8601;
use base Exporter;
our @EXPORT_OK = qw/debug info error warning iso_date pretty_id/;
our %EXPORT_TAGS = ( log => [qw/debug info error warning/] );

sub debug($)   { Gcis::syncer->logger->debug(@_); }
sub info($)    { Gcis::syncer->logger->info(@_); }
sub error($)   { Gcis::syncer->logger->error(@_); }
sub warning($) { warn "@_"; Gcis::syncer->logger->warn(@_); }

sub iso_date {
    my $dt = shift or return undef;
    my $parsed = DateTime::Format::ISO8601->parse_datetime($dt) or return undef;
    return $parsed->iso8601();
}

sub pretty_id {
    my $id = shift or return;
    $id =~ s[\(([^)]+\))][$1]g;
    $id = lc $id;
    $id =~ s/ /-/g;
    $id =~ s/\//-/g;
    $id =~ tr/a-z[0-9]_-/_/dc;
    $id =~ s/-$//;
    $id =~ s/^-//;
    die "cannot make id from @_" unless $id;
    return $id;
}

1;

