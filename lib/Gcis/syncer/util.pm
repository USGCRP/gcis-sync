package Gcis::syncer::util;
use DateTime;
use DateTime::Format::ISO8601;
use base Exporter;
our @EXPORT_OK = qw/debug info error warning iso_date/;
our %EXPORT_TAGS = ( log => [qw/debug info error warning/] );

sub debug($)   { Gcis::syncer->logger->debug(@_); }
sub info($)    { Gcis::syncer->logger->info(@_); }
sub error($)   { Gcis::syncer->logger->error(@_); }
sub warning($) { Gcis::syncer->logger->warn(@_); }

sub iso_date {
    my $dt = shift or return undef;
    my $parsed = DateTime::Format::ISO8601->parse_datetime($dt) or return undef;
    return $parsed->iso8601();
}
1;

