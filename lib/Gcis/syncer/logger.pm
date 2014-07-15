package Gcis::syncer::logger;
use base Exporter;
our @EXPORT = qw/debug info error warning/;

sub debug($)   { Gcis::syncer->logger->debug(@_); }
sub info($)    { Gcis::syncer->logger->info(@_); }
sub error($)   { Gcis::syncer->logger->error(@_); }
sub warning($) { Gcis::syncer->logger->warn(@_); }

1;

