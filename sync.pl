#!/usr/bin/env perl

use Getopt::Long qw/GetOptions/;
use Pod::Usage qw/pod2usage/;

use v5.14;
use lib './lib';
use lib $ENV{HOME}.'/gcis/gcis-pl-client/lib';
use Gcis::Client 0.03;
use sync;

binmode STDOUT, ':encoding(utf8)';
binmode STDERR, ':encoding(utf8)';

GetOptions(
    'dry_run|n' => \(my $dry_run),
);

my $syncer = sync->new(
    dry_run => $dry_run,
);
$syncer->run(
    which => [ qw/article/ ]
);

1;

