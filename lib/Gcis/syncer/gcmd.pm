package Gcmd::syncer::gcmd;

use Gcis::Client;
use Gcis::syncer::util qw/:log iso_date/;
use base 'Gcis::syncer';

# Only import platforms for which we are missing information
my $src = q[http://gcmdservices.gsfc.nasa.gov/static/kms/platforms/platforms.rdf];


1;

