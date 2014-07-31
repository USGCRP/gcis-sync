package Gcis::syncer::datacite;
use base 'Gcis::syncer';

use Smart::Comments;
use Mojo::UserAgent;
use Gcis::syncer::util qw/:log pretty_id/;
use Data::Dumper;
use List::MoreUtils qw/mesh/;
use v5.14;


...
# placeholder...
# > http://search.datacite.org/ui?&q=10.5067
# > http://search.datacite.org/api?&q=10.5067<http://search.datacite.org/ui?&q=10.5067>
# >
# > http://search.datacite.org/ui?&q=10.7289
# > http://search.datacite.org/api?&q=10.7289
# >

1;

