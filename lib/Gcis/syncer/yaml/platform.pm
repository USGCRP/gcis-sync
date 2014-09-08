package Gcis::syncer::yaml::platform;
use Gcis::syncer::util qw/:log/;
use v5.14;

sub instruments {
    my $s = shift;
    my ($gcid, $instruments) = @_;
    for my $instrument (@$instruments) {
        my $identifier = $instrument->{record}{identifier};
        my $instrument_gcid = "/instrument/$identifier";
        debug "Adding $instrument_gcid to $gcid";
        $s->_ingest_record($instrument_gcid,
          "/instrument" => $instrument->{record})
          or return 0;
        $s->_ingest_files($instrument_gcid => $instrument->{files});
        $s->_ingest_contributors($instrument_gcid => $instrument->{contributors});
        $s->_ingest_exterms($instrument_gcid => $instrument->{exterms});
        # instrument instance
        my $url = $gcid;
        $url =~ s[/([^/]+)$][/rel/$1]; # /platform/rel/$identifier
        debug "adding $identifier to $url";
        $s->gcis->post($url => {add => {instrument_identifier => $identifier}})
          or error $s->gcis->error;
    }

}

1;
