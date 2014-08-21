package Gcis::syncer::yaml;
use base 'Gcis::syncer';

use Smart::Comments;
use Mojo::UserAgent;
use Gcis::syncer::util qw/:log pretty_id/;
use Data::Dumper;
use List::MoreUtils qw/mesh/;
use YAML::XS qw/Load/;
use Path::Class qw/file dir/;
use FindBin;
use v5.14;

sub sync {
    my $s = shift;
    my %a = @_;
    my $limit   = $a{limit};
    my $dry_run = $a{dry_run};
    my $gcid    = $a{gcid};
    my $c       = $s->{gcis} or die "no client";

    my $dir = $FindBin::Bin.'/yaml';
    -d $dir or die "cannot find $dir";
    debug "$dir";
    dir($dir)->recurse( callback => sub {
        my $file = shift;
        return if $file->is_dir();
        return unless $file->basename =~ /\.yaml$/;
        $s->_ingest_file($file, $dry_run);
    } );
}

sub _ingest_file {
    my $s = shift;
    my $file = shift;
    my $dry_run = shift;
    my $data = Load(scalar $file->slurp);
    return if $dry_run;
    my $url = $data->{create};
    if (my $existing = $s->gcis->get($data->{gcid})) {
        $url = $existing->{uri};
    }
    debug "file : ".$file->basename;
    debug "gcid : ".$data->{gcid};
    info "POST to $url";
    $s->gcis->post($url, $data->{record}) or error $s->gcis->error;
    if (my $prov = $data->{prov}) {
        info "updating prov : $prov->{uri}";
        for my $entry (@{ $prov->{entries} }) {
            my $parent = $entry->{parent};
            if ($parent =~ m[^/lexicon]) {
                $s->gcis->ua->max_redirects(0);
                $parent = $s->gcis->get($parent)->{gcid};
                $s->gcis->ua->max_redirects(5);
            }
            die "missing rel" unless $entry->{rel};
            $s->gcis->post( $prov->{uri}, {
                    parent_uri => $parent,
                    parent_rel => $entry->{rel} } ) or error $s->gcis->error;
        }
    }
}

1;

