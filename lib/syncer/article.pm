package syncer::article;

use Text::Levenshtein qw/distance/;
use Gcis::Client;
use Smart::Comments;
use Data::Dumper;
use base 'syncer';
use v5.14;

sub debug($) { syncer->logger->debug(@_); }
sub info($) { syncer->logger->info(@_); }
sub error($) { syncer->logger->error(@_); }
sub warning($) { syncer->logger->warn(@_); }

sub very_different {
    my ($x,$y) = @_;
    return 1 if $x && !$y;
    return 1 if $y && !$x;
    return 0 if lc $x eq lc $y;
    return distance($x,$y) > 2;
}

# return 1 if changed
# return 2 if very different
sub _set_title {
    my $s = shift;
    my $article = shift;
    my $title = shift;
    return 0 if $article->{title} eq $title;
    my $return_value = 1;
    if (very_different($article->{title}, $title) > 2) {
        say "old title : $article->{title}";
        say "new title : $title";
        $return_value = 2;
    }
    $article->{title} = $title;
    return $return_value;
}

sub _set_year {
    my $s = shift;
    my $article = shift;
    my $year = shift;
    return 0 if $article->{year} eq $year;
    debug "year : changing $article->{year} to $year";
    $article->{year} = $year;
    return 1;
}

#is $article->{journal_vol},    $crossref->{volume}, "volume";

sub sync {
    my $s = shift;
    $|=1;
    my %a = @_;
    my $limit = $a{limit};
    my $dry_run = $a{dry_run};
    my $gcid = $a{gcid};
    return if ($gcid && $gcid !~ /\/article\//);

    my $c = $s->{gcis} or die "no client";
    info "starting articles";
    my $d = Gcis::Client->new->accept("application/vnd.citationstyles.csl+json;q=0.5")
             ->url("http://dx.doi.org");
    $d->logger($c->logger);
    my @articles;
    if ($gcid) {
       @articles = ( $c->get($gcid) );
    } else {
       @articles = @{ $c->get('/article?all=1') };
   }
    my %stats;
    my $i = 0;
    debug "debugging output enabled";
    for my $art (@articles) { ### Processing===[%]       done
        debug "article $art->{identifier} :";
        last if $limit && $i++ > $limit;
        my $uri = $art->{uri} or die "no uri";
        my $article = $c->get_form($art) or die "could not get form : ".Dumper($art);
        my $doi = $article->{doi} or do {
            debug "no doi for ".Dumper($article);
            next;
        };
        my $crossref = $d->get("/$doi") or do {
            warning "No info from crossref for $doi";
            next;
        };
        my $changed;
        my $how = "";
        if ($crossref->{title}) {
            if (my $title_change = $s->_set_title($article, $crossref->{title})) {
                $changed = 1;
                $stats{title_change}++;
                $how = $title_change == 1 ? "title touch-up" : "major title change";
            }
        } else {
            warning "no title in crossref for $doi";
        }
        if (my $year = $crossref->{issued}{'date-parts'}[0][0]) {
            if ($s->_set_year($article, $crossref->{issued}{'date-parts'}[0][0])) {
                $changed = 1;
                $stats{year_changed}++;
                $how .= ", " if $how;
                $how .= "year change";
            }
        } else {
            warning "No year in crossref for $doi";
        }
        if ($dry_run) {
            if ($changed) {
                info "ready to save http://dx.doi.org/$doi";
            } else {
                info "no change for $doi";
            }
            next;
        }
        unless ($changed) {
            debug "$uri : skip";
            $stats{skip}++;
        }
        next unless $changed;
        info "$uri : update ($how)";
        $c->post( "/article/$article->{identifier}" => $article) or error $c->error;
    }
    $s->{stats} = \%stats;
    return;
}

return 1;

