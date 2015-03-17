package Gcis::syncer::article;

use Text::Levenshtein qw/distance/;
use Gcis::Client;
use Smart::Comments -ENV;
use Data::Dumper;
use base 'Gcis::syncer';
use Gcis::syncer::util qw/:log/;
use v5.14;

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
    my $return_value = very_different($article->{title}, $title) ? 2 : 1;
    info "old title : $article->{title}";
    info "new title : $title";
    $article->{title} = $title;
    return $return_value;
}

# return 1 if year of article is one more (don't update year)
# return 2 if article year is more different
sub _set_year {
    my $s = shift;
    my $article = shift;
    my $year = shift;
    return 0 if $article->{year} eq $year;
    if ($article->{year} == ($year + 1)) {
        info "article year is one more than authority - no update";
        return 1;
    } 
    info "old year : $article->{year}";
    info "new year : $year";
    return 2;
}

sub _set_vol {
    my $s = shift;
    my $article = shift;
    my $vol = shift;
    return 0 if $article->{journal_vol} == $vol;
    info "old volume : $article->{journal_vol}";
    info "new volume : $vol";
    $article->{journal_vol} = $vol;
    return 1;
}

sub _set_issn {
    my $s = shift;
    my $journal = shift;
    my $issns = shift;
    for my $issn (@$issns) {
        return 0 if $journal->{online_issn} == $issn;
        return 0 if $journal->{print_issn}  == $issn;
    }
    my $v = "";
    $v = "$journal->{online_issn} (online)" if $journal->{online_issn};
    if ($journal->{print_issn}) {
        $v .= ", " if $v;
        $v .= "$journal->{print_issn} (print)" if $journal->{print_issn};
    }
    info "old issn : $v";
    info "new issn : @$issns[0]";
    $journal->{online_issn} = @$issns[0];
    return 1;
}

# return 1 if changed
# return 2 if very different
sub _set_journal_title {
    my $s = shift;
    my $journal = shift;
    my $title = shift;
    return 0 if $journal->{title} eq $title;
    info "old journal title : $journal->{title}";
    info "new journal title : $title";
    my $return_value = very_different($journal->{title}, $title) ? 2 : 1;
    $journal->{title} = $title;
    return $return_value;
}

sub _fix_issn {
    my $s = shift;
    my $issn = shift;
    my $title = shift;
    my %issn_map = (
        "Eos, Transactions American Geophysical Union"    => {"0096-3941" => "2324-9250"}, 
        "Journal of Geophysical Research: Oceans"         => {"0148-0227" => "2169-9291"}, 
        "Journal of Geophysical Research: Biogeosciences" => {"0148-0227" => "2169-8961"}, 
        "Journal of Geophysical Research: Earth Surface"  => {"0148-0227" => "2169-9011"}, 
        "c" => {"d" => "e"}, 
        );
    return $issn unless my $fix_issn = $issn_map{$title}{$issn};
    return $fix_issn;
}

$|=1;
sub sync {
    my $s       = shift;
    my %a       = @_;
    my $limit   = $a{limit};
    my $dry_run = $a{dry_run};
    my $gcid    = $a{gcid};
    return if ($gcid && $gcid !~ /\/article\//);
    my $c = $s->{gcis} or die "no client";

    my $d = Gcis::Client->new->accept("application/vnd.citationstyles.csl+json;q=0.5")
              ->url("http://api.crossref.org");

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
    for my $art (@articles) { 
        debug "article $art->{identifier} :";
        last if $limit && $i++ > $limit;
        my $uri = $art->{uri} or die "no uri";
        my $article = $c->get_form($art) or 
            die "could not get article form : ".Dumper($art);
        my $doi = $article->{doi} or do {
            debug "no doi for ".Dumper($article);
            next;
        };
        my $cref = $d->get("/works/$doi") or do {
            warning "No info from crossref for $doi";
            next;
        };
        my $crossref = $cref->{message} or do {
            warning "No content in crossref for $doi";
            next;
        };
        my $jou = $c->get("/journal/$article->{journal_identifier}") or 
            die "could not get journal : $article->{journal_identifer}";
        my $journal = $c->get_form($jou) or 
            die "could not get journal form : ".Dumper($jou);

        my %changed;
        my $how = "";

        if ($crossref->{title}) {
            if (my $title_change = $s->_set_title($article, $crossref->{title}[0])) {
                $changed{article} = 1;
                if ($title_change == 1) {
                    $stats{title_changed_touch_up}++;
                    $how = "title touch-up";
                } else {
                    $stats{title_changed_major}++;
                    $how = "major title change";
                }
            }
        } else {
            warning "no title in crossref for $doi";
        }

        if (my $year = $crossref->{issued}{'date-parts'}[0][0]) {
            if (my $year_change = $s->_set_year($article, $year)) {
                if ($year_change == 1) {
                    $stats{year_diff_but_okay}++;
                } else {
                    $changed{article} = 1;
                    $stats{year_changed}++;
                    $how .= ", " if $how;
                    $how .= "year change";
                }
            }
        } else {
            warning "No year in crossref for $doi";
        }

        if (my $vol = $crossref->{volume}) {
            if (my $vol_change = $s->_set_vol($article, $vol)) {
                $changed{article} = 1;
                $stats{vol_changed}++;
                $how .= ", " if $how;
                $how .= "volume change";
            }
        } else {
            warning "No volume in crossref for $doi";
        }

        {
            my $issn = $crossref->{ISSN};
            unless ($issn) {
                warning "No ISSN in crossref for $doi";
                next;
            }
            @$issn[0] = $s->_fix_issn(@$issn[0], $journal->{title});
            say "issn ".@$issn[0];
            if (my $issn_change = $s->_set_issn($journal, $issn)) {
               $changed{journal} = 1;
               $stats{issn_changed}++;
               $how .= ", " if $how;
               $how .= "issn change";
            }
            my $crossref_journal = $d->get("/journals/issn:@$issn[0]");
            unless ($crossref_journal) {
                warning "No journal in crossref for $doi";
                next;
            }
            my $journal_title = $crossref_journal->{message}{title};
            unless ($journal_title) {
                warning "No journal title in crossref for $doi";
                next;
            }
            if (my $journal_title_change = $s->_set_journal_title($journal, $journal_title)) {
                $changed{journal} = 1;
                $how .= ", " if $how;
                if ($journal_title_change == 1) {
                    $stats{journal_title_changed_touch_up}++;
                    $how .= "journal title touch-up";
                } else { 
                    $stats{journal_title_changed_major}++;
                    $how .= "journal major title change";
                }
            }
        }

        if ($dry_run) {
            if ($changed{article} || $changed{journal}) {
                info "ready to save http://dx.doi.org/$doi";
                info "$uri : update ($how)";
            } else {
                info "no change for $doi";
                $stats{skip}++;
            }
            next;
        }
        unless ($changed{article} || $changed{journal}) {
            debug "$uri : skip";
            $stats{skip}++;
            next;
        }
        info "$uri : update ($how)";
        if ($changed{article}) {
            $c->post( "/article/$article->{identifier}" => $article) or error $c->error;
        }
        if ($changed{journal}) {
            $c->post( "/journal/$article->{journal_identifier}" => $journal) or errror $c->error;
        }
    }
    $s->{stats} = \%stats;
    return;
}

return 1;

