package sync::article;

use Text::Levenshtein qw/distance/;
use Gcis::Client;
use Smart::Comments;
use v5.14;

sub new {
    my $s = shift;
    my %a = @_;
    bless \%a, $s;
}

sub very_different {
    my ($x,$y) = @_;
    return 1 if $x && !$y;
    return 1 if $y && !$x;
    return 0 if lc $x eq lc $y;
    return distance($x,$y) > 2;
}

sub _set_title {
    my $s = shift;
    my $article = shift;
    my $title = shift;
    return 0 if $article->{title} eq $title;
    if (very_different($article->{title}, $title) > 2) {
        say "old title : $article->{title}";
        say "new title : $title";
    }
    $article->{title} = $title;
    return 1;
}

sub _set_year {
    my $s = shift;
    my $article = shift;
    my $year = shift;
    return 0 if $article->{year} eq $year;
    say "year : changing $article->{year} to $year";
    $article->{year} = $year;
    return 1;
}

#is $article->{journal_vol},    $crossref->{volume}, "volume";

sub sync {
    my $s = shift;
    $|=1;
    my %a = @_;
    my $c = $s->{gcis} or die "no client";
    my $d = Gcis::Client->new->accept("application/vnd.citationstyles.csl+json;q=0.5")
             ->url("http://dx.doi.org");
    $d->logger($c->logger);
    my $articles = $c->get('/article?all=1');
    for my $art (@$articles) { ### Processing... [%]
        my $article = $c->get_form($art) or die "could not get form : ".Dumper($art);
        my $doi = $article->{doi};
        my $crossref = $d->get("/$doi") or next;
        my $changed;
        #$changed = $s->_set_title($article, $crossref->{title});
        $changed = $s->_set_year($article, $crossref->{issued}{'date-parts'}[0][0], "year" );
        if ($a{dry_run}) {
            say "ready to save http://dx.doi.org/$doi" if $changed;
            next;
        }
        next unless $changed;
        $c->post( "/article/$article->{identifier}"
                   => $article) or warn $c->error;
    }
}

return 1;

