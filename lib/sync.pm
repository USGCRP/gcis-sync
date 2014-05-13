package sync;
use sync::article;
use v5.14;

sub new {
    my $c = shift;
    my %args = @_;
    my $gcis =  Gcis::Client->new->use_env;
    $gcis->find_credentials->login;
    $gcis->logger(Mojo::Log->new(path => '/tmp/gcis-sync.log'));
    bless +{
        gcis => $gcis,
        dry_run => $args{dry_run},
    }, $c;
}

sub gcis {
   shift->{gcis}; 
}

sub run {
    my $s = shift;
    my %a = @_;
    my $which = $a{which} or return;
    say "gcis url : ".$s->gcis->url;
    for my $which (@$which) {
        my $class = "sync::$which";
        my $obj = $class->new(gcis => $s->gcis);
        say "syncing $which";
        $obj->sync(
            dry_run => $s->{dry_run},
        );
    }
}

1;

