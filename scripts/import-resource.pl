#!/usr/bin/env perl

use v5.20.1;
use Mojo::UserAgent;
use Gcis::Client;
use YAML::XS qw/Dump/;
use Text::Diff qw/diff/;
use Data::Dumper;
use exim;

my $src_url = q[http://data-stage.globalchange.gov];
my $dst_url = q[https://data.gcis-dev-front.joss.ucar.edu];
my $what = $ARGV[0] || q[person];
my $verbose = 1;
my $dry_run = 0;
my $max_change = 10;
my $do_all = 1;

my @valid_resources = qw[
    reference 
    article
    book
    generic
    report
    webpage
    journal
    dataset
    person
    organization
    ];

grep $what eq $_, @valid_resources or die "invalid resource : $what";

my $whats = $what eq 'person' ? 'people' : $what.'s';
my $whats_s = $whats =~ /s$/ ? $whats."'" : $whats."'s";

my $src = Gcis::Client->new(url => $src_url);

my $dst = $dry_run ? Gcis::Client->new(    url => $dst_url)
                   : Gcis::Client->connect(url => $dst_url);
my $comp = exim->new;

say "dry_run" if $dry_run;

sub same {
    my ($x,$y) = @_;
    return !( Dump($x) cmp Dump($y) );
}

sub ignore {
    my ($what, $s) = @_;

    delete $s->{$_} for (qw(href contributors));
    if (my $a = $s->{files}) {
        for (@$a) {
            delete $_->{href};
            delete $_->{thumbnail};
        }
    }
    delete $s->{articles} if ($what eq 'journal');
    delete $s->{aliases} if ($what eq 'organization');

    return 0;
}

sub find_diff { 
    my ($what, $src, $dst, $uri) = @_;

    my $s = $src->get($uri) or die "no src : $uri";
    my $d = $dst->get($uri) or die "no dst : $uri";

    ignore($what, $s);
    ignore($what, $d);

    my $diff = $comp->compare_hash($s, $d);
    return $diff if !$diff;

    return $diff if $what ne 'reference';

    my @items = keys %$diff;
    return $diff if @items > 1;
    return $diff if $diff->{child_publication_id} ne 'diff';

    my $id = $s->{child_publication_id};
    return $diff if !same_child_pub($src, $dst, $id);
    delete $diff->{child_publication_id};
    @items = keys %$diff;
    return @items ? $diff : 0;
}

sub same_child_pub {
    my ($src, $dst, $id) = @_;

    my $child_uri = get_child_uri($src, $id);
    my $pub_dst = $dst->get($child_uri) or do {
        say "\nno child publication in dst : $id, $child_uri" if $verbose;
        return 0;
    };
    my $pub_src = $src->get($child_uri) or do {
        say "\nno child publicaiton in src : $id, $child_uri" if $verbose;
        return 0;
    };
    for my $s ($pub_dst, $pub_src) {
        delete $s->{href};
        delete $s->{contributors};
        my $files = $s->{files};
        for my $f (@$files) {
            delete $f->{href};
        }
    }
    for my $item (qw[references parents files]) {
      if ($pub_src->{$item} eq 'ARRAY') {
          next if !@$pub_src->{$item};
      }
      if ($pub_dst->{$item} eq 'ARRAY') {
          next if !@$pub_src->{$item};
      }
      delete $pub_src->{$item};
      delete $pub_dst->{$item};
    }
    my $same = same($pub_src, $pub_dst);
    if (!$same  &&  $verbose) {
        say "\nchild publications different : $id";
        say diff(\Dump($pub_src), \Dump($pub_dst));
    }
    return $same;
}

sub get_child_uri {
    my $src = shift;
    my $id = shift;
    my $pub = $src->get("/publication/$id") or error $src->error;
    my $pub_uri = $pub->{uri};
    return $pub_uri;
}

sub dump_diff {
    my ($what, $src, $dst, $uri, $diff) = @_;
    my $s = $src->get($uri) or die "no src : $uri";
    my $d = $dst->get($uri) or die "no dst : $uri";

    ignore($what, $s);
    ignore($what, $d);

    my @items = keys %$diff;
    if (!grep 'child_publication_id' eq $_, @items) {
        delete $s->{child_publication_id};
        delete $d->{child_publication_id};
    }

    say " $uri";
    say diff(\Dump($s), \Dump($d));
    return 0;
}

say "src : ".$src->url;
say "dst : ".$dst->url;
say "resource : $what";

my $all = $do_all ? '?all=1' : '';

my @src = $src->get("/$what$all");
my @dst = $dst->get("/$what$all");

delete $_->{href} for @src, @dst;

say "counts :";
say "         src : ".@src;
say "         dst : ".@dst;

# key on uri
my %src = map {$_->{uri} => $_} @src;
my %dst = map {$_->{uri} => $_} @dst;

say "identifiers :";
my @only_in_src = grep !exists($dst{$_}), keys %src;
my @only_in_dst = grep !exists($src{$_}), keys %dst;
my @common      = grep  exists($dst{$_}), keys %src;
say "      common : ".@common;
say " only in src : ".@only_in_src;
say " only in dst : ".@only_in_dst;

say "content : ";
my %diffs;
my $ndiff = 0;
my $nsame = 0;
for (@common) {
    my $d = find_diff($what, $src, $dst, $_);
    if (!$d) {
        $nsame++;
        next;
    }
    $diffs{$_} = $d;
    $ndiff++;
}
   
say "        same : $nsame";
say "   different : $ndiff";

if ($verbose) {
    say "\nonly in $src_url : ";
    say " ".$_ for @only_in_src;
    say "\nonly in $dst_url : ";
    say " ".$_ for @only_in_dst;
    say "\ndifferences between resources in both places : ";
    dump_diff($what, $src, $dst, $_, $diffs{$_}) for keys %diffs;
}

my $is_person = $what eq 'person';
my %dst_names;
if ($is_person) {
    map $dst_names{$_->{last_name}}++, @dst;
    if ($verbose) {
        say "last name duplicates in $dst_url: ";
        for my $name (keys %dst_names) {
             say " $name : $dst_names{$name}" if ($dst_names{$name} > 1);
        }
    }
}
my $is_book = $what eq 'book';
my %dst_isbns;
if ($is_book) {
    %dst_isbns = map {$_->{isbn} => $_} @dst;
}

$dry_run ? say "$whats to add to $dst_url"
         : say "adding $whats to $dst_url";
my $n = 0;
for my $src_uri (@only_in_src) {
    my $name;
    my $obj = $src{$src_uri};
    if ($is_person) {
        my $name = $obj->{last_name};
        if ($dst_names{$name}) {
            say " same last name in dst : $name, $src_uri";
            next;
        }
    }
    if ($is_book) {
        if ($dst_isbns{$obj->{isbn}}) {
            say " isbn match : $obj->{isbn}, src uri : $src_uri, ".
                    "dst uri $dst_isbns{$obj->{isbn}}->{uri}";
            next;
        }
    }
    my @allow_new = qw[
        article
        book
        generic
        report
        webpage
        journal
        dataset
        person
        organization
        ];
    grep $what eq $_, @allow_new or next;

    last if $n >= $max_change;
    $n++;
    if ($is_person) {
        say " $name, $src_uri";
        $dst_names{$name}++;
    } else {
        say " $src_uri";
    }
    next if $dry_run;

    delete $obj ->{uri};
    if ($is_person) {
        $dst->post($src_uri, $obj) or error $dst->error;
    } else {
        $dst->post("/$what", $obj) or error $dst->error;
    }
}

my %single_diff;
my %single_diff_src_undef;
my %single_diff_dst_undef;
$ndiff = 0;
$dry_run ? say "\n$whats_s item to update in $dst_url", 
         : say "\nupdating $whats_s item in $dst_url";

for my $src_uri (keys %diffs) {
    my $src_obj = $src{$src_uri};
    my $dst_obj = $dst{$src_uri};
    my $diff = $diffs{$src_uri};
    my $name = $is_person ? $src_obj->{last_name}.", " : ''; 

    $ndiff++;

    my @items = keys %$diff;
    next if @items != 1;

    my $item = $items[0];
    next if $diff->{$item} ne 'diff'; 
    $single_diff{$item}++; 
    if ($what eq 'reference'  &&  $item eq 'child_publication_uri') {
        my $src_id = $src_obj->{child_publication_id};
        $src_obj->{$item} = get_child_uri($src, $src_id) if $src_id;
        my $dst_id = $dst_obj->{child_publication_id};
        $dst_obj->{$item} = get_child_uri($dst, $dst_id) if $dst_id;
    }
    $single_diff_src_undef{$item}++ if !$src_obj->{$item};
    $single_diff_dst_undef{$item}++ if !$dst_obj->{$item};

    my %update_allowed = ( 
        reference => {
            child_publication_uri => '',
        },
        article => {
            journal_identifier => '',
            journal_pages => '',
            journal_vol => '',
            title => 'not dst undef',
            url => 'okay src undef',
            year => 'not dst undef',
        },
        book => {
            isbn => 'not dst undef',
            title => '',
            topic => '',
            url => '',
        },
        generic => {
            url => '',
        },
        report => {
            in_library => 'not dst undef',
            publication_year => 'not dst undef',
            report_type_identifier => '',
            summary => '',
            title => '',
            topic => '',
            url => '',
        },
        webpage => {
            url => '',
        },
        journal => {
            country => '',
            online_issn => '',
            print_issn => '',
            publisher => '',
            url => 'not dst undef',
        },
        person => {
            orcid => 'not dst undef',
        },
        organization => {
            country_code => '',
            name => '',
            organization_type_identifier => '',
            url => '',
        }, 
    );

    next if !exists $update_allowed{$what}->{$item};
    if ($update_allowed{$what}->{$item} ne 'okay src undef') {
        next if !$src_obj->{$item};
    }
    next if !exists $update_allowed{$what}->{$item};
    if ($update_allowed{$what}->{$item} eq 'not dst undef') {
        next if $dst_obj->{$item};
    }
    if ($what eq 'reference'  &&  $item eq 'child_publication_uri') {
        if (!$dst->get($src_obj->{$item})) {
           say " child publication does not exist in dst : $src_obj->{$item}";
           next;
        }
        delete $dst_obj->{child_publication_id};
    }

    last if $n >= $max_change;
    $n++;
    say " $name$src_uri, $item";
    next if $dry_run;

    delete $dst_obj->{uri};
    $dst_obj->{$item} = $src_obj->{$item};
    $dst->post($src_uri, $dst_obj) or error $dst->error;
}

$dry_run ? say "\nNumber to be updated : $n"
         : say "\nNumber updated : $n";

say "\nNumber of single differences (of $ndiff checked)";
map {say " $_ : $single_diff{$_}"} keys %single_diff;
say "\nNumber of single differences src undef";
map {say " $_ : $single_diff_src_undef{$_}"} keys %single_diff_src_undef;
say "\nNumber of single differences dst undef";
map {say " $_ : $single_diff_dst_undef{$_}"} keys %single_diff_dst_undef;

my $n_mul_diff = $ndiff;
map {$n_mul_diff -= $single_diff{$_}} keys %single_diff;
say "\nNumber with multiple differences : ".$n_mul_diff;

say "\ndone";
