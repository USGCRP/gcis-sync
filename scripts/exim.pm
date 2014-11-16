package exim;

use Gcis::Client;
use strict;
use Data::Dumper;
use YAML;
use v5.14;

local $YAML::Indent = 2;

binmode STDOUT, ':encoding(utf8)';

my @item_list = qw (
    report
    chapters
    figures
    images
    tables
    findings
    references
    publications
    journals
    activities
    datasets
    people
    organizations
    files
    );

my $all = "?all=1";

sub _unique_uri {
    my $s = shift;
    my $type = shift;
    my $v = shift;
    if (!$s->{$type}) {
        $s->{$type}[0] = $v;
        return 1;
    }
    my $n = 0;
    while (my $uri = $s->{$type}[$n]->{uri}) {
        return 0 if $uri eq $v->{uri};
        $n++;
    }
    $s->{$type}[$n] = $v;
    return 1;
}

sub new {
    my $class = shift;
    my $base = shift;
    my $s->{gcis} = Gcis::Client->connect(url => $base);
    bless $s, $class;
    return $s;
}

sub get {
    my $s = shift;
    my $uri = shift;

    my $v = $s->{gcis}->get($uri);
    return wantarray && ref($v) eq 'ARRAY' ? @$v : $v;
}

sub logger {
    my $s = shift;
    my $logger = shift;

    $s->{gcis}->logger($logger);
    return 0;
}

sub logger_info {
    my $s = shift;
    my $message = shift;
    $s->{gcis}->logger->info($message);
    return 0;
}

sub get_report {
    my $s = shift;
    my $uri = shift;
    my $report = $s->get($uri) or die "no report";
    $s->{report}[0] = $report;
    for my $item (@item_list) {
        next if $item eq "report";
        $s->{$item} = [];
    }
    # $report->{summary} = "put summary back in after debug";
    return 0;
}

sub get_chapters {
    my $s = shift;
    my $type = shift;

    my $obj = $s->{$type}[0];
    my $chapters = $obj->{chapters};
    my $n = 0;
    my $obj->{chapter_uris} = [];
    for my $chap (@$chapters) {
        my $uri = $chap->{uri};
        my $chapter = $s->get("$uri$all") or die "no chapter";
        $s->{$type}[0]->{chapter_uris}[$n++] = $uri;
        $s->_unique_uri('chapters', $chapter);
    }
    delete $s->{$type}[0]->{chapters};

    return 0;
}

sub get_figures {
    my $s = shift;
    my $type = shift;

    my $obj_uri = $s->{$type}[0]->{uri};
    my @figures = $s->get("$obj_uri/figure$all") or return 1;
    my $n = 0;
    for my $fig (@figures) {
        my $figure = $s->get($fig->{uri}) or die "no figure";
        delete $figure->{chapter};
        $s->{figures}[$n++] = $figure;
    }

    return 0;
}

sub get_images {
    my $s = shift;
    my $type = shift;

    my $n_obj = 0;
    while (my $obj = $s->{$type}[$n_obj]) {
        my $images = $obj->{images};
        my $n_img = 0;
        $s->{$type}[$n_obj]->{image_uris} = [];
        for my $img (@$images) {
            my $uri = "/image/$img->{identifier}";
            my $image = $s->get($uri) or die "no image";
            $s->{$type}[$n_obj]->{image_uris}[$n_img++] = $uri;
            delete $image->{figures};
            $s->_unique_uri('images', $image);
        }
        delete $s->{$type}[$n_obj]->{images};
        $n_obj++;
    }

    return 0;
}

sub get_tables {
    my $s = shift;
    my $type = shift;

    my $obj_uri = $s->{$type}[0]->{uri};
    my @tables = $s->get("$obj_uri/table$all") or return 1;
    my $n = 0;
    for my $tab (@tables) {
        my $table = $s->get($tab->{uri}) or die "no figure";
        delete $table->{chapter};
        $s->{tables}[$n++] = $table;
    }

    return 0;
}

sub get_findings {
    my $s = shift;
    my $type = shift;

    my $obj = $s->{$type}[0];
    my $findings = $s->get("$obj->{uri}/finding$all") or return 1;
    my $n = 0;
    my $obj->{finding_uris} = [];
    for my $find (@$findings) {
        my $uri = $find->{uri};
        my $finding = $s->get($uri) or die "no finding";
        $obj->{finding_uris}[$n++] = $uri;
        delete $finding->{chapter};
        $s->_unique_uri('findings', $finding);
    }

    return 0;
}

sub get_references {
    my $s = shift;
    my $type = shift;

    my $obj_uri = $s->{$type}[0]->{uri};
    my @references = $s->get("$obj_uri/reference$all") or return 1;
    my $n = 0;
    for my $ref (@references) {
        my $reference = $s->get($ref->{uri}) or die "no reference";
        delete $reference->{chapter};
        $s->{references}[$n++] = $reference;
    }

    return 0;
}

sub get_publications {
    my $s = shift;
    my $type = shift;

    my $n_obj = 0;
    while (my $obj = $s->{$type}[$n_obj]) {
        if (my $uri = $obj->{child_publication_uri}) {
          my $pub = $s->get($uri) or die "no publication";
          $s->_unique_uri('publications', $pub);
        }
        $n_obj++;
    }

    return 0;
}

sub get_journals {
    my $s = shift;
    my $type = shift;

    my $n_obj = 0;
    while (my $obj = $s->{$type}[$n_obj]) {
        if (my $uri = $obj->{journal_identifier}) {
          my $jou = $s->get("/journal/$uri") or die "no journal";
          $jou->{articles} = [];
          $s->_unique_uri('journals', $jou);
        }
        $n_obj++;
    }

    return 0;
}

sub get_activities {
    my $s = shift;
    my $type = shift;

    my $n_obj = 0;
    while (my $obj = $s->{$type}[$n_obj]) {
        my $activities = $obj->{parents};
        for my $act (@$activities) {
            my $activity = $s->get($act->{activity_uri}) or die "no activity";
            my $pub_maps = $activity->{publication_maps};
            for my $pub_map (@$pub_maps) {
                my $child_uri = $pub_map->{child_uri};
                my $child = $s->get($child_uri) or die "no child";
                $pub_map->{child_uri} = $child->{uri};
                my $parent_uri = $pub_map->{parent_uri};
                my $parent = $s->get($parent_uri) or die "no parent";
                $pub_map->{parent_uri} = $parent->{uri};
            }
            $s->_unique_uri('activities', $activity);
        }
        $n_obj++;
    }

    return 0;
}

sub get_datasets {
    my $s = shift;
    my $type = shift;

    my $n_obj = 0;
    while (my $obj = $s->{$type}[$n_obj]) {
        my $pub_maps = $obj->{publication_maps};
        for my $pub_map (@$pub_maps) {
            my $parent_uri = $pub_map->{parent_uri};
            my $parent = $s->get($parent_uri) or die "no parent";
            ($parent->{uri} =~ /^\/dataset\//) or die "parent not a dataset";
            $s->_unique_uri('datasets', $parent);
        }
        $n_obj++;
    }

    return 0;
}

sub get_contributors {
    my $s = shift;
    my $type = shift;

    my $n_obj = 0;
    while (my $obj = $s->{$type}[$n_obj]) {
        my $contributors = $obj->{contributors};
        for my $con (@$contributors) {
            my $org_uri = $con->{organization_uri};
            my $org = $s->get($org_uri) or die "no organizaton";
            delete $con->{organization};
            $s->_unique_uri('organizations', $org);
            if (my $per_uri = $con->{person_uri} ) {
                my $per = $s->get($per_uri) or die "no person";
                delete $per->{contributors};
                $s->_unique_uri('people', $per);
            }
            delete $con->{person};
            delete $con->{person_id};
        }
        $n_obj++;
    }
    return 0;
}

sub get_files {
    my $s = shift;
    my $type = shift;

    my @objs = shift;
    my $n_obj = 0;
    while (my $obj = $s->{$type}[$n_obj]) {
        my $files = $obj->{files};
        my $n = 0;
        $s->{$type}[$n_obj]->{file_uris} = [];
        for my $f (@$files) {
            my $f_uri = $f->{uri};
            $s->{$type}[$n_obj]->{file_uris}[$n++] = $f_uri;
            my $file = $s->get($f_uri) or die "no file";
            $s->_unique_uri('files', $file);
        }
        delete $s->{$type}[$n_obj]->{files};
        $n_obj++;
    }
    return 0;
}

sub count {
   my $s = shift;
   my $type = shift;

   my $n = 0;
   while ($s->{$type}[$n]) {
       $n++;
   }
   return $n;
}

sub export {
   my $s = shift;
   my $e->{report} = $s->{report}[0];
   for my $item (@item_list) {
       $e->{items}->{$item} = $s->count($item);
       next if $item eq "report";
       $e->{$item} = $s->{$item};
   }
   say Dump($e) or die "unable to export report";
   return;
}

1;
