#!perl

use Test::More;
use FindBin;
use YAML::XS qw/Load/;
use Path::Class qw/dir file/;

my $top = dir($FindBin::Bin)->parent->subdir('yaml');

$top->recurse(callback => sub {
        my $file = shift;
        return if $file->is_dir;
        return unless $file->basename =~ /\.yaml$/;
        my $got = eval {
            Load(scalar $file->slurp)
        };
        ok !$@, "no errors parsing $file" or diag "$file : $@";
        ok ref($got) eq 'HASH', "$file returned a yaml hash" or diag $got;
    });

ok 1;
done_testing();

