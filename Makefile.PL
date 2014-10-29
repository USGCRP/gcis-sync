use strict;
use warnings;

use ExtUtils::MakeMaker;

WriteMakefile(
  VERSION   => '0.01',
  PREREQ_PM => {'Text::Levenshtein' => 0,
                'Gcis::Client'      => 0,
                'Smart::Comments'   => 0,
                'DateTime::Format::ISO8601' => 0,
               },
  test      => {TESTS => 't/*.t'}
);
