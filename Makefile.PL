use 5.008008;
use ExtUtils::MakeMaker;

WriteMakefile(
    NAME              => 'Stream::Simple',
    PREREQ_PM         => {},
    AUTHOR         => 'Vyacheslav Matjukhin <mmcleric@yandex-team.ru>',
    clean => { FILES => 'tfiles' },
);

