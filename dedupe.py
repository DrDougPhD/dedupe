#!/usr/bin/env python3
# -*- coding: utf-8 -*-
"""
SYNOPSIS

    python dedupe.py [-h] [-v] [-d DATABASE_FILE] [-o REPORT_FILEPATH]
                     [-s REMOVAL_SCRIPT] [-l HARDLINK_SCRIPT]
                     PATH [PATH ...]


DESCRIPTION

    Identify duplicate files that reside in a directory.


ARGUMENTS

    -h, --help                     show this help message and exit
    -v, --verbose                  verbose output
    -d, --db                       the path to the database
                                   (default: $cwd/dedupe.py.db)
    -o, --output-to-file           write duplicate file consumption analysis to
                                    file (default: stdout)
    -s, --create-remove-script     create script for removing duplicate files
                                    (default: no script)
    -l, --create-hardlink-script   create script to remove duplicate files and 
                                    convert them to hard links. Linux only.
                                    (default: no script)


AUTHOR

    Doug McGeehan <doug.mcgeehan@mst.edu>


DEPENDENCIES

    SQLAlchemy
    xxhash
    humanfriendly
    progressbar2
    

LICENSE

    Copyright 2017 Doug McGeehan - GNU GPLv3

"""

import dedupe.utils
logger = dedupe.setup_logger(name=__name__, verbosity=True)

import argparse
from datetime import datetime
import sys
import os
import logging
import humanfriendly

__appname__ = "dedupe"
__author__ = "Doug McGeehan"
__version__ = "0.1.0"
__license__ = "GNU GPLv3"

import dedupe.filesystem
import dedupe.db
import dedupe.duplicates


def main(args):
    # partition files under the specified directories by their file sizes
    logger.info('Walking directory and collecting filenames and sizes')
    filesizes = dedupe.filesystem.find_file_sizes(within=args.paths)

    # store dictionary in a sqlite db
    logger.info('Inserting files into database')
    db = dedupe.db.insert_files(filesizes, into=args.db)

    # remove singleton partitions (files that have a unique file size)
    logger.info('Filtering out singleton size-partitions')
    potential_duplicates = dedupe.utils.filter_singletons(filesizes)

    # for each partition, check for duplicates within
    logger.info('Finding duplicates within size-partitions by checksum')
    duplicate_partitions = dedupe.duplicates.repartition(
        filesize_partitions=potential_duplicates)

    # add checksums to the database
    logger.info('Adding checksums to database')
    dedupe.db.update_with_checksums(duplicate_partitions, db)

    # filter out singleton partitions (again)
    logger.info('Filtering out singleton checksum-partitions')
    only_duplicates = dedupe.utils.filter_singletons(duplicate_partitions)

    logger.info('Sorting redundant files by deduplication file savings')
    partitions_sorted_by_size_reduction = []
    for checksum, files in only_duplicates.items():
        redundant_file_count = len(files)-1
        redundant_occupied_size = files[0].size * redundant_file_count
        partitions_sorted_by_size_reduction.append((redundant_occupied_size,
                                                    files))

    partitions_sorted_by_size_reduction.sort(key=lambda x: x[0], reverse=True)

    logger.info('Finalizing analysis')

    # if a file was specified to write the report to, then write it there.
    # otherwise, write it to standard output.
    if args.report_filepath:
        output = open(args.report_filepath, 'w')

    else:
        output = sys.stdout

    potential_savings_total = 0
    for potential_savings, partition in partitions_sorted_by_size_reduction:
        potential_savings_total += potential_savings
        print('# {} in potential savings'.format(
            humanfriendly.format_size(potential_savings, binary=True)),
            file=output)
        for f in partition:
            print('{0.size: >13}\t{0.hash}\t{0.path}'.format(f),
                  file=output)

    print('# {} in total potential savings'.format(
        humanfriendly.format_size(potential_savings_total, binary=True)),
        file=output
    )

    # if the user specified a removal script, then create a script that will
    # preserve one file (the first file) and delete all duplicates
    if args.removal_script:
        with open(args.removal_script, 'w') as script:
            cumulative_potential_savings = 0
            for potential_savings, partition in\
                    partitions_sorted_by_size_reduction:

                cumulative_potential_savings += potential_savings

                preserved_file = partition.pop(0)
                script.write('#'*80 + '\n')
                script.write('# Preserving {}\n'.format(preserved_file.path))
                script.write('# {} in potential savings\n'.format(
                    humanfriendly.format_size(potential_savings, binary=True)))
                script.write('# {} in cumulative savings\n'.format(
                    humanfriendly.format_size(cumulative_potential_savings,
                                              binary=True)))

                for f in partition:
                    script.write('rm "{0.path}"\n'.format(f))

    # if the user specified a symlink script, then create a script that will
    # remove duplicates of a file and create a hard link
    if args.hardlink_script:
        with open(args.hardlink_script, 'w') as script:
            cumulative_potential_savings = 0
            for potential_savings, partition in\
                    partitions_sorted_by_size_reduction:

                cumulative_potential_savings += potential_savings

                preserved_file = partition.pop(0)
                script.write('#' * 80 + '\n')
                script.write('# Preserving {}\n'.format(preserved_file.path))
                script.write('# {} in potential savings\n'.format(
                    humanfriendly.format_size(potential_savings, binary=True)))
                script.write('# {} in cumulative savings\n'.format(
                    humanfriendly.format_size(cumulative_potential_savings,
                                              binary=True)))

                for f in partition:
                    # remove destination file and create hard link
                    script.write('ln --force "{preserved}"'
                                 '"{duplicate}"\n'.format(
                        preserved=preserved_file.path,
                        duplicate=f.path
                    ))


def existing_abspath(path):
    if os.path.exists(path):
        return os.path.abspath(path)
    else:
        raise ValueError('Path not found: {}'.format(os.path.abspath(path)))


def get_arguments():
    parser = argparse.ArgumentParser(
        description="Description printed to command-line if -h is called."
    )
    # during development, I set default to False so I don't have to keep
    # calling this with -v
    parser.add_argument('-v', '--verbose', action='store_true',
                        default=False, help='verbose output')
    parser.add_argument('-d', '--db', metavar='DATABASE_FILE',
                        default='dedupe.py.db',
                        help='the path to the database '
                             '(default: $cwd/dedupe.py.db)')
    parser.add_argument('-o', '--output-to-file', dest='report_filepath',
                        help='write duplicate file consumption analysis to'
                             ' file (default: stdout)')
    parser.add_argument('-s', '--create-remove-script', dest='removal_script',
                        help='create script for removing duplicate files'
                             ' (default: no script)')
    parser.add_argument('-l', '--create-hardlink-script',dest='hardlink_script',
                        help='create script to remove duplicate files and '
                             'convert them to hard links. Linux only. '
                             '(default: no script)')
    # TODO: only remove duplicates up to a point where a certain amount of
    # space is available from removal
    parser.add_argument('paths', metavar='PATH', nargs='+',
                        type=existing_abspath,
                        help='the path to which files will be checked')

    args = parser.parse_args()
    return args


if __name__ == '__main__':
    try:
        start_time = datetime.now()

        args = get_arguments()
        if args.verbose:
            logger.setLevel(logging.DEBUG)
        else:
            logger.setLevel(logging.INFO)

        logger.debug('Command-line arguments:')

        for arg in vars(args):
            value = getattr(args, arg)
            logger.debug('\t{argument_key}:\t{value}'.format(argument_key=arg,
                                                             value=value))

        logger.debug(start_time)

        main(args)

        finish_time = datetime.now()
        logger.debug(finish_time)
        logger.debug('Execution time: {time}'.format(
            time=(finish_time - start_time)
        ))
        logger.debug("#" * 20 + " END EXECUTION " + "#" * 20)

        sys.exit(0)

    except KeyboardInterrupt as e:  # Ctrl-C
        raise e

    except SystemExit as e:  # sys.exit()
        raise e

    except Exception as e:
        logger.exception("Something happened and I don't know what to do D:")
        sys.exit(1)
