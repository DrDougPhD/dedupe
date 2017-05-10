#!/usr/bin/env python
# -*- coding: utf-8 -*-
"""
SYNOPSIS

	python dedupe.py [-h,--help] [-v,--verbose] PATH [PATH ...]


DESCRIPTION

	Concisely describe the purpose this script serves.


ARGUMENTS

	-h, --help		show this help message and exit
	-v, --verbose		verbose output


AUTHOR

	Doug McGeehan


LICENSE

	Copyright 2017 Doug McGeehan - GNU GPLv3

"""

import argparse
from datetime import datetime
import sys
import os
import logging

from hurry.filesize import si
from hurry.filesize import size

import dedupe.utils
logger = dedupe.setup_logger(name=__name__, verbosity=True)

__appname__ = "dedupe"
__author__ = "Doug McGeehan"
__version__ = "0.0pre0"
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
    logger.info('Finding duplicates within size-partitions')
    duplicate_partitions = dedupe.duplicates.repartition(filesize_partitions=potential_duplicates)

    # add checksums to the database
    logger.info('Adding checksums to database')
    dedupe.db.update_with_checksums(duplicate_partitions, db)

    # filter out singleton partitions (again)
    logger.info('Filtering out singleton checksum-partitions')
    only_duplicates = dedupe.utils.filter_singletons(duplicate_partitions)

    partitions_sorted_by_size_reduction = []
    for checksum, files in only_duplicates.items():
        redundant_occupied_size = files[0].size * (len(files)-1)
        partitions_sorted_by_size_reduction.append((redundant_occupied_size,
                                                    files))

    potential_savings_total = 0
    partitions_sorted_by_size_reduction.sort(key=lambda x: x[0], reverse=True)
    for potential_savings, partition in partitions_sorted_by_size_reduction:
        potential_savings_total += potential_savings
        print('# {} in potential savings'.format(size(potential_savings,
                                                      system=si)))
        for f in partition:
            print('{0.size}\t{0.hash}\t{0.path}'.format(f))

    print('='*80)
    print('# {} in total potential savings'.format(size(potential_savings_total, system=si)))

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
    parser.add_argument('paths', metavar='PATH', nargs='+',
                        type=existing_abspath,
                        help='the path to which files will be checked')
    parser.add_argument('-d', '--db', metavar='DATABASE_FILE',
                        default='dedupe.py.db',
                        help='the path to the database '
                             '(default: $cwd/dedupe.py.db)')

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
