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
import src
import logging
logger = src.setup_logger(name=__name__, verbosity=True)

import collections
import pprint

__appname__ = "dedupe"
__author__ = "Doug McGeehan"
__version__ = "0.0pre0"
__license__ = "GNU GPLv3"

import argparse
from datetime import datetime
import sys
import os


def find_files_and_sizes(directory):
    file_sizes = collections.defaultdict(list)
    for directory, _, filenames in os.walk(directory):
        for f in filenames:
            path = os.path.join(directory, f)

            # obtain the file size
            size = os.path.getsize(path)
            file_sizes[size].append(path)
    for k in file_sizes:
        print(k)
        pprint.pprint(file_sizes[k])
    return file_sizes


def main(args):
    # partition files under the specified directories by their file sizes
    filesizes = collections.defaultdict(list)
    for directory in args.paths:
        print('-'*80)
        print(directory + ':')
        filesizes.update(
            find_files_and_sizes(directory)
        )

    # store dictionary in a sqlite db

    # remove singleton partitions (files that have a unique file size)

    # for each partition, check for duplicates within
    # re-partition by file size and duplication status
    # calculate a checksum on representative files in duplicate partitions
    # store the first 512 bytes of singleton partitions in the database
    pass


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
