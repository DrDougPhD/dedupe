import os
import logging
import collections
import pprint

import xxhash

logger = logging.getLogger(__name__)


def find_file_sizes(within):
    if isinstance(within, str):
        within = [within]

    elif isinstance(within, list):
        pass

    else:
        raise ValueError('Supplied directories is an unexpected format'
                         ' -- str or list of strs expected, but received'
                         ' {}'.format(type(within)))

    found_files = collections.defaultdict(list)
    for search_directory in within:
        finder = FileFinder(within=search_directory)
        files_within_dir = finder.find()

        for filesize, files_matching_size in files_within_dir.items():
            found_files[(filesize,)].extend(files_matching_size)

    logger.info('Search complete.')

    for filesize in found_files:
        logger.debug('Files of size {} bytes:'.format(filesize))
        logger.debug(pprint.pformat(found_files[filesize]))
        logger.debug('.'*40)

    return found_files


class FileFinder(object):
    def __init__(self, within):
        assert os.path.isdir(within), 'Directory not found: {}'.format(within)

        self.directory_tree_root = within
        self.filesizes_to_files = collections.defaultdict(list)

    def _next_filepath(self):
        logger.info('-'*75)
        logger.info('Iterating through "{}"'.format(self.directory_tree_root))

        previous_line_length = 0
        for directory, _, filenames in os.walk(self.directory_tree_root):
            if len(directory) <= 80:
                print(directory.ljust(previous_line_length), end='\r')
                previous_line_length = len(directory)

            for f in filenames:
                path = os.path.join(directory, f)
                if os.path.islink(path):
                    continue

                yield path

    def find(self):
        for filepath in self._next_filepath():
            file = File(path=filepath)
            logger.debug('{0: >15} -> "{1}"'.format(file.size, file.path))
            self.filesizes_to_files[file.size]\
                .append(file)

        return self.filesizes_to_files

class File(object):
    def __init__(self, path):
        self.path = path
        self.size = self._get_size(path)
        self.hasher = xxhash.xxh64()
        self.hash = None

    def _get_size(self, path):
        return os.path.getsize(path)

    def first_block(self, k):
        with open(self.path, 'rb') as f:
            return f.read(k)

    def checksum(self):
        with open(self.path, 'rb') as f:
            while True:
                data = f.read(4096)
                if not data:
                    break
                self.hasher.update(data)
        self.hash = self.hasher.hexdigest()
        return self.hash

    def __str__(self):
        return self.path

    def __repr__(self):
        if self.hash:
            return '<File(checksum={0}, path="{1}")>'.format(
                self.hash, self.path)

        return '<File(path="{}")>'.format(self.path)

    def __lt__(self, other):
        return self.path < other.path
