import os
import logging
import collections
import pprint

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
        for filesize in files_within_dir:
            found_files[filesize].extend(files_within_dir[filesize])
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
        for directory, _, filenames in os.walk(self.directory_tree_root):
            for f in filenames:
                path = os.path.join(directory, f)
                logger.info('File:\t{}'.format(path))
                yield path

    def find(self):
        for filepath in self._next_filepath():
            filesize = os.path.getsize(filepath)
            logger.debug('{0: >15} -> "{1}"'.format(filesize, filepath))
            self.filesizes_to_files[filesize]\
                .append(filepath)

        return self.filesizes_to_files
