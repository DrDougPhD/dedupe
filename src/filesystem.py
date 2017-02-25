import os
import logging
import collections

logger = logging.getLogger(__name__)


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
            self.filesizes_to_files[filesize]\
                .append(filepath)

        return self.filesizes_to_files
