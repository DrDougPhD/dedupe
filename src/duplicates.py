import logging

import collections
import pprint

logger = logging.getLogger(__name__)


def _load_first_block(path, bytes_to_read=4096):
    with open(path, 'rb') as f:
        first_block = f.read(bytes_to_read)
    return first_block


class DuplicatePartitioner(object):
    def __init__(self, files):
        assert len(files) > 1, 'Cannot partition a list of 1 file'

        self.files = files
        self.checksums = []
        self.checksum_to_files = collections.defaultdict(list)
        self._compute_checksums()


    def _compute_checksums(self):
        for file in self.files:
            checksum = file.checksum()
            self.checksum_to_files[checksum].append(file)


def repartition(filesize_partitions):
    logger.info('-'*80)
    logger.info('Repartitioning by checksums')

    repartitioned_files = collections.defaultdict(list)

    for bytesize, files in filesize_partitions.items():
        repartitioned_files_of_same_size = DuplicatePartitioner(files=files)
        repartitioned_files.update(
            repartitioned_files_of_same_size.checksum_to_files)

    logger.debug('+' * 60)
    logger.debug('New partitions:')
    for block, files in repartitioned_files.items():
        logger.debug(pprint.pformat(files))
        logger.debug('.'*50)

    return repartitioned_files
