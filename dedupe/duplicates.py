import logging

import collections
import pprint

import progressbar

logger = logging.getLogger(__name__)


def _load_first_block(path, bytes_to_read=4096):
    with open(path, 'rb') as f:
        first_block = f.read(bytes_to_read)
    return first_block

# TODO: this could probably be sped up by looking at the first k bytes of
#  each file
class DuplicatePartitioner(object):
    def __init__(self, files, progress, index):
        assert len(files) > 1, 'Cannot partition a list of 1 file'

        self.files = files
        self.checksum_to_files = collections.defaultdict(list)
        self._compute_checksums(progress, index)

    def _compute_checksums(self, progress, index):
        for file in self.files:
            checksum = file.checksum()
            self.checksum_to_files[checksum].append(file)

            progress.update(index)
            index += 1


def repartition(filesize_partitions):
    logger.debug('-'*80)
    logger.debug('Repartitioning by checksums')

    file_count = sum([len(files_matching_size)
                      for files_matching_size in filesize_partitions.values()])
    bar = progressbar.ProgressBar(max_value=file_count)

    repartitioned_files = collections.defaultdict(list)

    i = 0
    for bytesize, files in filesize_partitions.items():
        repartitioned_files_of_same_size = DuplicatePartitioner(
            files=files, progress=bar, index=i)
        # todo: if two files of different sizes share the same hash,
        # then this will overwrite one of the groups!

        for checksum, files_matching_checksum in \
            repartitioned_files_of_same_size.checksum_to_files.items():

            # TODO: this might merge files together with different file sizes
            #repartitioned_files[(bytesize, checksum)].extend(
            repartitioned_files[checksum].extend(
                files_matching_checksum)

        i += len(files)

    logger.debug('+' * 60)
    logger.debug('New partitions:')
    for block, files in repartitioned_files.items():
        logger.debug(pprint.pformat(files))
        logger.debug('.'*50)

    return repartitioned_files
