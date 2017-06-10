import logging

import collections
import pprint

import progressbar

logger = logging.getLogger(__name__)


def _load_first_block(path, bytes_to_read=4096):
    with open(path, 'rb') as f:
        first_block = f.read(bytes_to_read)
    return first_block


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


def partition_by_first_block(files, k, progress, index):
    firstblock_partitioning = collections.defaultdict(list)

    for f in files:
        block = f.first_block(k)
        firstblock_partitioning[block].append(f)

    return firstblock_partitioning


def firstblock_repartition(filesize_partitions, k=512):
    logger.info('Finding duplicates within size-partitions by first 512 bytes')
    logger.debug('-'*80)
    logger.debug('Repartitioning by first byte')

    file_count = sum([len(files_matching_size)
                      for files_matching_size in filesize_partitions.values()])
    bar = progressbar.ProgressBar(max_value=file_count)

    repartitioned_files = {}

    i = 0
    for bytesize in list(filesize_partitions):
        files = filesize_partitions[bytesize]
        first_block_to_file_map = partition_by_first_block(
            files=files, k=k, progress=bar, index=i,
        )
        # repartitioned_files_of_same_size = DuplicatePartitioner(
        #     files=files, progress=bar, index=i)

        for firstblock, same_block_files in first_block_to_file_map.items():
            repartitioned_files[(bytesize[0], firstblock)] = same_block_files

            bar.update(i)
            i += len(same_block_files)

        del filesize_partitions[bytesize]

    bar.update(i)
    # logger.debug('+' * 60)
    # logger.debug('New partitions:')
    # for block, files in repartitioned_files.items():
    #     logger.debug(pprint.pformat(files))
    #     logger.debug('.'*50)

    return repartitioned_files


def checksum_repartition(partitions):
    logger.debug('-'*80)
    logger.debug('Repartitioning by checksum')

    file_count = sum([len(files_matching_size)
                      for files_matching_size in partitions.values()])
    bar = progressbar.ProgressBar(max_value=file_count)

    repartitioned_files = collections.defaultdict(list)

    i = 0
    for key in list(partitions):
        files = partitions[key]

        for file in files:
            checksum = file.checksum()
            logger.debug('{0}\t{1}'.format(checksum, file.path))
            size = key[0]
            repartitioned_files[(size, checksum)].append(file)

            bar.update(i)
            i += 1

        del partitions[key]

    # logger.debug('+' * 60)
    # logger.debug('New partitions:')
    # for block, files in repartitioned_files.items():
    #     logger.debug(pprint.pformat(files))
    #     logger.debug('.'*50)

    return repartitioned_files
