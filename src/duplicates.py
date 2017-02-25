import logging

import collections
import pprint

logger = logging.getLogger(__name__)


def _load_first_block(path, bytes_to_read=512):
    with open(path, 'rb') as f:
        first_block = f.read(bytes_to_read)
    return first_block


def repartition(filesize_partitions):
    logger.info('-'*80)
    logger.info('Repartitioning by first block equality')

    repartitioned_files = collections.defaultdict(list)

    for bytesize, filepaths in filesize_partitions.items():
        for path in filepaths:
            logger.debug('File: {}'.format(path))
            first_block = _load_first_block(path)
            repartitioned_files[first_block].append(path)

    logger.debug('+' * 60)
    logger.debug('New partitions:')
    for block, filepaths in repartitioned_files.items():
        logger.debug(pprint.pformat(filepaths))
        logger.debug('.'*50)

    return repartitioned_files
