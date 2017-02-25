import logging
import pprint

logger = logging.getLogger(__name__)

def filter_singletons(filesizes):
    sizes_to_filter = filter(lambda key: len(filesizes[key]) <= 1,
                             filesizes)

    for size in list(sizes_to_filter):
        del filesizes[size]

    logger.debug('='*80)
    logger.debug('Potential duplicates (by filesize)')
    for filesize in filesizes:
        potential_duplicates_for_size = filesizes[filesize]
        logger.debug('{0: >14} bytes | {1: >5} items'.format(
            filesize, len(potential_duplicates_for_size)))
        logger.debug(pprint.pformat(potential_duplicates_for_size))

    return filesizes
