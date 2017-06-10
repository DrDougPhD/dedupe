import logging
import pprint

logger = logging.getLogger(__name__)

def filter_singletons(partitions):
    filecount = sum([len(files)
                     for files in partitions.values()])
    if filecount == 0:
        return partitions

    # identify filesizes that correspond to one or no files
    sizes_to_filter = filter(lambda key: len(partitions[key]) <= 1,
                             partitions)

    # delete these singleton filesizes from the set
    for size in list(sizes_to_filter):
        del partitions[size]

    non_singleton_filecount = sum([len(files)
                                   for files in partitions.values()])

    logger.info('{0} files to {1}: {2:.2%}'.format(
        filecount,
        non_singleton_filecount,
        non_singleton_filecount/filecount))

    logger.debug('='*80)
    logger.debug('Singleton filtering')
    for key, values in partitions.items():
        logger.debug('{0: >14} bytes | {1: >5} items'.format(
            key[0], len(values)))
        logger.debug(pprint.pformat(values))

    return partitions
