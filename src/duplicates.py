import logging
logger = logging.getLogger(__name__)

def repartition(filesize_partitions):
    logger.info('-'*80)
    logger.info('Repartitioning by first block equality')

