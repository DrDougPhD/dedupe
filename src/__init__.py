import logging


def setup_logger(name=None, verbosity=False):
    if name is None:
        name = __name__

    logger = logging.getLogger(__name__)
    logger.setLevel(logging.DEBUG)
    # create console handler with a higher log level
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)

    # create formatter and add it to the handlers
    ch.setFormatter(logging.Formatter(
        "%(levelname)s [%(filename)s:%(lineno)s - %(funcName)20s() ]"
        " %(message)s"
    ))

    # add the handlers to the logger
    logger.addHandler(ch)
    return logger
