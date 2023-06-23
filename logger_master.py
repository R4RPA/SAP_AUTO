import logging


def setup_logger(name):
    """
    Creates a logger with specified name.
    Args:
    name: str
        Name of the logger.
    Returns:
    logger object
    """
    logger = logging.getLogger(name)
    logger.setLevel(logging.DEBUG)  # adjust to your desired log level

    # Console handler
    ch = logging.StreamHandler()
    ch.setLevel(logging.DEBUG)  # adjust to your desired log level

    # File handler
    fh = logging.FileHandler(f'logger_file.log')
    fh.setLevel(logging.DEBUG)  # adjust to your desired log level

    # create formatter and add it to the handlers
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    ch.setFormatter(formatter)
    fh.setFormatter(formatter)

    # add the handlers to the logger
    logger.addHandler(ch)
    logger.addHandler(fh)

    return logger
