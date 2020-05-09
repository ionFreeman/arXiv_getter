import logging
def getLogger(module:str, console_level=logging.INFO, file_level=logging.DEBUG):
    # create formatter
    formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
    logger = logging.getLogger(module)
    logger.setLevel(logging.DEBUG)
    console = logging.StreamHandler()
    console.setFormatter(formatter)
    console.setLevel(console_level)
    logger.addHandler(console)
    logfile = logging.FileHandler(f"{module}.log")
    logfile.setFormatter(formatter)
    logfile.setLevel(file_level)  # conf (ie TODO move to configuration)
    logger.addHandler(logfile)
    # let's start her off
    logger.info(f"Configured logging for {module}")
    return logger
