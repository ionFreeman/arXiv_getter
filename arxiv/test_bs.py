import bs4

from lxml import etree


import logging
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
console = logging.StreamHandler()
console.setFormatter(formatter)
console.setLevel(logging.DEBUG)
logger.addHandler(console)
logfile = logging.FileHandler(f"{__name__}.log")
logfile.setFormatter(formatter)
logfile.setLevel(logging.DEBUG)  # conf (ie TODO move to configuration)
logger.addHandler(logfile)

logging.warning("one")
logger.warning("two")
logger.info("three")
logger.debug("four")