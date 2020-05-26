import sickle
from sickle.iterator import OAIItemIterator
from requests.exceptions import HTTPError, ConnectionError
from requests import Response
import logging
import time

MAX_CONSECUTIVE_REQUEST_FAILURES:int = 5

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


logger = getLogger(__name__)


class Sickle_Impl:
    def __init__(self, oai_url="http://export.arxiv.org/oai2", metadata_format='arXivRaw'):
        self.metadata_format = metadata_format
        self.arxiv: OAIItemIterator = sickle.Sickle(oai_url, iterator=OAIItemIterator)
        print(f"*** extracting metadata from {oai_url} in {metadata_format} format ***")

    def get_ids(self, set: str = 'cs'):
        counter = 0
        # OMG, the call to __next__ blows up on a 503 refresh response. I can't use iteration
        recerator:OAIItemIterator = self.arxiv.ListRecords(metadataPrefix=self.metadata_format, set=set)

        consecutive_failures = 0
        while True: # no assignment expressions in 3.7
            try:
                item = recerator.next()
                consecutive_failures = 0
            except StopIteration as si:
                break
            except HTTPError as he:
                logger.error(he)
                consecutive_failures+=1
                resp:Response = he.response
                if resp.status_code != 503 or consecutive_failures < MAX_CONSECUTIVE_REQUEST_FAILURES:
                    logger.error(f"OAIITemIterator.next failed with HTTPError {he.errno} Status Code {resp.status_code} with content {';'.join(he.args)}")
                    raise (he)
                logger.error(f"waiting ten seconds to resume harvesting ids from the OAI API due to HTTPError {he}")
                time.sleep(10)
                continue
            except ConnectionError as ce:
                consecutive_failures+=1
                logger.error(ce)
                if consecutive_failures < MAX_CONSECUTIVE_REQUEST_FAILURES:
                    raise(ce)
                logger.info("Taking a minute")
                time.sleep(60)
                continue
            counter += 1
            # logger.trace(f"item {counter} is {item.header.identifier}")
            ids = item.metadata['id']
            if len(ids) > 0:
                yield ids[0]
            if len(ids) > 1:
                logger.warning(f"item {counter} had multiple ids: {';'.join(ids)}")

    def get_batched_ids(self, batch_size=50000, set='cs'):
        current_batch = list()
        for id in self.get_ids(set):
            current_batch.append(id)
            current_batch_size = len(current_batch)
            if 0 == current_batch_size % 200:
                logger.debug(f"Harvested {current_batch_size} ids")
            if current_batch_size >= batch_size:
                yield current_batch
                current_batch = list()
