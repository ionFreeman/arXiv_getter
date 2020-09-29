import sickle
from lxml import etree
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
        trial = 0
        recerator:OAIItemIterator = None
        while recerator == None:
            # OMG, the call to __next__ blows up on a 503 refresh response. I can't use iteration
            try:
                recerator = self.arxiv.ListRecords(metadataPrefix=self.metadata_format, set=set)
            except HTTPError as he:
                refresh_period = detect_refresh_request(he.response.content.strip(), ns)
                if trial >= MAX_CONSECUTIVE_REQUEST_FAILURES:
                    raise he
                logger.info(f"arXiv asked us to wait {refresh_period} seconds on {to_ordinal(trial)} attempt in get_ids")
                trial = trial + 1
                time.sleep(refresh_period)
        consecutive_failures = 0
        backoff = 60
        last_backoff = 0
        while True: # no assignment expressions in 3.7
            try:
                item = recerator.next()
                consecutive_failures = 0
                backoff = 10
                last_backoff = 0
            except StopIteration as si:
                break
            except HTTPError as he:
                logger.error(he)
                consecutive_failures+=1
                resp:Response = he.response
                if resp.status_code != 503 or consecutive_failures > MAX_CONSECUTIVE_REQUEST_FAILURES:
                    logger.error(f"OAIITemIterator.next failed with HTTPError {he.errno} Status Code {resp.status_code} with content {';'.join(he.args)}")
                    raise (he)
                refresh_period = detect_refresh_request(he.response.content.strip(), ns)
                if(refresh_period):
                    delay = refresh_period
                else:
                    delay = backoff
                    hold_backoff = backoff
                    backoff = backoff + last_backoff
                    last_backoff = hold_backoff
                logger.error(f"waiting {delay} seconds to resume harvesting ids from the OAI API due to HTTPError {he}")
                time.sleep(delay)
                continue
            except ConnectionError as ce:
                consecutive_failures+=1
                logger.error(ce)
                if consecutive_failures > MAX_CONSECUTIVE_REQUEST_FAILURES:
                    raise(ce)
                logger.info(f"Taking {backoff} seconds")
                time.sleep(backoff)
                hold_backoff = backoff
                backoff = backoff + last_backoff
                last_backoff = hold_backoff
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


def to_ordinal(cardinal: int):
    cardinal_str = str(cardinal)
    last_digit = cardinal_str[-1]
    if last_digit in ('1', '2', '3') and (len(cardinal_str) == 1 or cardinal_str[-2] != "1"):
        if last_digit == '1':
            append = 'st'
        elif last_digit == '2':
            append = 'nd'
        else:  # last_digit == '3'
            append = 'rd'
    else:
        append = 'th'
    return f"{cardinal_str}{append}"


ns = {'arxiv': 'http://arxiv.org/schemas/atom'
    , 'atom': 'http://www.w3.org/2005/Atom'
    , 'html': 'http://www.w3.org/1999/xhtml'
    , 'opensearch': 'http://a9.com/-/spec/opensearch/1.1/'}


def detect_refresh_request(pdf_bytes: bytes, ns: dict = ns):
    """
    Sometimes, arXiv asks you to wait ten seconds and try again
    :return:
    """
    try:
        html:etree._Element = etree.fromstring(pdf_bytes, etree.HTMLParser())
        meta_elements = html.xpath('/html/head/meta[@http-equiv="refresh"]')
        if meta_elements and len(meta_elements):
            meta_element_attrib: dict = meta_elements[0].attrib
            refresh_delay = int(meta_element_attrib.get('content', "10"))
            return refresh_delay
        # No refresh header, look in the text
        import re
        refresh_re = re.compile('(?i)Retry after (\d+) seconds')
        periods = [refresh_re.match(period.text).group(1) for period in html.iterdescendants() if period.text and refresh_re.match(period.text)]
        if periods:
            return int(periods[0])
        logger.debug(f"Non-refresh response detected \n{pdf_bytes.decode('UTF-8')}")
    except ValueError as ve:
        # as of this writing, pdf files get processed through here (after the version downgrade)
        logger.debug(f'Downloaded content of length {len(pdf_bytes)} starting with {pdf_bytes[0:100]} threw {ve}')
    return False