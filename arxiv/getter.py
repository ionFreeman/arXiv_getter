import time

from sickle_impl import Sickle_Impl, getLogger

MAX_DELAY = 18000
base_url = "http://export.arxiv.org/api/query?"
import sys
import requests  # https://requests.readthedocs.io/en/master/api/
import json
# import pdftotext
import concurrent
from concurrent import futures
from concurrent.futures import Future, ThreadPoolExecutor, TimeoutError
import logging
import os
import re
from urllib import parse
from lxml import etree
from typing import List

"""
https://arxiv.org/help/api/tou
When using the legacy APIs (including OAI-PMH, RSS, and the arXiv API), make no more than one request every three seconds, and limit requests to a single connection at a time.
When using services via the arXiv API Gateway, make no more than 4 requests per second per connection, and limit requests to four connections at a time.
https://arxiv-org.atlassian.net/browse/ARXIVNG-1482?filter=15106
THe API Services Gateway has not been delivered, so we're using the arXiv API with the three second pause
"""

# BEGIN LOGGER SETUP
logger = getLogger(__name__)
# END LOGGER SETUP
logger.info("Logger initialized")
# SET UP EXECUTOR
executor = ThreadPoolExecutor(20, "arxiv_getter_")

# SHOW RUNTIME ARGUMENTS
print("; ".join(sys.argv))

topic = sys.argv[1]

max_records = int(sys.argv[2])  # conf

# hard code the forty arXiv CS categories; they're unlikely to change before the whole system's replaced
arxiv_categories = {
    "cs": ['AI', 'AR', 'CC', 'CE', 'CG', 'CL', 'CR', 'CV', 'CY', 'DB', 'DC', 'DL', 'DM', 'DS', 'ET', 'FL', 'GL', 'GR',
           'GT', 'HC', 'IR', 'IT', 'LG', 'LO', 'MA', 'MM', 'MS', 'NA', 'NE', 'NI', 'OH', 'OS', 'PF', 'PL', 'RO', 'SC',
           'SD', 'SE', 'SI', 'SY']}
arxiv_categories_querystring = '+OR+'.join(
    [f"cat:{key}.{val}" for key in arxiv_categories for val in arxiv_categories[key]])

# determine if downloaded bytes are marked as a PDF document
pdf_test = lambda response_content: response_content[:4] == b'%PDF'
ns = {'arxiv': 'http://arxiv.org/schemas/atom'
    , 'atom': 'http://www.w3.org/2005/Atom'
    , 'html': 'http://www.w3.org/1999/xhtml'
    , 'opensearch': 'http://a9.com/-/spec/opensearch/1.1/'}


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


def str2dict(tokens: str, field_delimiter=';', pair_delimiter='='):
    retval = dict()
    for nv in [namevalue.split(pair_delimiter) for namevalue in
               [token.strip() for token in tokens.split(field_delimiter)]]:
        retval[nv[0]] = nv[1]


def qualify_entry(entry: etree._Element, ns: dict):
    '''
    :param entry: an 'entry' child of the arXiv feed element
    :return: whether the entry has a journal reference, a pdf link and a doi
    '''
    whether = True
    whether = whether and entry.find('arxiv:doi', ns) is not None
    whether = whether and entry.find('arxiv:journal_ref', ns) is not None
    whether = whether and entry.find('atom:link[@type="application/pdf"]', ns) is not None
    return whether


def url_to_file_name(url: str):
    '''
    Take off the protocol and turn the dots and slashes to underscores. This will preserve uniqueness in this set,
    but not generally. Don't let this code get out ;)
    :param url: download link
    :return: a token that preserves uniqueness and is a legal file name
    '''
    return url.split('://')[-1].replace('.', '_').replace('/', '_')


def no_pdf(pdf_bytes: bytes, ns: dict = ns):
    """
    Determines whether arXiv has a 'no pdf' page set up for this URL
    :param pdf_bytes: the non-pdf content returned from the pdf_url
    :param ns: XML namespace dict
    :return: whether the argument is an XML document stating there's no PDF
    """
    try:
        # TODO arXiv does not always close the header link tags in these documents; load as html
        html = etree.fromstring(pdf_bytes, etree.HTMLParser())
        title_elements = html.xpath('/html/head/title')
        if title_elements and len(title_elements):
            title: str = title_elements[0].text
            if title:
                return title.startswith('No PDF')
        else:
            logger.warn(f"No title element in non-pdf content\n{pdf_bytes.decode('UTF-8')}")
    except ValueError as ve:
        logger.error(f'Downloaded content of length {len(pdf_bytes)} starting with {pdf_bytes[0:100]} threw {ve}')
    return False


def detect_refresh_request(pdf_bytes: bytes, ns: dict = ns):
    """
    Sometimes, arXiv asks you to wait ten seconds and try again
    :return:
    """
    try:
        html = etree.fromstring(pdf_bytes, etree.HTMLParser())
        meta_elements = html.xpath('/html/head/meta[@http-equiv="refresh"]')
        if meta_elements and len(meta_elements):
            meta_element_attrib: dict = meta_elements[0].attrib
            refresh_delay = int(meta_element_attrib.get('content', "10"))
            return refresh_delay
        logger.debug(f"Non-refresh response detected \n{pdf_bytes.decode('UTF-8')}")
    except ValueError as ve:
        logger.error(f'{pdf_bytes} threw {ve}')
    return False


def link_from_entry(entry: etree._Element):
    """
    from https://export.arxiv.org/denied.html
    For automated programmatic harvesting

    We ask that users intent on harvesting use the dedicated site `export.arxiv.org` for these purposes, which contains an up-to-date copy of the corpus and is specifically set aside for programmatic access. This will mitigate impact on readers who are using the main site interactively.
    :param entry: extracted journal entry from atom feed
    :return: pdf download link
    """
    raw_pdf_link: str = entry.find('atom:link[@type="application/pdf"]', ns).attrib['href']
    export_pdf_link: str = raw_pdf_link.replace("//arxiv.org", "//export.arxiv.org")
    return export_pdf_link


# TODO change to a POST request limiting to the passed IDs
def query_arXiv(id_batch: List[str], base_url=base_url, max_records=max_records, batch_number=0, topic=topic, year=None,
                delay_s=3):
    """
    reaches out to legacy arXiv query service
    NOTE The sort order is hard coded here
    @:return byte array response from http call
    """
    # requests.get('http://export.arxiv.org/api/query?max_results=200&start=0&search_query=cat:cs+AND+all:computing&sort_by=lastUpdatedDate&sort_order=descending')
    query_text = f'({arxiv_categories_querystring})+AND+all:{topic}'
    logger.info(f"In {delay_s} seconds, attempting metadata query {batch_number}")
    time.sleep(delay_s)
    data: dict = dict(max_results=max_records
                      , start=batch_number * max_records
                      , search_query=query_text
                      , sort_by='lastUpdatedDate'
                      , sort_order='descending'
                      , id_list=','.join(id_batch))
    query_response = requests.post(base_url, data)
    if query_response.status_code == 200:
        return query_response.content
    else:
        logging.error(f'query failed with http status code {query_response.status_code}')
        raise Exception(f"failed query {batch_number} {max_records} {topic}")


def get_pdf_links(id_batch: List[str], topic, batch_number=0, max_records=max_records, init_delay_s=3, ns: dict = ns):
    """
    arXiv API user's manual: https://arxiv.org/help/api/user-manual
    :return (whether arXiv returned as many entried as requested, filtered list of pdf download links)
    """
    # set up the retry backoff
    last_delay = 0
    current_delay = init_delay_s
    retry = True
    while (retry and current_delay < MAX_DELAY):
        try:
            # We can't use requests.get's query string builder as it url encodes colons and spaces, which arXiv does not permit
            response_bytes = query_arXiv(id_batch, base_url, max_records, batch_number, topic, current_delay)
            # 4. Find sections with something like 'grep -E '^\s*((I?(X(I?V)?|V)I?)|I)I{0,2}\.\s+[A-Z]\s?[A-Z]+' computing/*.txt'
            # 5. Store each section in a subfolder named for the section title
            response_tree: etree._Element = etree.fromstring(response_bytes)
            pdf_links = [link_from_entry(entry) for entry in
                         response_tree.getiterator('{http://www.w3.org/2005/Atom}entry') if qualify_entry(entry, ns)]
            # Calculate the total entries; this will tell you if you're on the last page
            records_returned = len(response_tree.findall('atom:entry', ns))
            (total_results, start_index, items_per_page) = map(
                lambda param: int(response_tree.find(f"opensearch:{param}", ns).text)
                , ('totalResults', 'startIndex', 'itemsPerPage')
            )
            retry = (start_index + items_per_page <= total_results) and (records_returned < items_per_page)
        except Exception as exc:
            logger.error(exc)
            retry = True
            records_returned = 0
            start_index = -1
            total_results = 0

        if retry:
            next_delay = current_delay + last_delay
            last_delay = current_delay
            current_delay = next_delay
            logger.info(f'''query return {records_returned} total entries, whereas we have only found {start_index}
of an expected {total_results}.''')
        else:
            logger.info(f'query returned {records_returned} total entries and {len(pdf_links)} qualified entries')
            return (records_returned == max_records, pdf_links)
    # We kept trying, but we didn't make it
    return (False, None)


def yield_pdf_links(id_batch: List[str], query_text: str, max_records: int):
    batch_index = 0
    logger.debug("reset batch id")
    # TODO catch the big-topic exception if the number is over 50,000 and break the query up by year
    more_batches = True
    while more_batches == True:
        (more_batches, pdf_links) = get_pdf_links(id_batch, query_text, batch_index, max_records)
        yield pdf_links
        logger.debug(f"completed batch {batch_index}")
        batch_index += 1


def download_pdf(target_dir: str, pdf_url: str):
    """
    :param target_dir: directory in which to store the downloaded article in Adobe's portable document format
    :param pdf_url: link to pdf
    :return: path to saved pdf file or None if the download failed
    """
    pdf_path = f'{target_dir}/{url_to_file_name(pdf_url)}.pdf'
    if os.path.exists(pdf_path):
        logger.debug(f"{pdf_path} already downloaded")
        return pdf_path
    logger.debug(f"""\n\n\n*** DOWNLOADING FOR ARTICLE {pdf_url} ***""")
    is_pdf = False
    # retrieve the pdf file
    # set up a fibonacci backoff
    last_backoff = 0
    backoff = 1  # conf
    for trial in range(20):  # conf
        # Fibonacci elements and cumulative wait times in seconds and hours
        #   1   1   2   3   5   8   13      21      34      55      89      144     233     377     610     987         1597            2584            4181            6765
        #   1   2   4   7   12  20  33      54      88      143     232     376     609     986     1596    2583        4180            6764            10945           17020
        #                                                                                                               1:09:40         1:52:44         3:02:25         4:43:40
        pdf_response = requests.get(pdf_url)
        http_status = pdf_response.status_code
        pdf_bytes = pdf_response.content.strip()
        # Some PDFs start with a UTF-8 byte order mark
        if b'\xef\xbb\xbf' == pdf_bytes[0:3]:
            pdf_bytes=pdf_bytes[3:]
        refresh_period = detect_refresh_request(pdf_bytes, ns) # Had to downgrade to 3.6; had an assignment operator below for this
        if http_status == 403:
            raise Exception(pdf_response.content.decode('UTF-8'))
        elif http_status == 200 and pdf_test(pdf_bytes):
            is_pdf = True
            break
        elif http_status == 200 and no_pdf(pdf_bytes, ns):
            logger.debug(f"arXiv logged a missing PDF at {pdf_url}")
            break
        elif refresh_period:
            logger.info(f"arXiv asked us to wait {refresh_period} seconds on {to_ordinal(trial)} attempt for {pdf_url}")
            time.sleep(refresh_period)
        else:
            logger.debug(f"{pdf_url}: download failed on {to_ordinal(trial)} attempt, waiting for {backoff} seconds")
            time.sleep(backoff)
            next_backoff = last_backoff + backoff
            last_backoff = backoff
            backoff = next_backoff

    if is_pdf:
        # Write the pdf file out to the file system
        with open(pdf_path, 'bw', 4096, closefd=True) as pdf_file:
            pdf_file.write(pdf_bytes)
            logger.debug(f"{pdf_url}: created {pdf_path}")
        return pdf_path


def download_pdfs(id_batch: List[str], topic: str, max_records: int):
    topic_dir = parse.quote_plus(topic)
    os.makedirs(topic_dir, exist_ok=True)
    for pdf_links in yield_pdf_links(id_batch, topic, max_records):
        for pdf_link in pdf_links:
            yield executor.submit(download_pdf, topic, pdf_link)


def main():
    for id_batch in Sickle_Impl().get_batched_ids(50000, 'cs'):
        logger.info(f"starting batch from {id_batch[0]} to {id_batch[-1]}")
        download_path_futures = download_pdfs(id_batch, topic, max_records)
        for download_path in download_path_futures:
            try:
                if download_path.result(3600):
                    logger.debug(f"Saved PDF to {download_path.result()}")
            except futures.TimeoutError as te:
                logger.error(f"{download_path} timed out")
            except requests.exceptions.ConnectionError as ce:
                logger.error(f"{download_path} download failed with {ce}")


if __name__ == "__main__":
    # execute only if run as a script
    main()


class getter:
    """ write a parallelized enqueuer to execute the API query in batches and send the article numbers to a list
    parallelized dequeuers will take batches of them off the list and sent them to Google cloud functions which will
    retrieve and store the PDFs
    """

    def enqueue(self):
        pass
