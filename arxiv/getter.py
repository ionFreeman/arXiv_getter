import time

base_url = "http://export.arxiv.org/api/query?"
import sys
import requests # https://requests.readthedocs.io/en/master/api/
import json
#import pdftotext
import asyncio
from asyncio import Future
import logging
import os
import re
from urllib import parse
from lxml import etree

"""
https://arxiv.org/help/api/tou
When using the legacy APIs (including OAI-PMH, RSS, and the arXiv API), make no more than one request every three seconds, and limit requests to a single connection at a time.
When using services via the arXiv API Gateway, make no more than 4 requests per second per connection, and limit requests to four connections at a time.
https://arxiv-org.atlassian.net/browse/ARXIVNG-1482?filter=15106
THe API Services Gateway has not been delivered, so we're using the arXiv API with the three second pause
"""

# BEGIN LOGGER SETUP
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
# END LOGGER SETUP
print("; ".join(sys.argv))

# determine if downloaded bytes are marked as a PDF document
pdf_test = lambda response_content: response_content[:4] == b'%PDF'
ns={'arxiv': 'http://arxiv.org/schemas/atom'
    , 'atom': 'http://www.w3.org/2005/Atom'
    , 'html': 'http://www.w3.org/1999/xhtml'}


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

def url_to_file_name(url:str):
    '''
    Take off the protocol and turn the dots and slashes to underscores. This will preserve uniqueness in this set,
    but not generally. Don't let this code get out ;)
    :param url: download link
    :return: a token that preserves uniqueness and is a legal file name
    '''
    return url.split('://')[-1].replace('.', '_').replace('/', '_')

def no_pdf(pdf_bytes:bytes, ns:dict=ns):
    """
    Determines whether arXiv has a 'no pdf' page set up for this URL
    :param pdf_bytes: the non-pdf content returned from the pdf_url
    :param ns: XML namespace dict
    :return: whether the argument is an XML document stating there's no PDF
    """
    try:
        # TODO arXiv does not always close the header link tags in these documents. Load them in beatiful soup
        root = etree.fromstring(pdf_bytes)
        title:etree._Element = root.find('html:head/html:title', ns)
        if title and title.text:
            return title.text.startswith('No PDF')
    except ValueError as ve:
        logger.error(f'{pdf_bytes} threw {ve}')
    return False

def get_pdf_links(topic, batch_number = 0, batch_size = 200, init_delay_s = 3, ns:dict = ns):

    """
    arXiv API user's manual: https://arxiv.org/help/api/user-manual
    """
    # We can't use requests.get's query string builder as it url encodes colons and spaces, which arXiv does not permit
    def query_arXiv(base_url = base_url, batch_size = batch_size, batch_number = batch_number, topic=topic, init_delay_s = init_delay_s):
        """
        reaches out to legacy arXiv query service
        NOTE The sort order is hard coded here
        @:return byte array response from http call
        """
        # requests.get('http://export.arxiv.org/api/query?max_results=200&start=0&search_query=cat:cs+AND+all:computing&sort_by=lastUpdatedDate&sort_order=descending')
        time.sleep(init_delay_s)
        query_text = f'cat:cs.LO+AND+all:{topic}'
        query = f"{base_url}max_results={batch_size}&start={batch_number*batch_size}&search_query={query_text}&sort_by=lastUpdatedDate&sort_order=descending"
        query_response = requests.get(query)
        if query_response.status_code == 200:
            return query_response.content
        else:
            logging.error(f'query failed with http status code {query_response.status_code}')
            raise Exception(f"failed query {batch_number} {batch_size} {topic}")


    response_bytes = query_arXiv(base_url, batch_size, batch_number, topic)
    # 4. Find sections with something like 'grep -E '^\s*((I?(X(I?V)?|V)I?)|I)I{0,2}\.\s+[A-Z]\s?[A-Z]+' computing/*.txt'
    # 5. Store each section in a subfolder named for the section title
    response_tree = etree.fromstring(response_bytes)
    pdf_links = [entry.find('atom:link[@type="application/pdf"]', ns).attrib['href'] for entry in response_tree.getiterator('{http://www.w3.org/2005/Atom}entry') if qualify_entry(entry, ns)]
    logger.info(f'query returned {pdf_links.count} qualified entries')
    return pdf_links


async def download_pdf(target_dir:str, pdf_url:str):
    """

    :param target_dir: directory in which to store the downloaded article in Adobe's portable document format
    :param pdf_url: link to pdf
    :return: path to saved pdf file or None if the download failed
    """
    logger.debug(f"""\n\n\n*** DOWNLOADING FOR ARTICLE {pdf_url} ***""")

    # retrieve the pdf file
    # set up a fibonacci backoff
    last_backoff = 0
    backoff = 1 # conf
    for trial in range(20): #conf
        # Fibonacci elements and cumulative wait times in seconds and hours
        #   1   1   2   3   5   8   13      21      34      55      89      144     233     377     610     987         1597            2584            4181            6765
        #   1   2   4   7   12  20  33      54      88      143     232     376     609     986     1596    2583        4180            6764            10945           17020
        #                                                                                                               1:09:40         1:52:44         3:02:25         4:43:40
        pdf_response = requests.get(pdf_url)
        pdf_bytes = pdf_response.content
        if pdf_test(pdf_bytes):
            break
        elif no_pdf(pdf_bytes):
            logger.info(f"arXiv logged a mising PDF at {pdf_url}")
        else:
            logger.debug(f"{pdf_url}: download failed for {trial}th time, waiting for {backoff} seconds")
            time.sleep(backoff)
            next_backoff = last_backoff + backoff
            last_backoff = backoff
            backoff = next_backoff
    if pdf_bytes:
        # Write the pdf file out to the file system
        pdf_path = f'{target_dir}/{url_to_file_name(pdf_url)}.pdf'
        with open(pdf_path, 'bw', 4096, closefd=True) as pdf_file:
            pdf_file.write(pdf_bytes)
            logger.info(f"{pdf_url}: created {pdf_path}")
        return pdf_path

async def download_pdfs(query_text, batch_index, max_records):
    topic_dir = parse.quote_plus(query_text)
    os.makedirs(topic_dir, exist_ok=True)
    return await asyncio.gather(*[download_pdf(topic_dir, pdf_link) for pdf_link in get_pdf_links(query_text, batch_index, max_records)])

topic = 'computing'
batches = int(sys.argv[1])
max_records = 200                                   #conf



async def main():
    await asyncio.gather(*[download_pdfs(topic, batch_index, max_records) for batch_index in range(batches)])


if __name__ == "__main__":
    # execute only if run as a script
    loop = asyncio.new_event_loop()
    loop.run_until_complete(main())

class getter:
    """ write a parallelized enqueuer to execute the API query in batches and send the article numbers to a list
    parallelized dequeuers will take batches of them off the list and sent them to Google cloud functions which will
    retrieve and store the PDFs
    """
    def enqueue(self):
        pass