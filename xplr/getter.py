baseUrl = "http://ieeexploreapi.ieee.org/api/v1/search/articles"
import sys
apikey = sys.argv[1] #arg
import requests # https://requests.readthedocs.io/en/master/api/
import json
#import pdftotext
import asyncio
from asyncio import Future
import logging
import os
import re
from urllib import parse


# BEGIN LOGGER SETUP
import logging
# create formatter
formatter = logging.Formatter('%(asctime)s - %(name)s - %(levelname)s - %(message)s')
logger = logging.getLogger(__name__)
console = logging.StreamHandler()
console.setFormatter(formatter)
console.setLevel(logging.DEBUG)
logger.addHandler(console)
audit = logging.FileHandler("audit.log")
audit.setFormatter(formatter)
audit.setLevel(logging.INFO)  # TODO give Audit its own logger
logger.addHandler(audit)
# END LOGGER SETUP

# determine if downloaded bytes are marked as a PDF document
pdf_test = lambda response_content: response_content[:4] == b'%PDF'

def get_article_numbers(topic, batch_number = 0, batch_size = 200):
    login = requests\
        .post(sys.argv[4]
              , {'user':sys.argv[2], 'pass':sys.argv[3], 'auth':'d1' #, 'NOCOOKIE':3506100
                 , 'url':['http://www.ieee.org/ieeexplore', 'http://www.ieee.org/ieeexplore']}
              , verify=False)

    for cookie in login.cookies:
        logger.debug(cookie, login.cookies[cookie.name])

    for header in login.headers:
        logger.debug(header, login.headers[header])

    def str2dict(tokens:str,field_delimiter=';',pair_delimiter='='):
        retval = dict()
        for nv in [namevalue.split(pair_delimiter) for namevalue in [token.strip() for token in tokens.split(field_delimiter)]]:
            retval[nv[0]] = nv[1]

    querytext = f'"Content Type":journals AND ({topic})'  # arg
    query_response = requests.get(baseUrl, params=dict(apikey= apikey
                                                       , max_records=batch_size
                                                       , start_record=1 + batch_size*batch_number
                                                       , querytext=querytext
                                                       , sort_field='article_number'))
    logger.debug(query_response.request.url)
    # 2. Capture the error that says you have done too many queries, set those start numbers aside
    # 3. Set aside the pending downloads if you start getting the authentication page
    # 4. Find sections with something like 'grep -E '^\s*((I?(X(I?V)?|V)I?)|I)I{0,2}\.\s+[A-Z]\s?[A-Z]+' computing/*.txt'
    # 5. Store each section in a subfolder named for the section title
    if query_response.status_code == 200:
        logger.debug(query_response.content)
        doclist = json.loads(query_response.content.decode("utf-8"))
        total_records = int(doclist['total_records'])
        if(total_records):
            for article_number in [article['article_number'] for article in doclist['articles'] if article['article_number']]:
                yield article_number
    else:
        logging.error(query_response.status_code)
        raise Exception(f"failed query {batch_number} {batch_size} {topic}")

def download_pdf(target_dir:str, article_number:str):
    logger.debug(f"""\n\n\n*** DOWNLOADING FOR ARTICLE {article_number} ***""")

    # retrieve the pdf file
    pdf_url = f"https://ieeexplore.ieee.org/stampPDF/getPDF.jsp?tp=&arnumber={article_number}&ref="
    # set up a fibonacci backoff
    last_backoff = 0
    backoff = 1 # conf
    for trial in range(20): #conf
        pdf_response = requests.get(pdf_url)
        pdf_bytes = pdf_response.content
        if pdf_test(pdf_bytes):
            break
        else:
            from time import sleep
            logger.debug(f"{article_number}: download failed for {trial}th time, waiting for {backoff} seconds")
            sleep(backoff)
            next_backoff = last_backoff + backoff
            last_backoff = backoff
            backoff = next_backoff
    # Write the pdf file out to the file system
    pdf_path = f'{target_dir}/{article_number}.pdf'
    with open(pdf_path, 'bw', 4096, closefd=True) as pdf_file:
        pdf_file.write(pdf_bytes)
        logger.info(f"{article_number}: created {pdf_path}")
    return pdf_path
'''
def convert_pdf_to_text(pdf_path:str):

    text_path = re.sub('\\.pdf$', '\.txt', pdf_path)
    # extract the text
    with open(pdf_path, 'br', 4096, closefd=True) as pdf_file:
        with open(text_path, 'w+', closefd=True) as text_file:
            try:
                text_file.write('\n\n'.join(pdftotext.PDF(pdf_file)))
                print(f"created {text_path}")
            except pdftotext.Error as pe:
                print(f"""getter was unable to parse {pdf_file} as PDF. It is probably the login page, which means either
(1) the VPN is not connected or you are not on the intranet or
(2) all of the 15 IEEEXplore licenses are consumed""")
                raise pe
            except BaseException as be:
                print(f"failed to create {text_path}")
                print(be)
'''
async def textResults(query_text, batch_index, max_records):
    topic_dir = parse.quote_plus(query_text)
    os.makedirs(topic_dir, exist_ok=True)
    return [download_pdf(topic_dir, article_number) for article_number in get_article_numbers(query_text, batch_index, max_records)]

query_text = 'computing'
batches = sys.argv[5]
max_records = 200                                   #conf



async def main():
    await asyncio.gather(*[textResults(query_text, batch_index, max_records) for batch_index in range(batches)])


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