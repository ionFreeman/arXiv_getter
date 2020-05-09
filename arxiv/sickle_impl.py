import sickle
from sickle.iterator import OAIItemIterator
import logging

from arxiv import getLogger

logger = getLogger(__name__)


class Sickle_Impl:
    def __init__(self, oai_url="http://export.arxiv.org/oai2", metadata_format='arXivRaw'):
        self.metadata_format = metadata_format
        self.arxiv: OAIItemIterator = sickle.Sickle(oai_url, iterator=OAIItemIterator)
        print(f"*** extracting metadata from {oai_url} in {metadata_format} format ***")

    def get_ids(self, set: str = 'cs'):
        counter = 0
        for item in self.arxiv.ListRecords(metadataPrefix=self.metadata_format, set=set):
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
