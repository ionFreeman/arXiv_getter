import sickle
from sickle.iterator import OAIItemIterator


arxiv:OAIItemIterator = sickle.Sickle("http://export.arxiv.org/oai2", iterator=OAIItemIterator)
# for metadata_format in ['oai_dc', 'arXiv', 'arXivRaw', 'arXivOld']:
metadata_format = 'arXivRaw'
print(f"*** {metadata_format} ***")
counter = 0
for item in arxiv.ListRecords(metadataPrefix = metadata_format, set = 'cs'):
    counter += 1
    print(item.header.identifier)
    print(item.raw)
    print(item.metadata['id'])
    print()
    print()
    if counter > 10:
        break

print(arxiv)