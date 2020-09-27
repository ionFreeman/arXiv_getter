import os
import re
import sys
from collections.abc import Iterator
from os import PathLike, DirEntry

import pdftotext


def offset_iterator(iter: Iterator, offset:int):
    index = 0
    for item in iter:
        if index >= offset:
            yield item
        index = index + 1

class Converter:
    def __init__(self, outputfolder:str, inputfolder:str, offset=0):
        import os
        self.outputfolder = outputfolder
        self.inputfolder = inputfolder
        self.offset = offset
        #TODO handle subfolders... offset should apply to the fully resolved list, output files should also end up in subfolders
        #TODO filter for pdf files

    # convert a pdf to text, store it in the outputfolder, return its path
    def convert_pdf_to_text(self, pdf_path:DirEntry):
        text_path = os.path.join(self.outputfolder, re.sub('\\.pdf$', '.txt', pdf_path.name))
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

    def convert_pdfs_to_text(self):
        index:int = self.offset
        for inputfile in offset_iterator(os.scandir(self.inputfolder), self.offset):
            if inputfile.name.strip().endswith(".pdf"):
                yield (index, self.convert_pdf_to_text(inputfile))
                index=index+1


def main():
    convertor = Converter(sys.argv[2], sys.argv[1], int(sys.argv[3]))
    list(convertor.convert_pdfs_to_text())

if __name__ == '__main__': main()

