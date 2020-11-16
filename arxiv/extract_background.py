import re
import os
import tempfile


class Extractor:
    # keys are find patterns, values are extract functions
    def __init__(self, match_extract:dict, content_folder:str = "."):
        self.match_extract = match_extract
        self.content_folder = os.path.abspath(content_folder)

    def extract(self, path):
        """
        For a given file path, test each find pattern against the content until one matches
        If a match is found, use the corresponding extract function to extract the desired text

        @:returns None if no pattern matched. ie, functions should never return None
        @:return extracted section of document
        """
        with open(path, 'r', 8192) as file:
            text = '\n'.join(file.readlines())
            file.close()

        for(regex, func) in self.match_extract:
            rgx:re.Pattern = regex
            if rgx.findall(text).count() > 0:  # probably should use finditer
                return func(text)

    def extract_all(self, walk: bool = False):
        """
        emit the discovered document sections
        :param walk: whether to descend into subfolders
        :return: a 2-tuple of path, document section
        """
        for path in os.listdir(self.content_folder):
            if walk and os.path.isdir(path):
                for extractedWithPath in Extractor(path).extractAll(walk):
                    yield extractedWithPath
            elif os.path.isfile(path):
                extracted = self.extract(path)
                if extracted: # skip the blanks as well as the Nones
                    yield path, extracted


    def extract_and_save_all(self, root_folder=os.path.join(tempfile.gettempdir(), str(os.getpid()))):
        """
        For any document matching a pattern, extract the desired section using the corresponding function
        :param root_folder: folder to which to save extracts... they'll have the same path the original had relative to
        the content root
        :return: count of successfully saved documents # todo add statistics on size least mean greatest stddev
        """
        abs_root = os.path.abspath(root_folder)
        re_content_folder = re.compile(f"^({self.content_folder}")
        path:str = ''
        for path, extracted in self.extract_all():
            out_path = re_content_folder.sub(abs_root, path)
            with open(out_path, 'w+') as file:
                file.write(extracted)

roman_numeral:re.Pattern = re.compile("(M{0,2}C?M)?(C?D)?(C{0,2}X?C)?(X?L)?(X{0,2}I?X)?(I?V)?I{0,3}")
def increment_roman_numeral(rn:str):
    """
    test
        XCIX
        I
        IX
        IV
        III
    :param rn:
    :return:
    """
    if not roman_numeral.match(rn):
        return None
    if rn.endswith('III'):
        if rn.endswith('VIII'):
            return rn.replace('IX')
        else:
            return rn.replace('III', 'IV')
    elif rn.endswith('IX') and len(rn) > 3 and rn[-4] == 'X':
        # OK, the 99 problem... CMXCIX or CDXCIX have to be handled
        if len(rn) > 5 and rn[-6] == 'C':
            return rn[:-6] + rn[-5]
        else:
            return rn[:-4] + rn[-3]
    elif rn.endswith('IX') or rn.endswith('IV'):
        return rn[:-2] + rn[-1]
    else:
        return f"{rn}I"


def load_match_extract() -> dict:
    # find a 'BACKGROUND" section preceded by an Arabic or Roman numeral
    one_match = re.compile(f"^\\s*(\\d+|{roman_numeral.pattern})[.]|\\s*BACKGROUND")

    def one_func(text:str) -> str:
        extract = '' # Don't return None -- that's the signal the match failed
        for item in one_match.finditer(text):
            startpos = item.end + 1
            # OK, get this section's number
            number = text[item.start(1):item.end(1)]
            print(f"found number {number}")
            if roman_numeral.match(number) is None:
                next_index = number + 1
            else:
                next_index:str = increment_roman_numeral(number)
            next_header = f"^\\s*{next_index}[.]\\s*[ A-Z]+"
            for follow in re.finditer(next_header, text):
                endpos = follow.start()
                if endpos < startpos:
                    continue
                extract = text[startpos:endpos]
                break
            break
        return extract
    # Send back the list of match patterns and extract functions
    return {one_match:one_func}


def main():
    match_extract = load_match_extract()
    Extractor(match_extract, "text/computing").extract_and_save_all()


if __name__ == '__main__':
    main()