import bs4


with open("nopdf.xml") as xmlh:
    xml = xmlh.read()
    xmlh.close()

soup:bs4.BeautifulSoup = bs4.BeautifulSoup(xml, features='lxml')
links = [child.attrs['href'] for child in soup.descendants
         if type(child) is bs4.Tag
            and child.name == 'link'
            and child.attrs.get('href')
            and child.attrs.get('type') == 'application/pdf'
         ]
print(links)

html = soup.findChild('html')
title:str = html.find('head').find('title').text

print(title)

from lxml import etree

html = etree.parse("nopdf.xml", etree.HTMLParser())
title_element =html.xpath('/html/head/title')
if title_element and len(title_element):
    title = title_element[0].text
print(title)