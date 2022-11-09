import requests
from tqdm import tqdm
from requests.adapters import HTTPAdapter
from urllib3.util.retry import Retry
from lxml import etree
from loguru import logger
from pathlib import Path
import datetime
import time
import urllib.parse
import gzip
from halo import Halo
from io import BytesIO
import re
import sys

def setup_requests() -> requests.Session:
        """
        Sets up a requests session to automatically retry on errors
        """
        http = requests.Session()
        assert_status_hook = (
            lambda response, *args, **kwargs: response.raise_for_status()
        )
        http.hooks["response"] = [assert_status_hook]
        retry_strategy = Retry(
            total=3,
            status_forcelist=[429, 500, 502, 503, 504],
            allowed_methods=["GET"],
            backoff_factor=1,
        )
        adapter = HTTPAdapter(max_retries=retry_strategy)
        http.mount("https://", adapter)
        http.mount("http://", adapter)
        return http

def getResumptionToken(response) -> str:
    """
    Extract ResumptionToken from OAI Response
    """
    namespaces = {"oai": "http://www.openarchives.org/OAI/2.0/"}
    try:
        root = etree.XML(response.content)
    except etree.XMLSyntaxError as e:
        logger.error(
            f"Fehler beim Parsen des Resumption Tokens: '{e}'")
        token = None
    else:
        token = root.findall(f".//oai:resumptionToken", namespaces)
        try:
            token[0]
        except:
            token = None
        else:
            if token[0].text is not None:
                token = token[0].text
            elif token[0].attrib['resumptionToken'] is not None:
                token = token[0].attrib['resumptionToken']
            else:
                token = None
            urllib.parse.quote_plus(token)
    return token

def getListSize(url):
    """
    Extract stated listSize of OAI Response
    """
    r = http.get(url)
    rtree = etree.fromstring(r.text.encode('utf-8'))
    if rtree.find('.//oai:resumptionToken', NAMESPACES) is not None:
        listsize = rtree.find('.//oai:resumptionToken', NAMESPACES).attrib['completeListSize']
    else:
        listsize = None
    return listsize

def getOAIrecords(url) -> str:
    r = http.get(url)
    rtree = etree.fromstring(r.text.encode('utf-8'))
    if rtree.find('.//oai:resumptionToken', NAMESPACES) is not None:
        resumptiontoken = rtree.find('.//oai:resumptionToken', NAMESPACES).text
    else:
        resumptiontoken = None
    oairecords = rtree.findall('.//oai:record//oai:metadata/rdf:RDF/rdf:Description', NAMESPACES)
    for e in oairecords:
        record_dict = { 'about': '',
                        'datemodified': '',
                        'elem': ''}
        e_about = e.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about']
        if e_about.startswith('https://d-nb.info'):
            pass
        else:
            e_datemodified = e.find('.//dcterms:modified', NAMESPACES).text
            record_dict['about'] = e_about
            record_dict['datemodified'] = datetime.datetime.strptime(e_datemodified, '%Y-%m-%dT%H:%M:%S.%f')
            record_dict['elem'] = e
            oai_records[e_about] = record_dict

    return resumptiontoken, len(oairecords)

# logger.remove()
lognamefilename = time.strftime("%Y-%m-%d_%H-%M-%S") + "_zdbdump-update.log"
logfile = lognamefilename
logger.add(
        logfile,
        level=0,
        format="<green>{time:YYYY-MM-DD HH:mm:ss.SSS}</green> | <level>{level: <8}</level> | <level>{message}</level>",
        enqueue=True
    )

DUMP = sys.argv[1]
if DUMP.startswith('http'):
    DUMP_NAME = DUMP.split('/')[-1]
else:
    DUMP_NAME = Path(DUMP).name
DUMP_DATE = re.findall(r'.*(\d{8})\.rdf\.gz', DUMP_NAME)[0]
OAI_DATE = re.sub(r'(\d{4})(\d{2})(\d{2})', r'\1-\2-\3', DUMP_DATE)
BASEURL = "https://services.dnb.de/oai/repository?"
PREFIX = "&metadataPrefix=RDFxml"
SET = "&set=zdb"
FROM = f"&from={OAI_DATE}"
CWD = Path.cwd()
NAMESPACES = {
        "rdf":"http://www.w3.org/1999/02/22-rdf-syntax-ns#",
        "schema":"http://schema.org/",
        "gndo":"https://d-nb.info/standards/elementset/gnd#",
        "lib":"http://purl.org/library/",
        "owl":"http://www.w3.org/2002/07/owl#",
        "xsd":"http://www.w3.org/2001/XMLSchema#",
        "skos":"http://www.w3.org/2004/02/skos/core#",
        "rdfs":"http://www.w3.org/2000/01/rdf-schema#",
        "editeur":"https://ns.editeur.org/thema/",
        "geo":"http://www.opengis.net/ont/geosparql#",
        "umbel":"http://umbel.org/umbel#",
        "rdau":"http://rdaregistry.info/Elements/u/",
        "sf":"http://www.opengis.net/ont/sf#",
        "bflc":"http://id.loc.gov/ontologies/bflc/",
        "dcterms":"http://purl.org/dc/terms/",
        "isbd":"http://iflastandards.info/ns/isbd/elements/",
        "foaf":"http://xmlns.com/foaf/0.1/",
        "mo":"http://purl.org/ontology/mo/",
        "marcRole":"http://id.loc.gov/vocabulary/relators/",
        "agrelon":"https://d-nb.info/standards/elementset/agrelon#",
        "dcmitype":"http://purl.org/dc/dcmitype/",
        "dbp":"http://dbpedia.org/property/",
        "dnbt":"https://d-nb.info/standards/elementset/dnb#",
        "madsrdf":"http://www.loc.gov/mads/rdf/v1#",
        "dnb_intern":"http://dnb.de/",
        "v":"http://www.w3.org/2006/vcard/ns#",
        "wdrs":"http://www.w3.org/2007/05/powder-s#",
        "ebu":"http://www.ebu.ch/metadata/ontologies/ebucore/ebucore#",
        "bibo":"http://purl.org/ontology/bibo/",
        "gbv":"http://purl.org/ontology/gbv/",
        "dc":"http://purl.org/dc/elements/1.1/",
        "oai":"http://www.openarchives.org/OAI/2.0/"
        }


http = setup_requests()

if not Path(CWD, DUMP_NAME).is_file():
    logger.info("Lade Dump herunter")
    r = requests.get(DUMP, stream=True)
    with open(Path(CWD, DUMP_NAME), 'wb') as f:
        spinner = Halo(text='Downloading Dump', spinner='dots')
        spinner.start()
        for chunk in r.raw.stream(1024, decode_content=False):
            if chunk:
                f.write(chunk)
        spinner.stop()
else:
    logger.info("Der Dump liegt lokal vor")


with Halo(text='Parse Dump', spinner='dots'):
    # Dump Inhalt lesen und in LXML einlesen
    with gzip.open(Path(CWD, DUMP_NAME), 'rb') as f:
        dump_content = f.read()
    tree = etree.parse(BytesIO(dump_content))
    # Über alle Elemente iterieren und Dictionary füllen
    zdb_descriptions = tree.findall('./rdf:Description', NAMESPACES)
    zdb_entries = {}
    for e in zdb_descriptions:
            record_dict = { 'about': '',
                            'datemodified': '',
                            'elem': ''}
            e_about = e.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about']
            e_datemodified = e.find('.//dcterms:modified', NAMESPACES).text
            record_dict['about'] = e_about
            record_dict['datemodified'] = datetime.datetime.strptime(e_datemodified, '%Y-%m-%dT%H:%M:%S.%f')
            record_dict['elem'] = e
            zdb_entries[e_about] = record_dict

logger.info(f'Dump beinhaltet {len(zdb_entries)} Einträge')

'''
Über OAI alle Records abrufen, Abgleich des Modification-Datums über //dcterms:modified
'''

oai_records = {}

URL = BASEURL + "verb=ListRecords" + PREFIX + SET + FROM

listsize = getListSize(URL)

logger.info(f'{listsize} aktualisierte Records auf der OAI Schnittstelle ({URL})')

resumptiontoken = getOAIrecords(URL)[0]

# Generator und andere Nutzung von tqdm damit wir für den while-loop eine Progressbar bekommen

def generator(resumptiontoken):
    records = 0
    while resumptiontoken:
        try:
            resumptiontoken, numberofcompletedrecords = getOAIrecords(BASEURL + 'verb=ListRecords&resumptionToken=' + resumptiontoken)
        except Exception as e:
            logger.warning(e)
        else:
            records += numberofcompletedrecords
            yield

pbar = tqdm(desc='OAI Harvesting', dynamic_ncols=True, total=int(listsize))
for _ in generator(resumptiontoken):
    pbar.update(50)
pbar.close()

logger.info(f'{len(oai_records)} Datensätze über OAI bekommen')

numberofupdatedrecords = 0
numberofnewrecords = 0

logger.info(f"Gleiche OAI und ZDB Einträge ab und aktualisiere Daten")

for e in tqdm(oai_records, desc='Abgleich', dynamic_ncols=True):
    try:
        zdb_entries[e]
    except:
        zdb_entries[e] = oai_records[e]
        logger.debug(f"{e} neu hinzugefügt")
        numberofnewrecords += 1
    else:
        if zdb_entries[e]['datemodified'] < oai_records[e]['datemodified']:
            del zdb_entries[e]
            zdb_entries[e] = oai_records[e]
            logger.debug(f"{e} aktualisiert")
            numberofupdatedrecords += 1

logger.info(f"Insgesamt {numberofupdatedrecords} aktualisiert")
logger.info(f"Insgesamt {numberofnewrecords} neu hinzugefügt")

newRootElement = etree.Element('{http://www.w3.org/1999/02/22-rdf-syntax-ns#}RDF')
for e in tqdm(zdb_entries, desc='XML Erstellung', dynamic_ncols=True):
    elem = zdb_entries[e]['elem']
    newRootElement.append(elem)

logger.info("Dump erstellt")

with Halo(text='Clean up Dump', spinner='dots'):
    tree = etree.ElementTree(newRootElement)
    # etree.cleanup_namespaces(tree, top_nsmap=NAMESPACES, keep_ns_prefixes=['rdf'])
logger.info(f"Namespaces korrigiert und LXML Tree geschrieben")

newdumpname = f"zdb_lds_{datetime.datetime.today().strftime('%Y%m%d')}.rdf.gz"
with Halo(text='Schreibe Dump', spinner='dots'):
    with gzip.open(newdumpname, 'wb') as f:
        tree.write(f, encoding="UTF-8", xml_declaration=True)

logger.info(f"Aktualisierten Dump in Datei {newdumpname} geschrieben")


