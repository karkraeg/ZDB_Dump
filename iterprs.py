# %%
from lxml import etree
import gzip
import datetime

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
zdb_entries = {}

# for event, elem in etree.iterparse(gzip.GzipFile('/Users/karl/Coding/_misc/ZDB_Dump/20220705.rdf.gz'), tag='{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description' ,events=("start", "end")):
#     if elem.getparent().tag == "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}RDF":
#                 record_dict = { 'about': '',
#                             'datemodified': '',
#                             'elem': ''}
#                 e_about = elem.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about']
#                 e_datemodified = elem.find('.//dcterms:modified', NAMESPACES).text
#                 record_dict['about'] = e_about
#                 record_dict['datemodified'] = datetime.datetime.strptime(e_datemodified, '%Y-%m-%dT%H:%M:%S.%f')
#                 record_dict['elem'] = str(elem)
#                 zdb_entries[e_about] = record_dict
# elem.clear()
# %%
for event, elem in etree.iterparse(gzip.GzipFile('/Users/karl/Coding/_misc/ZDB_Dump/RDF/zdb_lds_20220704.rdf.gz'), events=("start", "end")):
#for event, elem in etree.iterparse('/Users/karl/Coding/_misc/ZDB_Dump/smallRDFsample.rdf', events=("start", "end")):
    tag = elem.tag
    if event == 'start':
        if tag == "{http://www.w3.org/1999/02/22-rdf-syntax-ns#}Description":
            newtree = etree.Element("record")
            for child in elem:
                newtree.append(child)
            record_dict = { 'about': '',
                        'datemodified': '',
                        'elem': ''}
            try:
                e_about = elem.attrib['{http://www.w3.org/1999/02/22-rdf-syntax-ns#}about']
            except:
                pass
            else:
                try:
                    e_datemodified = newtree.find('.//dcterms:modified', NAMESPACES).text
                except:
                    e_datemodified = None

                if e_datemodified is not None:
                    record_dict['datemodified'] = datetime.datetime.strptime(e_datemodified, '%Y-%m-%dT%H:%M:%S.%f')
                else:
                    record_dict['datemodified']
                record_dict['about'] = e_about
                # convert the xml tree to string to save precoius memory ;)
                record_dict['elem'] = etree.tostring(newtree, encoding='utf8', method='xml')
                zdb_entries[e_about] = record_dict
    elem.clear()
# %%
len(zdb_entries)
#%%
reverse = etree.fromstring(zdb_entries['https://ld.zdb-services.de/resource/9-7']['elem'])
moddate = reverse.find('.//dcterms:modified', NAMESPACES)
moddate
# %%
