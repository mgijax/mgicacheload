"""
Load the cache of notes representing the display values for GO annotation extensions (VOC_Evidence_Property).

10/23/2023      lec
        wts2-1311/fl2-596/lib_py_postgres, etc.
        simplify the sql; remove temp table; remove offset/limit

12/21/2022      lec
        wts2-993/In some cases, identifiers are not resolving to linkable text in GO annotations. See the annotations for Slc30a3.
        changed:  batchSize from 1000 to 250000
        added:  testCounts() to test Slc30a3

"""

from optparse import OptionParser
import os
import re
import db
import mgi_utils
import go_annot_extensions

db.setTrace()

USAGE="""
usage: %prog [-S -D -U -P -K]
"""

### Constants ###

try:
    OUT_DIR = os.environ['MGICACHEBCPDIR']
except:
    OUT_DIR = ""
    
COLDL = '\t'
LINEDL = '\n'

NOTE_BCP_FILE = OUT_DIR + "/MGI_Note.go_annot_extensions.bcp"

# note type for annotation extension display/link
DISPLAY_NOTE_TYPE_KEY = 1045

# MGI Type key for voc_evidence_property
PROPERTY_MGITYPE_KEY = 41

# current date
CDATE = mgi_utils.date("%m/%d/%Y")

# use mgd_dbo
CREATEDBY_KEY = 1001

# External database links
# (Logical DB name, Actual DB name)
DATABASE_PROVIDERS = [
    # Cell Ontology
    ('CL','Cell Ontology'),
    # Ensembl
    ('Ensembl Gene Model','Ensembl Gene Model'),
    # PR: IDs
    ('Protein Ontology', 'Protein Ontology'),
    # UniProtKB: IDs
    ('SWISS-PROT', 'UniProt')
]

def readCommandLine():
    """
    Read command line input returns options
    """

    parser = OptionParser(usage=USAGE)
    
    parser.add_option("-S", dest="dbServer")
    parser.add_option("-D", dest="dbName")
    parser.add_option("-U", dest="dbUser")
    parser.add_option("-P", dest="passwordFile")
    parser.add_option("-K", dest="evidenceKey")

    (options, args) = parser.parse_args()

    if options.dbServer or options.dbName or options.dbUser or options.passwordFile:
        
        if not options.dbServer:
            parser.error('Need to specify -S dbServer')
            
        if not options.dbName:
            parser.error('Need to specify -D dbName')
            
        if not options.dbUser:
            parser.error('Need to specify -U dbUser')
        
        if not options.passwordFile:
            parser.error('Need to specify -P passwordFile')
            
        password = password = open(options.passwordFile, 'r').readline().strip()

        # set the server and database settings
        db.set_sqlLogin(options.dbUser, password, options.dbServer, options.dbName)
        
    else:
        # using default database options
        pass
    
    return options

### Query functions ###

def queryAnnotExtensions():
    """
    Query all the annotation extensions 
    """
    
    # get the correct _propertyterm_keys and _evidenceterm_keys for annotation extensions
    extensionProcessor = go_annot_extensions.Processor()
    propertyTermKeys = extensionProcessor.querySanctionedPropertyTermKeys()
    evidenceTermKeys = extensionProcessor.querySanctionedEvidenceTermKeys()
    
    propertyTermKeyClause = ",".join([str(k) for k in propertyTermKeys])
    evidenceTermKeyClause = ",".join([str(k) for k in evidenceTermKeys])
    
    #
    # search for all annotations where voc_evidence_property where 
    #   voc_annot.ve._evidenceterm_key matches evidenceTermKeyClause
    #   voc_evidence_property._propertyterm_key matches propertyTermKeyClause
    # and
    #   1. search for all annotations where voc_evidence_property.value matches acc_accession.accid of a vocabulary term
    #   union
    #   2. search for all annotations where voc_evidence_property.value matches acc_accession.accid of a marker
    #   union
    #   3. search for all annotations where voc_evidence_property.value does not match acc_accession.accid (1 or 2)
    #

    query = '''
        (
        select distinct a.accid, t.term, vep._evidenceproperty_key, vep.value
        from acc_accession a, voc_term t, voc_annot va, voc_evidence ve, voc_evidence_property vep
        where va._annottype_key = 1000
            and va._annot_key = ve._annot_key
            and ve._evidenceterm_key in (%s)
            and ve._annotevidence_key = vep._annotevidence_key
            and vep._propertyterm_key in (%s)
            and vep.value != ''
            and vep.value = a.accid
            and a.private = 0
            and a._mgitype_key = 13
            and a._object_key = t._term_key
        union all
        select distinct a.accid, m.symbol, vep._evidenceproperty_key, vep.value
        from acc_accession a, mrk_marker m, voc_annot va, voc_evidence ve, voc_evidence_property vep
        where va._annottype_key = 1000
            and va._annot_key = ve._annot_key
            and ve._evidenceterm_key in (%s)
            and ve._annotevidence_key = vep._annotevidence_key
            and vep._propertyterm_key in (%s)
            and vep.value != ''
            and vep.value = a.accid
            and a._mgitype_key = 2
            and a.preferred = 1
            and a._logicaldb_key = 1
            and a.prefixpart = 'MGI:'
            and a._object_key = m._marker_key
        union all
        select distinct null, null, vep._evidenceproperty_key, vep.value
        from voc_annot va, voc_evidence ve, voc_evidence_property vep
        where va._annottype_key = 1000
            and va._annot_key = ve._annot_key
            and ve._evidenceterm_key in (%s)
            and ve._annotevidence_key = vep._annotevidence_key
            and vep._propertyterm_key in (%s)
            and vep.value != ''
            and not exists (select 1 from acc_accession a, voc_term t
                where vep.value = a.accid
                and a.private = 0
                and a._mgitype_key = 13
                and a._object_key = t._term_key
                )
            and not exists (select 1 from acc_accession a, mrk_marker m
                where vep.value = a.accid
                and a._mgitype_key = 2
                and a.preferred = 1
                and a._logicaldb_key = 1
                and a.prefixpart = 'MGI:'
                and a._object_key = m._marker_key
                )
        )
        order by value
    ''' % (evidenceTermKeyClause, propertyTermKeyClause, evidenceTermKeyClause, propertyTermKeyClause, evidenceTermKeyClause, propertyTermKeyClause)
    
    results = db.sql(query, "auto")
    
    for r in results:
        r['value'] = extensionProcessor.processValue(r['value'])
    
    return results

def queryProviderLinkMap():
    """
    Query and build an {acc_actualdb.name : url} map for linking annotation extensions
    """

    providerLinkMap = {}
    
    # Query by both logical DB and actual DB to get the exact url we want to use
    logicalActualDbClause = ",".join("('%s','%s')" % (ldb, adb) for (ldb, adb) in DATABASE_PROVIDERS)
    
    query = '''
    select adb.name, adb.url
    from acc_logicaldb ldb
    join acc_actualdb adb on
        adb._logicaldb_key = ldb._logicaldb_key
    where (ldb.name, adb.name) in (%s)
    ''' % ( logicalActualDbClause)
    results = db.sql(query, 'auto')
    
    for result in results:
        providerLinkMap[result['name']] = result['url']
    
    return providerLinkMap
    
### Business Logic Functions ###
def transformProperties(properties, providerLinkMap={}):
    """
    Transform the properties into their display values using provided 
            providerLinkMap {actualdb.name : urlpattern} -- for external urls
    
    returns [ {'displayNote', '_evidenceproperty_key'}, ]
    """
    
    transformed = []

    # Regex for special provider/logical DB hanlding
    CL_regex = re.compile(r'^CL:', re.I)
    EMAPA_regex = re.compile(r'^EMAPA:', re.I)
    ENSEMBL_regex = re.compile(r'^ENSEMBL:', re.I)
    GO_regex = re.compile(r'^GO:', re.I)
    MGI_regex = re.compile(r'^MGI:', re.I)
    PRO_regex = re.compile(r'^PR:', re.I)
    UNIPROTKB_regex = re.compile(r'^UniprotKB:', re.I)
    
    #print(providerLinkMap)

    for property in properties:

        value = property['value']
        id = property['accid']
        term = property['term']

        if GO_regex.match(value):
                # link GO ID to GO term detail
                if id is not None:
                        value = makeNoteTag(value, term, 'GO')
                
        elif EMAPA_regex.match(value):
                # link EMAPA ID to EMAPA term detail
                if id is not None:
                        value = makeNoteTag(value, term, 'EMAPA')
                
        elif CL_regex.match(value) and 'Cell Ontology' in providerLinkMap:
                # link form of ID has an underscore
                linkValue = value.replace(':','_')
                url = providerLinkMap['Cell Ontology'].replace('@@@@', linkValue)
                value = makeNoteTag(url, term)
                
        elif MGI_regex.match(value):
                # link via Marker detail
                if id is not None:
                        value = makeNoteTag(value, term, 'Marker')
                
        elif ENSEMBL_regex.match(value) and 'Ensembl Gene Model' in providerLinkMap:
            # remove prefix for linking
            linkValue = value[ (value.find(':') + 1) : ]
            url = providerLinkMap['Ensembl Gene Model'].replace('@@@@', linkValue)
            value = makeNoteTag(url, value)
        
        elif PRO_regex.match(value) and 'Protein Ontology' in providerLinkMap:
             # keep the PR: prefix
            linkValue = value
            url = providerLinkMap['Protein Ontology'].replace('@@@@', linkValue)
            value = makeNoteTag(url, value)
        
        elif UNIPROTKB_regex.match(value) and 'UniProt' in providerLinkMap:
            # remove prefix for linking
            linkValue = value[ (value.find(':') + 1) : ]
            url = providerLinkMap['UniProt'].replace('@@@@', linkValue)
            value = makeNoteTag(url, value)
            
        else:
            #only use if debugging
            #print("Could not map ID = %s" % value)
            pass
        
        transformed.append({
         'displayNote': value,
         '_evidenceproperty_key': property['_evidenceproperty_key']   
        })
    
    return transformed    

def makeNoteTag(url, display, type='Link'):
    """
    return an MGI note tag string
    """ 
    return '\\\\%s(%s|%s|)' % (type, url, display)

### Functions to perform the updates ###    

def writeToBCPFile(properties, noteFile, startingKey):
    """
    Write the properties to the output files noteFile for MGI_Note increment _note_key using startingKey
    """

    key = startingKey
    for property in properties:
        # write MGI_Note
        note = [key,
                property['_evidenceproperty_key'],
                PROPERTY_MGITYPE_KEY,
                DISPLAY_NOTE_TYPE_KEY,
                property['displayNote'],
                CREATEDBY_KEY,
                CREATEDBY_KEY,
                CDATE,
                CDATE
                ]
        noteFile.write('%s%s' % (COLDL.join([str(c) for c in note]), LINEDL) )
        key += 1

def process():
    """
    Process the cache load
    Drop/reloads 'GO Property Display' notes 
        (either all, or for specific evidenceKey)
    """

    # drop existing notes
    db.sql('delete from mgi_note where _notetype_key = %d' % DISPLAY_NOTE_TYPE_KEY, None)
    
    # get _note_key to use for inserts
    results = db.sql(''' select nextval('mgi_note_seq') as maxKey ''', 'auto')
    startingNoteKey = results[0]['maxKey']
    
    properties = queryAnnotExtensions()
    providerLinkMap = queryProviderLinkMap()
    
    noteFile = open(NOTE_BCP_FILE, 'w')
    
    try:
        # transform the properties to their display/links
        properties = transformProperties(properties, providerLinkMap)
            
        # write BCP files
        writeToBCPFile(properties, noteFile, startingNoteKey)

    finally:
        noteFile.close()
    
    # insert the new data    
    db.bcp(NOTE_BCP_FILE, 'MGI_Note', setval="mgi_note_seq", setkey="_note_key")
    db.commit()

if __name__ == "__main__":
    
    # process using command line input
    options = readCommandLine()
    process()
    db.commit()
    
