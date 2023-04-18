"""
Load the cache of notes
    representing the display values for 
    GO annotation extensions (VOC_Evidence_Property).

12/21/2022      lec
        wts2-993/In some cases, identifiers are not resolving to linkable text in GO annotations. See the annotations for Slc30a3.
        changed:  batchSize from 1000 to 250000
        added:  testCounts() to test Slc30a3
"""

from optparse import OptionParser
import os
import tempfile
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

VOCAB_TERM_MGITYPE_KEY = 13
MARKER_MGITYPE_KEY = 2

# current date
CDATE = mgi_utils.date("%m/%d/%Y")

# use mgd_dbo
CREATEDBY_KEY = 1001

# temp table for joining against ACC_Accession
TEMP_ID_TABLE = "tmp_annot_id"

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
    Read command line input
    returns options
    """

    parser = OptionParser(usage=USAGE)
    
    parser.add_option("-S", dest="dbServer")
    parser.add_option("-D", dest="dbName")
    parser.add_option("-U", dest="dbUser")
    parser.add_option("-P", dest="passwordFile")
    parser.add_option("-K", dest="evidenceKey")

    (options, args) = parser.parse_args()

    if options.dbServer or options.dbName \
        or options.dbUser or options.passwordFile:
        
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

def _queryAnnotExtensions(evidenceKey=None,
                         limit=None,
                         offset=None):
    """
    Query all the annotation extensions 
        (optionally uses evidenceKey)
        (optionally uses limit + offset for batch processing)
    """
    
    # get the correct _propertyterm_keys and _evidenceterm_keys for annotation extensions
    extensionProcessor = go_annot_extensions.Processor()
    propertyTermKeys = extensionProcessor.querySanctionedPropertyTermKeys()
    evidenceTermKeys = extensionProcessor.querySanctionedEvidenceTermKeys()
    
    propertyTermKeyClause = ",".join([str(k) for k in propertyTermKeys])
    evidenceTermKeyClause = ",".join([str(k) for k in evidenceTermKeys])
    
    # optional evidenceKey clause
    evidenceKeyClause = ""
    if evidenceKey:
        evidenceKeyClause = "and ve._annotevidence_key = %s" % evidenceKey
        
    # optional limit, offset
    limitClause = ""
    if limit:
        limitClause = "limit %s offset %s" % (limit, offset)
            
    #        and va._object_key = 105
    #        and ve._refs_key = 74370
    query = '''
        select vep.*
        from voc_annot va, voc_evidence ve, voc_evidence_property vep
        where va._annottype_key = 1000
            and va._annot_key = ve._annot_key
            and ve._evidenceterm_key in (%s)
            and ve._annotevidence_key = vep._annotevidence_key
            and vep._propertyterm_key in (%s)
            and vep.value != ''
        %s
        order by ve._annotevidence_key, vep.value
        %s
    ''' % (evidenceTermKeyClause, \
        propertyTermKeyClause, \
        evidenceKeyClause,\
        limitClause \
    )
    
    #print(query)
    results = db.sql(query, "auto")
    
    for r in results:
        r['value'] = extensionProcessor.processValue(r['value'])
    
    return results

def _createTempIDTable(properties):
    """
    Create a temp table TEMP_ID_TABLE with the current properties
    for joining against ACC_Accession to build maps
        between IDs and display values
    """
    
    # setup a temp table to enable querying
    #    ACC_Accession via join
    
    dropTempTable = '''
        drop table if exists %s
    ''' % TEMP_ID_TABLE
    db.sql(dropTempTable, None)
    
    createTempTable = '''
        create temp table %s (
           id text NOT NULL
        )
    ''' % TEMP_ID_TABLE
    #print(createTempTable)
    db.sql(createTempTable, None)
    
    # write a BCP file to insert into temp table
    temp = tempfile.NamedTemporaryFile(mode="w")
    try:
        for property in properties:
            id = property['value']
            temp.write("%s\n" % (id) )
        temp.seek(0)
        db.bcp(temp.name, TEMP_ID_TABLE, schema=None)
        
    finally:
        temp.close()
            
    indexTempTable = '''
        create index %s_id_idx on %s (id)
    ''' % (TEMP_ID_TABLE, TEMP_ID_TABLE)
    db.sql(indexTempTable, None)
    
def _queryTermIDMap():
    """
    Query and build a {termID : term} map based on
        the 'value' inside each property
        
    Assumes TEMP_ID_TABLE has been populated
    """
    termIDMap = {}
    
    query = '''
    select id.id,
    term.term
    from %s id
    join acc_accession acc on
        acc.accid = id.id
        and _mgitype_key = %d
    join voc_term term on
        term._term_key = acc._object_key
    order by preferred
    ''' % (TEMP_ID_TABLE, VOCAB_TERM_MGITYPE_KEY)
    #print(query)
    results = db.sql(query, 'auto')
    
    for result in results:
        termIDMap[result['id']] = result['term']
    
    return termIDMap
    
def _queryMarkerIDMap():
    """
    Query and build a {markerID : symbol} map based on
        the 'value' inside each property
        
    Assumes TEMP_ID_TABLE has been populated
    """
    markerIDMap = {}

    query = '''
    select id.id,
    mrk.symbol
    from %s id
    join acc_accession acc on
        acc.accid = id.id
        and _mgitype_key = %d
    join mrk_marker mrk on
        mrk._marker_key = acc._object_key
    order by preferred
    ''' % (TEMP_ID_TABLE, MARKER_MGITYPE_KEY)
    #print(query)
    results = db.sql(query, 'auto')
    
    for result in results:
        markerIDMap[result['id']] = result['symbol']
        
    return markerIDMap
   
def _queryProviderLinkMap():
    """
    Query and build an {acc_actualdb.name : url} map for
        linking annotation extensions
        
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
    #print(query)
    results = db.sql(query, 'auto')
    
    for result in results:
        providerLinkMap[result['name']] = result['url']
    
    return providerLinkMap
    
### Business Logic Functions ###
def transformProperties(properties,
                        termIDMap={},
                        markerIDMap={},
                        providerLinkMap={}):
    """
    Transform the properties into their display values
        using provided termIDMap {id:term},
            and markerIDMap {id:symbol},
            and providerLinkMap {actualdb.name : urlpattern} -- for external urls
    
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
    
    for property in properties:
        value = property['value']

        ### IDs that we link and map to voc terms ###
        
        if GO_regex.match(value):
            id = value
            if id in termIDMap:
                term = termIDMap[id]
                # link GO ID to GO term detail
                value = makeNoteTag(id, term, 'GO')
                
        elif EMAPA_regex.match(value):
            id = value
            if id in termIDMap:
                term = termIDMap[id]
                # link EMAPA ID to EMAPA term detail
                value = makeNoteTag(id, term, 'EMAPA')
                
        elif CL_regex.match(value) and 'Cell Ontology' in providerLinkMap:
            id = value
            if id in termIDMap:
                term = termIDMap[id]
                # link form of ID has an underscore
                linkValue = id.replace(':','_')
                url = providerLinkMap['Cell Ontology'].replace('@@@@', linkValue)
                value = makeNoteTag(url, term)
                
        ### IDs that we link and map to marker symbols ###
                
        elif MGI_regex.match(value):
            # all MGI IDs should be a mouse marker
            id = value
            if id in markerIDMap:
                symbol = markerIDMap[id]
                # link via Marker detail
                value = makeNoteTag(id, symbol, 'Marker')
                
        ### IDs that we link, but do not map ###       
        
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
            pass
            #print "Could not map ID = %s" % value
        
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

def _writeToBCPFile(properties, 
                    noteFile,
                    startingKey):
    """
    Write the properties to the output files 
        noteFile for MGI_Note
        increment _note_key using startingKey
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

def process(evidenceKey=None):
    """
    Process the cache load
    Drop/reloads 'GO Property Display' notes 
        (either all, or for specific evidenceKey)
    """

    if evidenceKey:
        updateSingleEvidence(evidenceKey)
    
    else:
        updateAll()
        
def updateSingleEvidence():
    """
    Update single evidence record's annotation extensions
    """
    
    # Only if the EI needs this in the future will we add it
    raise Exception("Not Implemented")
    
def updateAll():
    """
    Update all the annotation extension display notes
    """
    
    # drop existing notes
    cmd = ''' delete from mgi_note where _notetype_key = %d ''' % DISPLAY_NOTE_TYPE_KEY
    db.sql(cmd, None)
    
    # get _note_key to use for inserts
    results = db.sql(''' select nextval('mgi_note_seq') as maxKey ''', 'auto')
    startingNoteKey = results[0]['maxKey']
    
    # begin batch processing
    batchSize = 250000
    offset = 0
    properties = _queryAnnotExtensions(limit=batchSize, offset=offset)
    providerLinkMap = _queryProviderLinkMap()
    
    noteFile = open(NOTE_BCP_FILE, 'w')
    
    try:
        while properties:
            # setup the lookups for IDs to display values
            _createTempIDTable(properties)
            termIDMap = _queryTermIDMap()
            markerIDMap = _queryMarkerIDMap()
            
            # transform the properties to their display/links
            properties = transformProperties(properties, termIDMap, markerIDMap, providerLinkMap)
            
            # write BCP files
            _writeToBCPFile(properties, noteFile, startingNoteKey)

            # fetch new batch of properties
            startingNoteKey += batchSize
            offset += batchSize
            properties = _queryAnnotExtensions(limit=batchSize, offset=offset)
    
    finally:
        noteFile.close()
    
    # insert the new data    
    db.bcp(NOTE_BCP_FILE, 'MGI_Note')
    db.sql(''' select setval('mgi_note_seq', (select max(_Note_key) from MGI_Note)) ''', None)
    db.commit()

def testCounts():
    # run this if you want to test the counts for Slc30a3 (42422)

    noteSQL = '''
    select n.*
    from mgi_note n, voc_evidence e, voc_evidence_property p, voc_annot a 
    where a._annottype_key = 1000
    and a._object_key = 42422
    and a._annot_key = e._annot_key
    and e._annotevidence_key = p._annotevidence_key
    and p._evidenceproperty_key = n._object_key
    and n._notetype_key = 1045
    order by n.note
    '''
    results = db.sql(noteSQL, 'auto')
    print('count of notes for Slc30a3: ', str(len(results)))

    annotSQL = '''
    select t.term, p.value
    from voc_evidence e, voc_evidence_property p, voc_annot a, voc_term t
    where a._annottype_key = 1000
    and a._object_key = 42422
    and a._annot_key = e._annot_key
    and e._annotevidence_key = p._annotevidence_key
    and p._propertyterm_key = t._Term_key
    and p._propertyterm_key in (10995953,10995919,10995920,10995922,10995925,10995928,10995931,10995932,10995933,10995934,10995935,10995936,10995937,10995938,10995940,10995941,10995945,10995946,10995947,10995949,10995950,10995951,10995952,10995960,10995963,10995964,10995965,10995966,10995967,10995968,10995969,10995970,10995971,10995972,10995973,10995974,10995975,10995976,10995977,10995978,10995979,10995980,10995981,10995982,10995983,10995984,10995985,10995986,10995987,10995988,10995989,10995990,10995991,10995992,10995993,13946898,15336083,15336084,16071180,17863564,19366754,19366755,19788375,19788376,21040292,26967043,27897499,29174556,29174557,30059450,32167776,35216336,40435559,40435560,40435561,40435562,40435563,40975434,52719963,67491796,67993660,68424959,69147444,91431505,91431507,91431508,91431509,100803635,100803636,10995929,10995921,10995923,10995924,10995959,12559890,13040278,19366756,40435555,40435556,40435557,40435558,40975433,40975435,40975436,40975437,10995926,10995927,10995930,10995942,10995943,10995944,10995954,10995955,10995956,10995957,10995958,10995961,10995962,83442724,91431506,91794702)
    order by p.value
    '''
    results = db.sql(annotSQL, 'auto')
    print('count of annotation for Slc30a3: ', str(len(results)))
    
if __name__ == "__main__":
    
    # process using command line input
    options = readCommandLine()
    
    db.useOneConnection(1)
    db.sql('start transaction', None)
    
    process(evidenceKey=options.evidenceKey)
    db.commit()

    #testCounts()
    #db.commit()
    
