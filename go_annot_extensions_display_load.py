#!/usr/local/bin/python
"""
Load the cache of notes
    representing the display values for 
    GO annotation extensions (VOC_Evidence_Property).
"""

from optparse import OptionParser
import os
import tempfile

import db
import mgi_utils
import go_annot_extensions


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

NOTE_BCP_FILE = OUT_DIR + "/MGI_Note.bcp"
NOTECHUNK_BCP_FILE = OUT_DIR + "/MGI_NoteChunk.bcp"

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

def _queryMaxNoteKey():
    """
    return latest usable MGI_Note._note_key
    """
    return db.sql('''select max(_note_key) as maxkey from mgi_note''', 
        'auto')[0]['maxkey'] or 1
        

def _queryAnnotExtensions(evidenceKey=None,
                         limit=None,
                         offset=None):
    """
    Query all the annotation extensions 
        (optionally uses evidenceKey)
        (optionally uses limit + offset for batch processing)
    """
    
    # get the correct _propertyterm_keys and _evidenceterm_keys 
    #    for annotation extensions
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
            
    query = '''
        select vep.*
        from voc_evidence ve
        join voc_evidence_property vep on
            vep._annotevidence_key = ve._annotevidence_key
        where ve._evidenceterm_key in (%s)
            and vep._propertyterm_key in (%s)
        %s
        %s
    ''' % (evidenceTermKeyClause, \
        propertyTermKeyClause, \
        evidenceKeyClause,\
        limitClause \
    )
    
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
    db.sql(createTempTable, None)
    
    # write a BCP file to insert into temp table
    temp = tempfile.NamedTemporaryFile()
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
    results = db.sql(query, 'auto')
    
    for result in results:
        markerIDMap[result['id']] = result['symbol']
        
    return markerIDMap
   
    
    
### Business Logic Functions ###
def transformProperties(properties,
                        termIDMap={},
                        markerIDMap={}):
    """
    Transform the properties into their display values
        using provided termIDMap {id:term},
            and markerIDMap {id:symbol}
    
    returns [ {'displayNote', '_evidenceproperty_key'}, ]
    """
    
    transformed = []
    
    for property in properties:
        
        value = property['value']
        
        if value in termIDMap:
            value = termIDMap[value]
            
        elif value in markerIDMap:
            value = markerIDMap[value]
            
        else:
            pass
            #print "Could not map ID = %s" % value
        
        # transform the value into a link
        #value = "\\Link(#|%s|)" % value
        
        transformed.append({
         'displayNote': value,
         '_evidenceproperty_key': property['_evidenceproperty_key']   
        })
    

    return transformed    


    
### Functions to perform the updates ###    

def _writeToBCPFile(properties, 
                    noteFile,
                    chunkFile,
                    startingKey):
    """
    Write the properties to the output files 
        noteFile for MGI_Note
        chunkFile for MGI_NoteChunk
        
        increment _note_key using startingKey
    """

    key = startingKey
    for property in properties:
        
        # write MGI_Note
        note = [key,
                property['_evidenceproperty_key'],
                PROPERTY_MGITYPE_KEY,
                DISPLAY_NOTE_TYPE_KEY,
                CREATEDBY_KEY,
                CREATEDBY_KEY,
                CDATE,
                CDATE
                ]
        noteFile.write('%s%s' % (COLDL.join([str(c) for c in note]), LINEDL) )
        
        
        # write MGI_NoteChunk
        notechunk = [key,
                     1,
                     property['displayNote'],
                     CREATEDBY_KEY,
                     CREATEDBY_KEY,
                     CDATE,
                     CDATE
        ]
        chunkFile.write('%s%s' % (COLDL.join([str(c) for c in notechunk]), LINEDL) )

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
    cmd = '''
    delete from mgi_note
    where _notetype_key = %d
    ''' % DISPLAY_NOTE_TYPE_KEY
    db.sql(cmd, None)
    
    
    # get _note_key to use for inserts
    startingNoteKey = _queryMaxNoteKey() + 1
    
    
    # begin batch processing
    batchSize = 10000
    offset = 0
    properties = _queryAnnotExtensions(limit=batchSize, offset=offset)
    
    noteFile = open(NOTE_BCP_FILE, 'w')
    chunkFile = open(NOTECHUNK_BCP_FILE, 'w')
    
    try:
        while properties:
            
            # setup the lookups for IDs to display values
            _createTempIDTable(properties)
            
            termIDMap = _queryTermIDMap()
            
            markerIDMap = _queryMarkerIDMap()
            
            
            # transform the properties to their display/links
            properties = transformProperties(properties, termIDMap, markerIDMap)
            
            
            # write BCP files
            _writeToBCPFile(properties, noteFile, chunkFile, startingNoteKey)
            
            
            # fetch new batch of properties
            startingNoteKey += batchSize
            offset += batchSize
            properties = _queryAnnotExtensions(limit=batchSize, offset=offset)
    
    finally:
        noteFile.close()
        chunkFile.close()
    
    
    # insert the new data    
    db.bcp(NOTE_BCP_FILE, 'MGI_Note')
    db.bcp(NOTECHUNK_BCP_FILE, 'MGI_NoteChunk')



if __name__ == "__main__":
    
    # process using command line input
    options = readCommandLine()
    
    db.useOneConnection(1)
    db.sql('start transaction', None)
    
    process(evidenceKey=options.evidenceKey)
    
    db.commit()
    
    