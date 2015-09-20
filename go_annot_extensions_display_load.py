#!/usr/local/bin/python
"""
Load the cache of notes
    representing the display values for 
    GO annotation extensions (VOC_Evidence_Property).
"""

from optparse import OptionParser
import os

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

# current date
CDATE = mgi_utils.date("%m/%d/%Y")

# use mgd_dbo
CREATEDBY_KEY = 1001


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
    
    
### Business Logic Functions ###
def transformProperties(properties):
    """
    Transform the properties into their display values
    returns [ {'displayNote', '_evidenceproperty_key'}, ]
    """
    
    transformed = []
    
    for property in properties:
        
        value = property['value']
        
        # transform the value
        value = "\\\\Link(#|%s|)" % value
        
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
        
            properties = transformProperties(properties)
            
            _writeToBCPFile(properties, noteFile, chunkFile, startingNoteKey)
            
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
    
    
