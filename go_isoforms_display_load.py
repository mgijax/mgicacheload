"""
Load the cache of notes
    representing the display values for 
    GO isoforms (VOC_Evidence_Property).
"""

from optparse import OptionParser
import os
import re

import db
import mgi_utils
import go_isoforms

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

# note type for GO isoform display/link
DISPLAY_NOTE_TYPE_KEY = 1046
# MGI Type key for voc_evidence_property
PROPERTY_MGITYPE_KEY = 41

VOCAB_TERM_MGITYPE_KEY = 13
MARKER_MGITYPE_KEY = 2

# current date
CDATE = mgi_utils.date("%m/%d/%Y")

# use mgd_dbo
CREATEDBY_KEY = 1001


# External database links
# (Logical DB name, Actual DB name)
DATABASE_PROVIDERS = [
    # EMBL: IDs
    ('Sequence DB', 'EMBL'),
    # NCBI: and RefSeq: IDs
    ('RefSeq', 'RefSeq'),
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
    
    # get the correct _propertyterm_keys 
    #    for annotation isoforms
    isoformProcessor = go_isoforms.Processor()
    propertyTermKeys = isoformProcessor.querySanctionedPropertyTermKeys()
    
    propertyTermKeyClause = ",".join([str(k) for k in propertyTermKeys])
    
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
        where vep._propertyterm_key in (%s)
        %s
        %s
    ''' % ( propertyTermKeyClause, \
        evidenceKeyClause,\
        limitClause \
    )
    
    results = db.sql(query, "auto")
    
    for r in results:
        
        # set value to multiple values list
        r['value'] = isoformProcessor.processValue(r['value'])
    
    return results




def _queryProviderLinkMap():
    """
    Query and build an {acc_actualdb.name : url} map for
        linking isoforms
        
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
def transformProperties(properties,
                        providerLinkMap={}):
    """
    Transform the properties into their display values
        using provided termIDMap {id:term},
            and markerIDMap {id:symbol}
    
    returns [ {'displayNote', '_evidenceproperty_key'}, ]
    """
    
    transformed = []

    # Regex for special provider/logical DB hanlding
    EMBL_regex = re.compile(r'^EMBL:', re.I)
    NCBI_regex = re.compile(r'^NCBI:', re.I)
    PRO_regex = re.compile(r'^PR:', re.I)
    REFSEQ_regex = re.compile(r'^RefSeq:', re.I)
    UNIPROTKB_regex = re.compile(r'^UniprotKB:', re.I)
    
    for property in properties:
        
        values = []

        for value in property['value']:
           
            # special provider cases
            if EMBL_regex.match(value) and \
                'EMBL' in providerLinkMap:
                
                linkValue = value[ (value.find(':') + 1) : ]
                url = providerLinkMap['EMBL'].replace('@@@@', linkValue)
                value = makeNoteTag(url, value)
                
            elif NCBI_regex.match(value) and \
                'RefSeq' in providerLinkMap:
                
                linkValue = value[ (value.find(':') + 1) : ]
                url = providerLinkMap['RefSeq'].replace('@@@@', linkValue)
                value = makeNoteTag(url, value)
                
            elif PRO_regex.match(value) and \
                'Protein Ontology' in providerLinkMap:
                
                # keep the PR: prefix
                linkValue = value
                url = providerLinkMap['Protein Ontology'].replace('@@@@', linkValue)
                value = makeNoteTag(url, value)
                
            elif REFSEQ_regex.match(value) and \
                'RefSeq' in providerLinkMap:
                
                linkValue = value[ (value.find(':') + 1) : ]
                url = providerLinkMap['RefSeq'].replace('@@@@', linkValue)
                value = makeNoteTag(url, value)
                
            elif UNIPROTKB_regex.match(value) and \
                'UniProt' in providerLinkMap:
                
                linkValue = value[ (value.find(':') + 1) : ]
                url = providerLinkMap['UniProt'].replace('@@@@', linkValue)
                value = makeNoteTag(url, value)
                
    
            
            values.append(value)
        
        
        if values:
        
            value = ", ".join(values)
            
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
    cmd = '''
    delete from mgi_note
    where _notetype_key = %d
    ''' % DISPLAY_NOTE_TYPE_KEY
    db.sql(cmd, None)
    
    
    # get _note_key to use for inserts
    results = db.sql(''' select nextval('mgi_note_seq') as maxKey ''', 'auto')
    startingNoteKey = results[0]['maxKey']
    
    # begin batch processing
    batchSize = 10000
    offset = 0
    
    properties = _queryAnnotExtensions(limit=batchSize, offset=offset)
    providerLinkMap = _queryProviderLinkMap()
    
    noteFile = open(NOTE_BCP_FILE, 'w')
    
    try:
        while properties:
            
            # transform the properties to their display/links
            properties = transformProperties(properties, providerLinkMap)
            
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



if __name__ == "__main__":
    
    # process using command line input
    options = readCommandLine()
    
    db.useOneConnection(1)
    db.sql('start transaction', None)
    
    process(evidenceKey=options.evidenceKey)
    
    db.commit()
    
    
