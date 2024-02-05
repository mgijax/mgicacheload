'''
#
# Purpose:
#
# Create bcp file for GXD_Expression cache table
#
# Usage:
#       gxdexpression.py -Sserver -Ddbname -Uuser -Ppasswordfile -Kassaykey
#
#       if assaykey == 0, then create BCP file for all assays
#       if assaykey > 0, then update cache for only the assay specified by key
#               
# Processing:
#
# History
#
# 07/8/2022     lec
#       - wts2-935/GXD_Expression cache load: bug in calculating isrecombinase (has_driver)
#
# 09/21/2021    sc
#       - YAKS project, expression cell type annotation
#         add cell type to cache
#
# 02/05/2015    kstone
#       - Initial add. Meant to replace stored procedure
#
#
'''

import sys
import os
import getopt
import mgi_utils
import db

#
# when the PWI calls this script, it is *not* sourcing ./Configuration
#

try:
    outDir = os.environ['MGICACHEBCPDIR']

except:
    outDir = ''
    db.setTrace()

### Constants ###

COLDL = '|'
LINEDL = '\n'
TABLE = 'GXD_Expression'

# current date
CDATE = mgi_utils.date("%m/%d/%Y")

# BCP file to write to in full laod mode
BCP_FILENAME = outDir + '/%s.bcp' % TABLE

# number of assays to process at a time in full load mode
ASSAY_BATCH_SIZE = 1000

CACHE_FIELDS = [
        ('_expression_key','%s'),
        ('_assay_key','%s'),
        ('_refs_key','%s'),
        ('_assaytype_key','%s'),
        ('_genotype_key','%s'),
        ('_marker_key','%s'),
        ('_emapa_term_key','%s'),
        ('_celltype_term_key','%s'),
        ('_stage_key','%s'),
        ('_specimen_key','%s'),
        ('_gellane_key','%s'),
        ('resultNote','\'%s\''),
        ('expressed','%s'),
        ('strength','\'%s\''),
        ('age','\'%s\''),
        ('agemin','%s'),
        ('agemax','%s'),
        ('isrecombinase','%s'),
        ('isforgxd','%s'),
        ('hasimage','%s')
]

# order of fields
INSERT_SQL = 'insert into GXD_Expression (%s) values (%s)' % \
        (
        ','.join([x[0] for x in CACHE_FIELDS]),
        ','.join([x[1] for x in CACHE_FIELDS])
)

### Methods ###

def showUsage():
        '''
        #
        # Purpose: Displays the correct usage of this program and exits
        #
        '''
 
        usage = 'usage: %s\n' % sys.argv[0] + \
                '-S server\n' + \
                '-D database\n' + \
                '-U user\n' + \
                '-P password file\n' + \
                '-K assay key\n'

        sys.stderr.write(usage)
        sys.exit(1)

def parseCommandArgs():
        """
        Reads in command line args, prints usage if necessary
        if successful, inits db and returns assayKey
        """

        try:
                optlist, args = getopt.getopt(sys.argv[1:], 'S:D:U:P:K:')
        except:
                showUsage()

        server = None
        database = None
        user = None
        password = None
        assayKey = None

        for opt in optlist:
                if opt[0] == '-S':
                        server = opt[1]
                elif opt[0] == '-D':
                        database = opt[1]
                elif opt[0] == '-U':
                        user = opt[1]
                elif opt[0] == '-P':
                        password = str.strip(open(opt[1], 'r').readline())
                elif opt[0] == '-K':
                        assayKey = int(opt[1])
                else:
                        showUsage()

        if server is None or \
           database is None or \
           user is None or \
           password is None or \
           assayKey is None:
                showUsage()

        db.set_sqlLogin(user, password, server, database)

        return assayKey

def process(assayKey):
        """
        processes cache based on assayKey argument
        if assayKey == 0 we create full BCP File
        else we live update the cache for one assay
        """
        db.useOneConnection(1)

        # determine type of load
        if assayKey == 0:
                createFullBCPFile()
        else:
                updateSingleAssay(assayKey)

        db.useOneConnection(0)

### Shared Methods (for any type of load) ###

def _fetchMaxExpressionKey():
        """
        Return the current max _expression_key in cache
        """
        return db.sql('''select max(_expression_key) as maxkey from %s''' % TABLE, 'auto')[0]['maxkey'] or 1

def _fetchMaxAssayKey():
        """
        Return the current max _assay_key in gxd_assay
        """
        return db.sql('''select max(_assay_key) as maxkey from gxd_assay''', 'auto')[0]['maxkey'] or 1

def _fetchInsituResults(assayKey=None, startKey=None, endKey=None):
        """
        Load Insitu results from DB
        returns results
        """
        
        # first fetch insitu data from DB
        where = ''
        if startKey != None and endKey != None:
                where = 'where a._assay_key >= %s and a._assay_key < %s' % (startKey, endKey)

        elif assayKey:
                where = 'where a._assay_key = %d' % assayKey

        insituSql = '''
        select 
                a._assay_key,
                a._refs_key,
                a._assaytype_key,
                s._genotype_key,
                a._marker_key,
                irs._emapa_term_key,
                ct._celltype_term_key,
                irs._stage_key,
                strength.term as strength,
                s.age,
                s.agemin,
                s.agemax,
                s._specimen_key,
                s.sex,
                ir._result_key,
                trim(ir.resultNote) as resultNote,
                ip._imagepane_key,
                i._image_key,
                i.xDim as image_xdim,
                reporter.term reportergene,
                exists (select 1 from mgi_relationship mr
                        join gxd_allelegenotype gag
                        on gag._allele_key = mr._object_key_1
                        and mr._category_key = 1006
                        where gag._genotype_key = s._genotype_key) has_driver
        from gxd_assay a 
                join
                gxd_specimen s on
                        s._assay_key = a._assay_key
                join
                gxd_insituresult ir on
                        ir._specimen_key = s._specimen_key
                join
                voc_term strength on
                        strength._term_key = ir._strength_key
                join
                gxd_isresultstructure irs on
                        irs._result_key = ir._result_key
                left outer join
                gxd_insituresultimage iri on
                        iri._result_key = ir._result_key
                left outer join
                img_imagepane ip on
                        ip._imagepane_key = iri._imagepane_key
                left outer join
                img_image i on 
                        i._image_key = ip._image_key
                left outer join
                voc_term reporter on
                        reporter._term_key = a._reportergene_key
                left outer join
                gxd_isresultcelltype ct on
                         ct._result_key = ir._result_key
        %(where)s
        ''' % {'where':where}

        results = db.sql(insituSql, 'auto')

        #print("got %d results" % len(results))
        return results

def _fetchGelResults(assayKey=None, startKey=None, endKey=None):
        """
        Load Gel (Blot) results from DB returns results
        """
        
        # first fetch gel data from DB
        where = ''
        if startKey != None and endKey != None:
                where =  'where a._assay_key >= %s and a._assay_key < %s' % (startKey, endKey)

        elif assayKey:
                where = 'where a._assay_key = %d' % assayKey

        gelSql = '''
        select 
                a._assay_key,
                a._refs_key,
                a._assaytype_key,
                gl._genotype_key,
                a._marker_key,
                gls._emapa_term_key,
                null as _celltype_term_key,
                gls._stage_key,
                strength.term as strength,
                gl.age,
                gl.agemin,
                gl.agemax,
                gl._gellane_key,
                gl.sex,
                gb._gelband_key,
                ip._imagepane_key,
                i._image_key,
                i.xDim as image_xdim,
                reporter.term reportergene
        from gxd_assay a 
                join
                gxd_gellane gl on (
                        gl._assay_key = a._assay_key
                        and exists (select 1 from voc_term t where gl._gelcontrol_key = t._term_key and t.term = 'No')
                ) join
                gxd_gellanestructure gls on
                        gls._gellane_key = gl._gellane_key
                join
                gxd_gelband gb on
                        gb._gellane_key = gl._gellane_key
                join
                voc_term  strength on
                        strength._term_key = gb._strength_key
                left outer join
                img_imagepane ip on
                        ip._imagepane_key = a._imagepane_key
                left outer join
                img_image i on 
                        i._image_key = ip._image_key
                left outer join
                voc_term reporter on
                        reporter._term_key = a._reportergene_key
        %s      
        ''' % (where)

        results = db.sql(gelSql, 'auto')
        #print("got %d results" % len(results))

        return results

def groupResultsBy(dbResults, columns):
        """
        groups the database results by specified list of columns
        returns dictionary of columns to results
        """

        resultMap = {}

        # group database results by cache uniqueness
        for dbResult in dbResults:
                if len(columns) == 1:
                    key = dbResult[columns[0]]
                else:
                    key = tuple(dbResult[k] for k in columns)
                resultMap.setdefault(key, []).append(dbResult)

        return resultMap

def mergeInsituResults(dbResults):
        """
        groups the results from database by uniqueness of cache fields,
        returns list result groups
        """
        return list(groupResultsBy(dbResults, ['_specimen_key','_emapa_term_key', '_stage_key', '_celltype_term_key']).values())

def mergeGelResults(dbResults):
        """
        groups the results from database by uniqueness of cache fields,
        returns list result groups
        """
        return list(groupResultsBy(dbResults, ['_gellane_key','_emapa_term_key', '_stage_key', '_celltype_term_key']).values())

def generateCacheResults(isFull, dbResultGroups, assayResultMap):
        """
        transforms groups of database results
        returns list of cache records
        """

        results = []
        for group in dbResultGroups:

                # pick one row to represent this cache result
                rep = group[0]

                allResultsForAssay = assayResultMap[rep['_assay_key']]

                # compute extra cache fields

                expressed, allstrength = computeExpressedFlag(group)
                isforgxd = computeIsForGxd(group)
                isrecombinase = computeIsRecombinase(group)

                # compute fields associated with entire assay
                hasimage = computeHasImage(allResultsForAssay)

                # check specimen key
                resultNote = ''

                try:
                    _specimen_key = rep['_specimen_key']
                    if _specimen_key is not None:
                        if rep['resultNote'] is not None:
                            resultNote = rep['resultNote']
                            resultNote = resultNote.replace('\\', '\\\\')
                            resultNote = resultNote.replace('#', '\#')
                            resultNote = resultNote.replace('?', '\?')
                            resultNote = resultNote.replace('\r', '\\r')
                            resultNote = resultNote.replace('\n', '\\n')
                            resultNote = resultNote.replace('|', '\|')
                            resultNote = resultNote.replace("'s", "''s")
                except:
                    if isFull == 1:
                        _specimen_key = ''
                    else:
                        _specimen_key = 'null'

                # check gellane key
                try:
                    _gellane_key = rep['_gellane_key']
                except:
                    if isFull == 1:
                        _gellane_key = ''
                    else:
                        _gellane_key = 'null'

                agemin = rep['agemin']
                agemax = rep['agemax']
                if agemin == None:
                        agemin = '-1'
                if agemax == None:
                        agemax = '-1'

                results.append([
                        rep['_assay_key'],
                        rep['_refs_key'],
                        rep['_assaytype_key'],
                        rep['_genotype_key'],
                        rep['_marker_key'],
                        rep['_emapa_term_key'],
                        rep['_celltype_term_key'],
                        rep['_stage_key'],
                        _specimen_key,
                        _gellane_key,
                        resultNote,
                        expressed,
                        allstrength,
                        rep['age'],
                        agemin,
                        agemax,
                        isrecombinase,
                        isforgxd,
                        hasimage
                ])
                        
        return results

def computeExpressedFlag(dbResults):
        """
        compute an expressed flag
        based on a group of database results    
        """

        expressed = 0
        strengths = []

        for r in dbResults:
                if r['strength'] not in ['Absent', 'Not Applicable']:
                        expressed = 1
                if r['strength'] not in strengths:
                        strengths.append(r['strength'])

        return expressed, ','.join(strengths)

def computeIsForGxd(dbResults):
        """
        compute an isforgxd flag
        based on a group of database results    
        """
        
        if dbResults and dbResults[0]['_assaytype_key'] not in [10,11]:
                return 1

        return 0

def computeIsRecombinase(dbResults):
        """
        compute an isrecombinase flag
        based on a group of database results    
        """

        if dbResults:
                if dbResults[0]['_assaytype_key'] in [10,11]:
                        return 1
                
                if dbResults[0]['_assaytype_key'] == 9 and dbResults[0]['has_driver']:
                        return 1

        return 0

def computeHasImage(dbResults):
        """
        compute a hasimage flag
        based on a group of database results    
        """

        for r in dbResults:
                if r['_imagepane_key'] and r['_image_key'] and r['image_xdim']:
                        return 1
        return 0

### Full Load Processing Methods ###

def createFullBCPFile():
        """
        Creates the BCP file for a full reload of the cache
        ( This file is used by external process to load cache )
        """

        # clear any previous bcp file
        fp = open(BCP_FILENAME, 'w')
        fp.close()

        maxAssayKey = _fetchMaxAssayKey()

        # batches of assays to process at a time
        batchSize = ASSAY_BATCH_SIZE

        numBatches = int((maxAssayKey / batchSize) + 1)

        startingCacheKey = 1
        for i in range(numBatches):
                startKey = i * batchSize
                endKey = startKey + batchSize

                #print("processing batch of _assay_keys %s to %s" % (startKey, endKey))

                # get insitu results
                dbResults = _fetchInsituResults(startKey=startKey, endKey=endKey)
                #print('createFullBCPFile \n %s' % dbResults)
                resultGroups = mergeInsituResults(dbResults)
                assayResultMap = groupResultsBy(dbResults, ['_assay_key'])
                
                # get gel results
                dbResults = _fetchGelResults(startKey=startKey, endKey=endKey)
                resultGroups.extend(mergeGelResults(dbResults))
                assayResultMap.update(groupResultsBy(dbResults, ['_assay_key']))

                # use groups of DB results to compute cache columns
                # and create the actual cache records
                results = generateCacheResults(1, resultGroups, assayResultMap)
                # write/append found results to BCP file
                _writeToBCPFile(results, startingKey=startingCacheKey)

                startingCacheKey += len(results)

def _writeToBCPFile(results, startingKey=1):
        """
        Write cache results to BCP file
        """

        fp = open(BCP_FILENAME, 'a')
        key = startingKey
        for result in results:
                # add expression key
                result.insert(0, key)
                
                # add creation and modification date
                result.append(CDATE)
                result.append(CDATE)

                fp.write('%s%s' % (COLDL.join([_sanitizeBCP(c) for c in result]), LINEDL) )
                key += 1

        fp.close()

def _sanitizeBCP(col):
        if col==None:
                return ''
        return str(col)
                
### Single Assay Processing Methods ###

def updateSingleAssay(assayKey):
        """
        Updates cache with all results for specified assayKey
        """

        # check for either gel data or insitu data
        # fetch the appropriate database results
        # merge them into groups by unique cache keys
        #       e.g. _result_key + _emapa_term_key + _stage_key for insitus

        dbResults = []
        resultGroups = []
        if _fetchIsAssayGel(assayKey):
                dbResults = _fetchGelResults(assayKey=assayKey)
                # group/merge the database results
                resultGroups = mergeGelResults(dbResults)
        else:
                dbResults = _fetchInsituResults(assayKey=assayKey)
                # group/merge the database results
                resultGroups = mergeInsituResults(dbResults)

        # use groups of DB results to compute cache columns
        # and create the actual cache records
        assayResultMap = {assayKey: dbResults}
        results = generateCacheResults(0, resultGroups, assayResultMap)
        
        # perform live update on found results
        _updateExpressionCache(assayKey, results)

def _fetchIsAssayGel(assayKey):
        """
        Query database to check if assay is
        a gel type assay
        """

        isgelSql = '''
            select t.isgelassay
            from gxd_assay a
                join
                gxd_assaytype t on
                        t._assaytype_key = a._assaytype_key
            where _assay_key = %s
        ''' % assayKey
        results = db.sql(isgelSql, 'auto')      
        isgel = 0
        if results:
            isgel = results[0]['isgelassay']

        return isgel

def _updateExpressionCache(assayKey, results):
        """
        Do live update on results for assayKey
        """

        db.sql('begin transaction', None)

        # delete all cache records for assayKey
        deleteSql = 'delete from %s where _assay_key = %s' % (TABLE, assayKey)
        
        db.sql(deleteSql, None)
        db.commit()

        maxKey = _fetchMaxExpressionKey()

        # insert new results
        for result in results:
                maxKey += 1
                result.insert(0, maxKey)
                insertSql = INSERT_SQL % tuple([_sanitizeInsert(c) for c in result])
                db.sql(insertSql, None)
                db.commit()
        db.sql('end transaction', None)

def _sanitizeInsert(col):
        if col==None:
                return 'NULL'
        return col

if __name__ == '__main__':

    print('%s' % mgi_utils.date())
    assayKey = parseCommandArgs()
    process(assayKey)
    print('%s' % mgi_utils.date())
