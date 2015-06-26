#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp file for GXD_Expression cache table
#
# Usage:
#	gxdexpression.py -Sserver -Ddbname -Uuser -Ppasswordfile -Kassaykey
#
#	if assaykey == 0, then create BCP file for all assays
#	if assaykey > 0, then update cache for only the assay specified by key
#		
# Processing:
#
# History
#
# 02/05/2015	kstone
#	- Initial add. Meant to replace Sybase stored procedure
#
#
'''

import sys
import os
import getopt
import string
import mgi_utils
import db

db.setTrace()
db.setAutoTranslate(False)
db.setAutoTranslateBE(False)

#
# when the EI calls this script, it is *not* sourcing ./Configuration
#

try:
    outDir = os.environ['MGICACHEBCPDIR']

except:
    outDir = ''

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
	('_structure_key','%s'),
	('_emaps_key','%s'),
	('_specimen_key','%s'),
	('_gellane_key','%s'),
	('expressed','%s'),
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
	Reads in command line args,
		prints usage if necessary
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
			password = string.strip(open(opt[1], 'r').readline())
		elif opt[0] == '-K':
			assayKey = string.atoi(opt[1])
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
	return db.sql('''select max(_expression_key) as maxkey from %s''' % TABLE, 
		'auto')[0]['maxkey'] or 1

def _fetchMaxAssayKey():
	"""
	Return the current max _assay_key in gxd_assay
	"""
	return db.sql('''select max(_assay_key) as maxkey from gxd_assay''', 
		'auto')[0]['maxkey'] or 1

def _initEmapsTempTable(assayKey=None):
	"""
	Create the emaps_temp table
	for mapping AD structure keys to emaps
	"""

	emapsSelect = '''
	select distinct accs._object_key as _structure_key,
                acce._object_key as _emaps_key
	'''

	emapsFrom = '''
	from acc_accession accs
        left outer join mgi_emaps_mapping em on
                em.accid=accs.accid
        left outer join acc_accession acce on
                acce.accid=em.emapsid
                and acce._mgitype_key = 13
	'''

	emapsWhere = '''
	where   accs._mgitype_key = 38
		and accs.preferred = 1
		and accs._logicaldb_key = 1
	'''
	
	emapsTemp = '''
	%s
	INTO TEMPORARY TABLE emaps_temp
	%s
	%s
	''' % (emapsSelect, emapsFrom, emapsWhere)
	if assayKey:
		emapsTemp = '''
		%s
		INTO TEMPORARY TABLE emaps_temp
		%s
		%s
		    and  exists (
			select 1 from gxd_isresultstructure irs
			join gxd_insituresult ir on
				ir._result_key=irs._result_key
			join gxd_specimen s on
				s._specimen_key=ir._specimen_key
			where irs._structure_key=accs._object_key
				and s._assay_key = %d
		    )
		UNION
		%s
		%s
		%s
		    and exists (
			select 1 from gxd_gellanestructure gls
			join gxd_gellane gl on
				gl._gellane_key=gls._gellane_key
			where gls._structure_key=accs._object_key
				and gl._assay_key = %d
		    )
		''' % (emapsSelect, emapsFrom, emapsWhere, assayKey, \
			emapsSelect, emapsFrom, emapsWhere, assayKey)
	db.sql(emapsTemp, None)
	db.sql('create index emaps_struct_idx on emaps_temp (_structure_key)', None)

def _fetchInsituResults(assayKey=None, startKey=None, endKey=None):
	"""
	Load Insitu results from DB
	returns results
	"""
	
	# first fetch insitu data from DB
	where = ''
	if startKey != None and endKey != None:
		where = 'where a._assay_key >= %s and a._assay_key < %s' % \
			(startKey, endKey)

	elif assayKey:
		where = 'where a._assay_key = %d' % assayKey

	insituSql = '''
	select 
		a._assay_key,
		a._refs_key,
		a._assaytype_key,
		s._genotype_key,
		a._marker_key,
		irs._structure_key,
		strength.strength,
		s.age,
		s.agemin,
		s.agemax,
		s._specimen_key,
		s.sex,
		ir._result_key,
		ip._imagepane_key,
		i._image_key,
		i.xDim as image_xdim,
		reporter.term reportergene,
		et._emaps_key
	from gxd_assay a 
		join
		gxd_specimen s on
			s._assay_key = a._assay_key
		join
		gxd_insituresult ir on
			ir._specimen_key = s._specimen_key
		join
		gxd_strength strength on
			strength._strength_key = ir._strength_key
		join
		gxd_isresultstructure irs on
			irs._result_key = ir._result_key
		join
		emaps_temp et on
			et._structure_key = irs._structure_key	
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
	%s
	''' % (where)

	results = db.sql(insituSql, 'auto')

	#print "got %d results" % len(results)
	return results

def _fetchGelResults(assayKey=None, startKey=None, endKey=None):
	"""
	Load Gel (Blot) results from DB
	returns results
	"""
	
	# first fetch gel data from DB
	where = ''
	if startKey != None and endKey != None:
		where =  'where a._assay_key >= %s and a._assay_key < %s' % \
			(startKey, endKey)

	elif assayKey:
		where = 'where a._assay_key = %d' % assayKey

	gelSql = '''
	select 
		a._assay_key,
		a._refs_key,
		a._assaytype_key,
		gl._genotype_key,
		a._marker_key,
		gls._structure_key,
		strength.strength,
		gl.age,
		gl.agemin,
		gl.agemax,
		gl._gellane_key,
		gl.sex,
		gb._gelband_key,
		ip._imagepane_key,
		i._image_key,
		i.xDim as image_xdim,
		reporter.term reportergene,
		et._emaps_key
	from gxd_assay a 
		join
		gxd_gellane gl on (
			gl._assay_key = a._assay_key
			and gl._gelcontrol_key = 1
		) join
		gxd_gellanestructure gls on
			gls._gellane_key = gl._gellane_key
		join
		emaps_temp et on
			et._structure_key = gls._structure_key
		join
		gxd_gelband gb on
			gb._gellane_key = gl._gellane_key
		join
		gxd_strength strength on
			strength._strength_key = gb._strength_key
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
	#print "got %d results" % len(results)

	return results

def groupResultsBy(dbResults, columns):
	"""
	groups the database results
	by specified list of columns
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
	groups the results from database
	by uniqueness of cache fields,
	returns list result groups
	"""

	return groupResultsBy(dbResults, ['_specimen_key','_structure_key']).values()

def mergeGelResults(dbResults):
	"""
	groups the results from database
	by uniqueness of cache fields,
	returns list result groups
	"""
	return groupResultsBy(dbResults, ['_gellane_key','_structure_key']).values()

def generateCacheResults(dbResultGroups, assayResultMap):
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

		expressed = computeExpressedFlag(group)
		isforgxd = computeIsForGxd(group)
		isrecombinase = computeIsRecombinase(group)

		# compute fields associated with entire assay
		hasimage = computeHasImage(allResultsForAssay)

		# check specimen key
		_specimen_key = rep.has_key('_specimen_key')  and rep['_specimen_key'] or None

		# check gellane key
		_gellane_key = rep.has_key('_gellane_key')  and rep['_gellane_key'] or None

		results.append([
			rep['_assay_key'],
			rep['_refs_key'],
			rep['_assaytype_key'],
			rep['_genotype_key'],
			rep['_marker_key'],
			rep['_structure_key'],
			rep['_emaps_key'],
			_specimen_key,
			_gellane_key,
			expressed,
			rep['age'],
			rep['agemin'],
			rep['agemax'],
			isrecombinase,
			isforgxd,
			hasimage
		])
			
	return results

def computeExpressedFlag(dbResults):
	"""
	compute an expressed flag
	based on a group of database results	
	@unittested
	"""
	for r in dbResults:
		if r['strength'] not in ['Absent', 'Not Applicable']:
			return 1
			
	return 0

def computeIsForGxd(dbResults):
	"""
	compute an isforgxd flag
	based on a group of database results	
	@unittested
	"""
	if dbResults  \
	    and dbResults[0]['_assaytype_key'] not in [10,11]:
		return 1

	return 0

def computeIsRecombinase(dbResults):
	"""
	compute an isrecombinase flag
	based on a group of database results	
	@unittested
	"""
	if dbResults:
		if dbResults[0]['_assaytype_key'] in [10,11]:
			return 1
		
		if dbResults[0]['_assaytype_key'] == 9 \
		    and dbResults[0]['reportergene'] in ['Cre', 'FLP']:
			return 1

	return 0

def computeHasImage(dbResults):
	"""
	compute a hasimage flag
	based on a group of database results	
	@unittested
	"""
	for r in dbResults:
		if r['_imagepane_key'] \
		    and r['_image_key'] \
		    and r['image_xdim']:
			return 1
	return 0

### Full Load Processing Methods ###

def createFullBCPFile():
	"""
	Creates the BCP file
	for a full reload of the cache
	( This file is used by external process to load cache )
	"""

	# clear any previous bcp file
	fp = open(BCP_FILENAME, 'w')
	fp.close()

	maxAssayKey = _fetchMaxAssayKey()

	# batches of assays to process at a time
	batchSize = ASSAY_BATCH_SIZE

	numBatches = (maxAssayKey / batchSize) + 1

	_initEmapsTempTable()

	startingCacheKey = 1
	for i in range(numBatches):
		startKey = i * batchSize
		endKey = startKey + batchSize

		print "processing batch of _assay_keys %s to %s" % (startKey, endKey)

		# get insitu results
		dbResults = _fetchInsituResults(startKey=startKey, endKey=endKey)
		resultGroups = mergeInsituResults(dbResults)
		assayResultMap = groupResultsBy(dbResults, ['_assay_key'])

		
		# get gel results
		dbResults = _fetchGelResults(startKey=startKey, endKey=endKey)
		resultGroups.extend(mergeGelResults(dbResults))
		assayResultMap.update(groupResultsBy(dbResults, ['_assay_key']))

		# use groups of DB results to compute cache columns
		# and create the actual cache records
		results = generateCacheResults(resultGroups, assayResultMap)

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
	#	e.g. _result_key + _structure_key for insitus

	_initEmapsTempTable(assayKey)

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
	results = generateCacheResults(resultGroups, assayResultMap)
	
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

	db.sql('commit transaction', None)

def _sanitizeInsert(col):
	if col==None:
		return 'NULL'
	return col



if __name__ == '__main__':

    print '%s' % mgi_utils.date()

    assayKey = parseCommandArgs()

    process(assayKey)

    print '%s' % mgi_utils.date()

