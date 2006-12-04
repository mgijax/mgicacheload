#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp file for IMG_Cache
#
# Usage:
#	imgcache.py -Uuser -Ppasswordfile -Kobjectkey
#
#	if objectkey == 0, then retrieve all images
#	if objectkey > 0, then retrieve images specified by key
#	if objectkey == -1, then retrieve images that do not have a cache record
#	if objectkey == -2, then retrieve images that have a modification date = today
#		
# Processing:
#
# History
#
# 12/04/2006	lec
#	- TR 7710
#
#
'''

import sys
import os
import getopt
import string
import db
import mgi_utils

try:
    COLDL = os.environ['COLDELIM']
    LINEDL = '\n'
    table = os.environ['TABLE']
    outDir = os.environ['MGICACHEBCPDIR']
except:
    table = 'IMG_Cache'

insertSQL = 'insert into IMG_Cache values (%s,%s,%s,%s,%s,%s,%s,%s)'

def showUsage():
	'''
	#
	# Purpose: Displays the correct usage of this program and exits
	#
	'''
 
	usage = 'usage: %s\n' % sys.argv[0] + \
		'-U user\n' + \
		'-P password file\n' + \
		'-K object key\n'

	sys.stderr.write(usage)
	sys.exit(1)
 
def process(objectKey):

	#
	# images
	#

        cmd = 'select i._Image_key, i._MGIType_key, i._Refs_key, i._ThumbnailImage_key, i.figureLabel, r.year ' + \
		'into #images ' + \
		'from IMG_Image i, BIB_Refs r ' + \
		'where i._MGIType_key = 8 ' + \
		'and i._ThumbnailImage_key is not null ' + \
		'and i._Refs_key = r._Refs_key'

	if objectKey > 0:
	    cmd = cmd + 'and i._Refs_key = %s' % (objectKey)

	# all images that don't have entries in the cache table
        elif objectKey == -1:
	    cmd = cmd + 'and not exists (select 1 from %s c where i._Image_key = c._Image_key)' % (table)

	# all images modified today

        elif objectKey == -2:
	    cmd = cmd + 'and convert(char(10), i.modification_date, 101) = convert(char(10), getdate(), 101)'

	db.sql(cmd, None)
	db.sql('create index idx1 on #images(_Image_key)', None)

	#
	# assay/marker objects
	#
	db.sql('select distinct i.*, _ObjectMGIType_key = 2, _Object_key = a._Marker_key, a._AssayType_key, sortOrder = 2 ' + \
		'into #imageassoc ' + \
		'from #images i, IMG_ImagePane p, GXD_Assay a ' + \
		'where i._Image_key = p._Image_key ' + \
		'and p._ImagePane_key = a._ImagePane_key', None)

	db.sql('insert into #imageassoc ' + \
		'select distinct i.*, 2, a._Marker_key, a._AssayType_key, sortOrder = 2 ' + \
		'from #images i, IMG_ImagePane p, GXD_Assay a, GXD_Specimen s, GXD_InSituResult r, GXD_InSituResultImage g ' + \
		'where i._Image_key = p._Image_key ' + \
		'and p._ImagePane_key = g._ImagePane_key ' + \
		'and g._Result_key = r._Result_key ' + \
		'and r._Specimen_key = s._Specimen_key ' + \
		'and s._Assay_key = a._Assay_key', None)

	db.sql('create index idx1 on #imageassoc(_Image_key)', None)
	db.sql('create index idx2 on #imageassoc(_Image_key, sortOrder, year, figureLabel)', None)

	# those with insitu assays only
	db.sql('update #imageassoc set sortOrder = 1 from #imageassoc a1 where a1._AssayType_key in (1,6,9) ' + \
		'and not exists (select 1 from #imageassoc a2 where a1._Image_key = a2._Image_key ' + \
		'and a2._AssayType_key in (2,3,4,5,8))', None)

	# those with gel assays only
	db.sql('update #imageassoc set sortOrder = 3 from #imageassoc a1 where a1._AssayType_key in (2,3,4,5,8) ' + \
		'and not exists (select 1 from #imageassoc a2 where a1._Image_key = a2._Image_key ' + \
		'and a2._AssayType_key in (1,6,9))', None)

	#
	# mgi ids for full size images
	#

        results = db.sql('select i._Image_key, a.numericPart ' + \
		'from #imageassoc i, ACC_Accession a ' + \
		'where i._Image_key = a._Object_key ' + \
		'and a._MGIType_key = 9 ' + \
		'and a._LogicalDB_key = 1 ' + \
		'and a.prefixPart =  "MGI:" ' + \
		'and a.preferred = 1 ', 'auto')

	mgifullsize = {}
        for r in results:
	    mgifullsize[r['_Image_key']] = r['numericPart']

	#
	# mgi ids for thumbnail images
	#

        results = db.sql('select i._Image_key, a.numericPart ' + \
		'from #imageassoc i, ACC_Accession a ' + \
		'where i._ThumbnailImage_key = a._Object_key ' + \
		'and a._MGIType_key = 9 ' + \
		'and a._LogicalDB_key = 1 ' + \
		'and a.prefixPart =  "MGI:" ' + \
		'and a.preferred = 1 ', 'auto')

	mgithumbnail = {}
        for r in results:
	    mgithumbnail[r['_Image_key']] = r['numericPart']

	# process all records

	results = db.sql('select * from #imageassoc order by sortOrder, _Image_key, year desc, figureLabel', 'auto')
	x = 1
	prevKey = 0

	if objectKey == 0:

	    cacheBCP = open(outDir + '/%s.bcp' % (table), 'w')

	    for r in results:

		key = r['_Image_key']

		if prevKey != key:
		    x = 1
		    prevKey = key

	        cacheBCP.write(mgi_utils.prvalue(key) + COLDL + \
			     mgi_utils.prvalue(r['_ThumbnailImage_key']) + COLDL + \
			     mgi_utils.prvalue(r['_MGIType_key']) + COLDL + \
			     mgi_utils.prvalue(r['_Object_key']) + COLDL + \
			     mgi_utils.prvalue(r['_ObjectMGIType_key']) + COLDL + \
			     mgi_utils.prvalue(r['_Refs_key']) + COLDL + \
			     mgi_utils.prvalue(mgifullsize[key]) + COLDL + \
			     mgi_utils.prvalue(mgithumbnail[key]) + COLDL + \
			     mgi_utils.prvalue(x) + LINEDL)
	        cacheBCP.flush()
		x = x + 1

	    cacheBCP.close()

        else:

	    # delete existing cache table entries

	    db.sql('delete %s ' % (table) + \
		'from #imageassoc a, %s c ' % (table) + \
		'where a._Image_key = c._Image_key', None)

	    for r in results:

		key = r['_Image_key']

#	        db.sql(insertSQL % ( \
#		    mgi_utils.prvalue(key), \
#		    mgi_utils.prvalue(jnum[key]['numericPart']), \
#		    mgi_utils.prvalue(jnum[key]['accID']), \
#	            mgi_utils.prvalue(mgi[key]['accID']), \
#		    mgi_utils.prvalue(pubmedID), \
#	            mgi_utils.prvalue(r['reviewStatus']), \
#		    mgi_utils.prvalue(r['journal']), \
#		    mgi_utils.prvalue(r['citation']), \
#		    mgi_utils.prvalue(r['short_citation']),
#		    mgi_utils.prvalue(r['isReviewArticle']),
#		    mgi_utils.prvalue(isReviewArticle)), None)

#
# Main Routine
#

print '%s' % mgi_utils.date()

try:
	optlist, args = getopt.getopt(sys.argv[1:], 'U:P:K:')
except:
	showUsage()

server = db.get_sqlServer()
database = db.get_sqlDatabase()
user = None
password = None
objectKey = None

for opt in optlist:
	if opt[0] == '-U':
		user = opt[1]
	elif opt[0] == '-P':
		password = string.strip(open(opt[1], 'r').readline())
	elif opt[0] == '-K':
		objectKey = string.atoi(opt[1])
	else:
		showUsage()

if user is None or \
   password is None or \
   objectKey is None:
	showUsage()

db.set_sqlLogin(user, password, server, database)
db.useOneConnection(1)

if objectKey == 0:
    db.set_sqlLogFunction(db.sqlLogAll)

process(objectKey)
db.useOneConnection(0)
print '%s' % mgi_utils.date()

