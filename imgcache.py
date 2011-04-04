#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp file for IMG_Cache
# 	Assay (GXD) and phenotype images that have thumbnails and are in
#	Pixel DB
#
# Usage:
#	imgcache.py -Sserver -Ddatabase -Uuser
#                   { -Ppasswordfile || -Wpassword }
#                   { -J JNumber || -Kobjectkey }
#
#	if the JNumber is given, it is converted to a reference key and
#		the script handles it as if it was called with objectkey > 0
#	if objectkey == 0, then retrieve all images
#	if objectkey > 0, then retrieve images specified by key
#	if objectkey == -1, then retrieve images that do not have a cache record
#	if objectkey == -2, then retrieve images that have a modification date = today
#		
# History
#
# 04/04/2011	lec
#	- TR 10658; added _Cache_key
#
# 11/24/2010	lec
#	- TR 10033/image class
#
# 05/27/2008 - lec
#	- fix insertSQL for assay type
#
# 04/17/2008 - jsb - altered to include phenotype images for TR8627
#
# 12/04/2006 - lec - TR 7710
#
'''

import sys
import os
import getopt
import string
import time
import db
import mgi_utils

try:
    COLDL = os.environ['COLDELIM']
    LINEDL = '\n'
    table = os.environ['TABLE']
    outDir = os.environ['MGICACHEBCPDIR']
except:
    table = 'IMG_Cache'

insertSQL = 'insert into IMG_Cache values (%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,%s,"%s",%s)'

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
		'{-P password file || -W password}\n' + \
		'{-J JNumber -K || object key}\n'

	sys.stderr.write(usage)
	sys.exit(1)


def getRefKey(jNumber):
	#
	#  Get the reference key for the J-Number.
	#
	results = db.sql('''select _Object_key from ACC_Accession 
		where accID = "%s" 
		and _MGIType_key = 1 
		and _LogicalDB_key = 1 
		and preferred = 1''' % (jNumber), 'auto')

	if len(results) == 0:
	    sys.stderr.write('J-Number "' + jNumber + '" does not exist.\n')
	    sys.stderr.write('Cannot refresh image cache.\n')
	    sys.exit(1)

	return results[0]['_Object_key']

 
def process(objectKey):

        #
        # next available primary key
        #
    
        results = db.sql('select cacheKey = max(_Cache_key) from %s' % (table), 'auto')
        for r in results:
	    nextMaxKey = r['cacheKey']

        if nextMaxKey == None:
            nextMaxKey = 0

	#
	# retrieve images that have thumbnails and are in pixel DB
	# only include Expression and Phenotype images
	#

        cmd = '''select i._Image_key, i._MGIType_key, i._ImageClass_key, 
		 i._Refs_key, i._ThumbnailImage_key, 
		 i.figureLabel, ip._ImagePane_key, ip.paneLabel, r.year, a.numericPart 
		into #images 
		from IMG_Image i, BIB_Refs r, IMG_ImagePane ip, ACC_Accession a, VOC_Term t
		where i._MGIType_key in (8, 11) 
		and i._ImageClass_key = t._Term_key
		and t.term in ('Expression', 'Phenotypes')
		and i._ThumbnailImage_key is not null 
		and i.xdim is not null 
		and i._Refs_key = r._Refs_key 
		and i._Image_key = ip._Image_key 
		and r._Refs_key = a._Object_key 
		and a._MGIType_key = 1 
		and a.prefixPart = "J:"'''

	if objectKey > 0:
	    cmd = cmd + ' and i._Refs_key = %s' % (objectKey)

	# images that don't have entries in the cache table
        elif objectKey == -1:
	    cmd = cmd + ' and not exists (select 1 from %s c where c._Image_key = i._Image_key' % (table)

	# all images modified today
        elif objectKey == -2:
	    cmd = cmd + ' and convert(char(10), i.modification_date, 101) = convert(char(10), getdate(), 101)'

	db.sql(cmd, None)
	db.sql('create index idx1 on #images(_Image_key)', None)
	db.sql('create index idx2 on #images(_ImagePane_key)', None)

	#
	# image/marker associations
	#
	# sort:  insitu assays only (1), both insitu & gel assays (2), gel assays only (3)
	#

	# add GXD gel images

	db.sql('''select distinct i.*, _ObjectMGIType_key = 2, _Object_key = a._Marker_key, 
		a._AssayType_key, t.assayType, sortOrder = 2 
		into #imageassoc 
		from #images i, GXD_Assay a, GXD_AssayType t 
		where i._ImagePane_key = a._ImagePane_key 
		and a._AssayType_key = t._AssayType_key''', None)

	# add GXD in situ images

	db.sql('''insert into #imageassoc 
		select distinct i.*, 2, a._Marker_key, a._AssayType_key, t.assayType, sortOrder = 2 
		from #images i, GXD_Assay a, GXD_AssayType t, 
		     GXD_Specimen s, GXD_InSituResult r, GXD_InSituResultImage g 
		where i._ImagePane_key = g._ImagePane_key 
		and g._Result_key = r._Result_key 
		and r._Specimen_key = s._Specimen_key 
		and s._Assay_key = a._Assay_key 
		and a._AssayType_key = t._AssayType_key''', None)

	# must allow null assay type info to accommodate pheno images

	db.sql ('alter table #imageassoc modify _AssayType_key null', None)
	db.sql ('alter table #imageassoc modify assayType null', None)

	# add pheno images

	db.sql ('''insert into #imageassoc
		select distinct i.*, 2, aa._Marker_key, null, null, sortOrder = 5
		from #images i, IMG_ImagePane_Assoc ipa, ALL_Allele aa
		where i._ImagePane_key = ipa._ImagePane_key
		and ipa._Object_key = aa._Allele_key
		and ipa._MGIType_key = 11	-- allele''', None)

	# prioritize pheno images from J:98862, leave others as sort order 5

	db.sql ('update #imageassoc set sortOrder = 4 where sortOrder = 5 and numericPart = 98862', None)

	# add indexes for performance

	db.sql('create index idx1 on #imageassoc(_Image_key)', None)
	db.sql('create index idx2 on #imageassoc(_ThumbnailImage_key)', None)
	db.sql('create index idx3 on #imageassoc(_AssayType_key)', None)
	db.sql('create index idx4 on #imageassoc(_Object_key)', None)
	db.sql('create index idx5 on #imageassoc(_Object_key, sortOrder, year, figureLabel, _Image_key)', None)

	# update sort order for those with insitu assays only (by marker)
	db.sql('''update #imageassoc set sortOrder = 1 
		from #imageassoc a1 
		where a1._AssayType_key in (1,6,9) 
		and not exists (select 1 from #imageassoc a2 where a1._Image_key = a2._Image_key
		and a1._Object_key = a2._Object_key
		and a2._AssayType_key in (2,3,4,5,8))''', None)

	# update sort order for those with gel assays only (by marker)
	db.sql('''update #imageassoc set sortOrder = 3 
		from #imageassoc a1 where a1._AssayType_key in (2,3,4,5,8) 
		and not exists (select 1 from #imageassoc a2 where a1._Image_key = a2._Image_key 
		and a1._Object_key = a2._Object_key 
		and a2._AssayType_key in (1,6,9))''', None)

	# get pixeldb ids for full size images

        results = db.sql('''select i._Image_key, a.numericPart 
		from #imageassoc i, ACC_Accession a 
		where i._Image_key = a._Object_key 
		and a._MGIType_key = 9 
		and a._LogicalDB_key = 19 
		and a.preferred = 1 ''', 'auto')

	pixfullsize = {}
        for r in results:
	    pixfullsize[r['_Image_key']] = r['numericPart']

	# get pixeldb ids for thumbnail images

        results = db.sql('''select i._Image_key, a.numericPart 
		from #imageassoc i, ACC_Accession a 
		where i._ThumbnailImage_key = a._Object_key 
		and a._MGIType_key = 9 
		and a._LogicalDB_key = 19 
		and a.preferred = 1 ''', 'auto')

	pixthumbnail = {}
        for r in results:
	    pixthumbnail[r['_Image_key']] = r['numericPart']

	# process all records

	gxdResults = db.sql('''select *
		from #imageassoc 
		where sortOrder <= 3
		order by _Object_key, sortOrder, year desc, numericPart,
			figureLabel, _Image_key''', 'auto')

	phenoResults = db.sql('''select *
		from #imageassoc 
		where sortOrder > 3
		order by _Object_key, sortOrder, numericPart desc,
			figureLabel, _Image_key''', 'auto')

	results = gxdResults + phenoResults

	# generate a unique sequence number (starting at 1) for a given
	# Marker/Image pair for GXD.  The pheno images will use their own
	# sequence numbers starting with 1.

	x = 0			# sequence number
	prevMarkerKey = 0
	prevImageKey = 0
	prevSortOrder = 0

	# if not running for a specific reference, generate a file & use bcp

	if objectKey == 0:

	    cacheBCP = open(outDir + '/%s.bcp' % (table), 'w')

	    for r in results:

		markerKey = r['_Object_key']
		imageKey = r['_Image_key']
		sortOrder = r['sortOrder']

	        # If there is no pixel ID for the thumbnail, do not create
	        # a record for the cache table.

	        if not pixthumbnail.has_key(imageKey):
	            continue

		# if we have a new marker, or if we are just beginning the
		# section of pheno images, restart our numbering at 1

		if (prevMarkerKey != markerKey) or \
		    ((prevSortOrder < 4) and (sortOrder >= 4)):
			x = 1

		elif prevImageKey != imageKey:
		    x = x + 1

		prevMarkerKey = markerKey
		prevImageKey = imageKey
		prevSortOrder = sortOrder

		nextMaxKey = nextMaxKey + 1

	        cacheBCP.write(str(nextMaxKey) + COLDL +
			     mgi_utils.prvalue(imageKey) + COLDL + \
			     mgi_utils.prvalue(r['_ThumbnailImage_key']) + COLDL + \
			     mgi_utils.prvalue(r['_ImagePane_key']) + COLDL + \
			     mgi_utils.prvalue(r['_MGIType_key']) + COLDL + \
			     mgi_utils.prvalue(markerKey) + COLDL + \
			     mgi_utils.prvalue(r['_ObjectMGIType_key']) + COLDL + \
			     mgi_utils.prvalue(r['_ImageClass_key']) + COLDL + \
			     mgi_utils.prvalue(r['_Refs_key']) + COLDL + \
			     mgi_utils.prvalue(r['_AssayType_key']) + COLDL + \
			     mgi_utils.prvalue(pixfullsize[imageKey]) + COLDL + \
			     mgi_utils.prvalue(pixthumbnail[imageKey]) + COLDL + \
			     mgi_utils.prvalue(x) + COLDL + \
			     mgi_utils.prvalue(r['assayType']) + COLDL + \
			     mgi_utils.prvalue(r['figureLabel']) + COLDL + \
			     mgi_utils.prvalue(r['paneLabel']) + LINEDL)
	        cacheBCP.flush()

	    cacheBCP.close()

	# otherwise, use inline sql to update for the specific reference

        else:

	    # delete existing cache table entries

	    db.sql('delete from %s where _Refs_key = %s' % (table, objectKey), None)

	    for r in results:

		markerKey = r['_Object_key']
		imageKey = r['_Image_key']
		sortOrder = r['sortOrder']

	        # If there is no pixel ID for the thumbnail, do not create
	        # a record for the cache table.

	        if not pixthumbnail.has_key(imageKey):
	            continue

		# if we have a new marker, or if we are just beginning the
		# section of pheno images, restart our numbering at 1

		if (prevMarkerKey != markerKey) or \
		    ((prevSortOrder < 4) and (sortOrder >= 4)):
		    x = 1

		elif prevImageKey != imageKey:
		    x = x + 1

		prevMarkerKey = markerKey
		prevImageKey = imageKey
		prevSortOrder = sortOrder

		# tweak values of fields that can be null

		if r['paneLabel'] == None:
		    paneLabel = 'null'
		else:
		    paneLabel = '"' + r['paneLabel'] + '"'

		if r['assayType'] == None:
		    assayType = 'null'
		else:
		    assayType = '"' + r['assayType'] + '"'

		if r['_AssayType_key'] == None:
		    assayTypeKey = 'null'
		else:
		    assayTypeKey = r['_AssayType_key']

		# do the insertion one row at a time

	        nextMaxKey = nextMaxKey + 1

	        db.sql(insertSQL % (str(nextMaxKey), \
		    mgi_utils.prvalue(imageKey), \
		    mgi_utils.prvalue(r['_ThumbnailImage_key']), \
		    mgi_utils.prvalue(r['_ImagePane_key']), \
		    mgi_utils.prvalue(r['_MGIType_key']), \
		    mgi_utils.prvalue(markerKey), \
		    mgi_utils.prvalue(r['_ObjectMGIType_key']), \
		    mgi_utils.prvalue(r['_ImageClass_key']), \
		    mgi_utils.prvalue(r['_Refs_key']), \
		    mgi_utils.prvalue(assayTypeKey), \
		    mgi_utils.prvalue(pixfullsize[imageKey]), \
		    mgi_utils.prvalue(pixthumbnail[imageKey]), \
		    mgi_utils.prvalue(x),\
		    mgi_utils.prvalue(assayType),\
		    mgi_utils.prvalue(r['figureLabel']), \
		    mgi_utils.prvalue(paneLabel)), None)

#
# Main Routine
#

print '%s' % mgi_utils.date()

try:
	optlist, args = getopt.getopt(sys.argv[1:], 'S:D:U:P:W:J:K:')
except:
	showUsage()

server = None
database = None
user = None
password = None
jNumber = None
objectKey = None

for opt in optlist:
	if opt[0] == '-S':
		server = opt[1]
	elif opt[0] == '-D':
		database = opt[1]
	elif opt[0] == '-U':
		user = opt[1]
	elif opt[0] == '-P':
		password = string.strip(open(opt[1], 'r').readline())
	elif opt[0] == '-W':
		password = opt[1]
	elif opt[0] == '-J':
		jNumber = opt[1]
	elif opt[0] == '-K':
		objectKey = string.atoi(opt[1])
	else:
		showUsage()

if server is None or \
   database is None or \
   user is None or \
   password is None or \
   (jNumber is None and objectKey is None):
	showUsage()

db.set_sqlLogin(user, password, server, database)
db.useOneConnection(1)

if jNumber is not None:
	objectKey = getRefKey(jNumber)

if objectKey == 0:
	db.set_sqlLogFunction(db.sqlLogAll)

process(objectKey)
db.useOneConnection(0)
print '%s' % mgi_utils.date()

