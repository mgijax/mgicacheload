#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp file for BIB_Citation_Cache
#
# Usage:
#	bibcitation.py -Uuser -Ppasswordfile -Kobjectkey
#
#	if objectkey == 0, then retrieve all references
#	if objectkey > 0, then retrieve reference specified by key
#	if objectkey == -1, then retrieve references that do not have a cache record
#	if objectkey == -2, then retrieve references that have a modification date = today
#		
# Processing:
#
# History
#
# 04/16/2014	lec
#	- update usage to include -S and -D
#	- coordinate with ei/dsrc/PythonReference
#
# 01/19/2010	lec
#	- TR 10037/remove quotes from citations
#
# 10/19/2006	lec
#	- TR 6812
#
#
'''

import sys
import os
import getopt
import string
#import db
import mgi_utils

COLDL = os.environ['COLDELIM']
outDir = os.environ['MGICACHEBCPDIR']
LINEDL = '\n'

try:
    table = os.environ['TABLE']
    if os.environ['DB_TYPE'] == 'postgres':
        import pg_db
        db = pg_db
        db.setTrace()
        db.setAutoTranslateBE()
    else:
        import db
        db.set_sqlLogFunction(db.sqlLogAll)
except:
    import db
    db.set_sqlLogFunction(db.sqlLogAll)
    table = 'BIB_Citation_Cache'


insertSQL = 'insert into BIB_Citation_Cache values (%s,%s,"%s","%s","%s","%s","%s","%s","%s", %s, "%s")'

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
		'-K object key\n'

	sys.stderr.write(usage)
	sys.exit(1)
 
def process(objectKey):

	#
	# reference attributes
	#

	if os.environ['DB_TYPE'] == 'postgres':

	    cmd = 'select r._Refs_key, r.journal, reviewStatus = s.name, r.isReviewArticle, ' + \
		    'coalesce(r.journal, "") || " " || coalesce(r.date, "") || ";" || coalesce(r.vol, "") || "(" || coalesce(r.issue, "") || "):" || coalesce(r.pgs, "") as citation, ' \
		    'coalesce(r._primary, "") || ", " || coalesce(r.journal, "") || " " || coalesce(r.date, "") || ";" || coalesce(r.vol, "") || "(" || coalesce(r.issue, "") || "):" || coalesce(r.pgs, "") as short_citation ' \
		    'into #refsTemp ' + \
		    'from BIB_Refs r, BIB_ReviewStatus s ' + \
		    'where r._ReviewStatus_key = s._ReviewStatus_key '
	else:

	    cmd = 'select r._Refs_key, r.journal, reviewStatus = s.name, r.isReviewArticle, ' + \
		    'r.journal || " " || r.date || ";" || r.vol || "(" || r.issue || "):" || r.pgs as citation, ' \
		    'r._primary || ", " || r.journal || " " || r.date || ";" || r.vol || "(" || r.issue || "):" || r.pgs as short_citation ' \
		    'into #refsTemp ' + \
		    'from BIB_Refs r, BIB_ReviewStatus s ' + \
		    'where r._ReviewStatus_key = s._ReviewStatus_key '
	

	if objectKey > 0:
	    cmd = cmd + 'and r._Refs_key = %s' % (objectKey)

	# all references that don't have entries in the cache table
        elif objectKey == -1:
	    cmd = cmd + 'and not exists (select 1 from %s c where r._Refs_key = c._Refs_key)' % (table)

	# all references modified today

        elif objectKey == -2:
	    cmd = cmd + 'and convert(char(10), r.modification_date, 101) = convert(char(10), getdate(), 101)'

	db.sql(cmd, None)
	db.sql('create index idx1 on #refsTemp(_Refs_key)', None)

	#
	# mgi ids
	#

        results = db.sql('select a._Object_key, a.accID ' + \
		'from #refsTemp r, ACC_Accession a ' + \
		'where r._Refs_key = a._Object_key ' + \
		'and a._MGIType_key = 1 ' + \
		'and a._LogicalDB_key = 1 ' + \
		'and a.prefixPart =  "MGI:" ' + \
		'and a.preferred = 1 ', 'auto')

	mgi = {}
        for r in results:
	    mgi[r['_Object_key']] = r

	#
	# jnum ids
	#

        results = db.sql('select a._Object_key, a.accID, a.numericPart ' + \
		'from #refsTemp r, ACC_Accession a ' + \
		'where r._Refs_key = a._Object_key ' + \
		'and a._MGIType_key = 1 ' + \
		'and a._LogicalDB_key = 1 ' + \
		'and a.prefixPart =  "J:" ' + \
		'and a.preferred = 1', 'auto')

	jnum = {}
        for r in results:
	    jnum[r['_Object_key']] = r

	#
	# pubmed ids
	#

        results = db.sql('select a._Object_key, a.accID ' + \
		'from #refsTemp r, ACC_Accession a ' + \
		'where r._Refs_key = a._Object_key ' + \
		'and a._MGIType_key = 1 ' + \
		'and a._LogicalDB_key = 29 ' + \
		'and a.preferred = 1', 'auto')

	pubmed = {}
        for r in results:
	    pubmed[r['_Object_key']] = r

	# process all records

	results = db.sql('select * from #refsTemp', 'auto')

	if objectKey == 0:

	    cacheBCP = open(outDir + '/%s.bcp' % (table), 'w')

	    for r in results:

		key = r['_Refs_key']

		if r['isReviewArticle'] == 1:
		    isReviewArticle = 'Yes'
                else:
		    isReviewArticle = 'No'

	        cacheBCP.write(mgi_utils.prvalue(key) + COLDL + \
			     mgi_utils.prvalue(jnum[key]['numericPart']) + COLDL + \
			     mgi_utils.prvalue(jnum[key]['accID']) + COLDL + \
			     mgi_utils.prvalue(mgi[key]['accID']) + COLDL)

		if pubmed.has_key(key):
		    cacheBCP.write(mgi_utils.prvalue(pubmed[key]['accID']) + COLDL)
                else:
		    cacheBCP.write(COLDL)

	        cacheBCP.write(mgi_utils.prvalue(r['reviewStatus']) + COLDL + \
			       mgi_utils.prvalue(r['journal']) + COLDL + \
			       mgi_utils.prvalue(r['citation']) + COLDL + \
			       mgi_utils.prvalue(r['short_citation']) + COLDL + \
			       mgi_utils.prvalue(r['isReviewArticle']) + COLDL + \
			       isReviewArticle + LINEDL)
	        cacheBCP.flush()

	    cacheBCP.close()

        else:

	    # delete existing cache table entries

	    db.sql('delete %s ' % (table) + \
		'from #refsTemp r, %s c ' % (table) + \
		'where r._Refs_key = c._Refs_key', None)

	    for r in results:

		key = r['_Refs_key']

		if r['isReviewArticle'] == 1:
		    isReviewArticle = 'Yes'
                else:
		    isReviewArticle = 'No'

	        if pubmed.has_key(key):
		    pubmedID = pubmed[key]['accID']
                else:
		    pubmedID = 'null'

		# TR 10037/remove quotes from citations
		citation = string.replace(r['citation'], '"', '')
		short_citation = string.replace(r['short_citation'], '"', '')

	        db.sql(insertSQL % ( \
		    mgi_utils.prvalue(key), \
		    mgi_utils.prvalue(jnum[key]['numericPart']), \
		    mgi_utils.prvalue(jnum[key]['accID']), \
	            mgi_utils.prvalue(mgi[key]['accID']), \
		    mgi_utils.prvalue(pubmedID), \
	            mgi_utils.prvalue(r['reviewStatus']), \
		    mgi_utils.prvalue(r['journal']), \
		    mgi_utils.prvalue(citation), \
		    mgi_utils.prvalue(short_citation),
		    mgi_utils.prvalue(r['isReviewArticle']),
		    mgi_utils.prvalue(isReviewArticle)), None)

#
# Main Routine
#

print '%s' % mgi_utils.date()

try:
	optlist, args = getopt.getopt(sys.argv[1:], 'S:D:U:P:K:')
except:
	showUsage()

server = None
database = None
user = None
password = None
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
        elif opt[0] == '-K':
                objectKey = string.atoi(opt[1])
        else:
                showUsage()

if server is None or \
   database is None or \
   user is None or \
   password is None or \
   objectKey is None:
        showUsage()

db.set_sqlLogin(user, password, server, database)
db.useOneConnection(1)

#if objectKey == 0:
#    db.set_sqlLogFunction(db.sqlLogAll)

process(objectKey)
db.useOneConnection(0)
print '%s' % mgi_utils.date()

