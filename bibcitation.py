#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp file for BIB_Citation_Cache
#
# Usage:
#	bibcitation.py -Sdbserver -Ddatabase -Uuser -Ppasswordfile -Kobjectkey
#
# Processing:
#
# History
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
import db
import mgi_utils

try:
    COLDL = os.environ['COLDELIM']
    LINEDL = '\n'
    table = os.environ['TABLE']
    outDir = os.environ['MGICACHEBCPDIR']
except:
    table = 'BIB_Citation_Cache'

deleteSQL = 'delete from BIB_Citation_Cache where _Refs_key = %s'
insertSQL = 'insert into BIB_Citation_Cache values (%s,%s,"%s","%s","%s","%s","%s","%s","%s")'

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
	# mgi ids
	#

        cmd = 'select a._Object_key, a.accID ' + \
		'from ACC_Accession a ' + \
		'where a._MGIType_key = 1 ' + \
		'and a._LogicalDB_key = 1 ' + \
		'and a.prefixPart =  "MGI:" ' + \
		'and a.preferred = 1 '

	if objectKey != 0:
	    cmd = cmd + 'and a._Object_key = %s' % (objectKey)

	results = db.sql(cmd, 'auto')
	mgi = {}
        for r in results:
	    mgi[r['_Object_key']] = r

	#
	# jnum ids
	#

        cmd = 'select a._Object_key, a.accID, a.numericPart ' + \
		'from ACC_Accession a ' + \
		'where a._MGIType_key = 1 ' + \
		'and a._LogicalDB_key = 1 ' + \
		'and a.prefixPart =  "J:" ' + \
		'and a.preferred = 1 '

	if objectKey != 0:
	    cmd = cmd + 'and a._Object_key = %s' % (objectKey)

	results = db.sql(cmd, 'auto')
	jnum = {}
        for r in results:
	    jnum[r['_Object_key']] = r

	#
	# pubmed ids
	#

        cmd = 'select a._Object_key, a.accID ' + \
		'from ACC_Accession a ' + \
		'where a._MGIType_key = 1 ' + \
		'and a._LogicalDB_key = 29 ' + \
		'and a.preferred = 1 '

	if objectKey != 0:
	    cmd = cmd + 'and a._Object_key = %s' % (objectKey)

	results = db.sql(cmd, 'auto')
	pubmed = {}
        for r in results:
	    pubmed[r['_Object_key']] = r

	#
	# reference attributes
	#

        cmd = 'select r._Refs_key, r.journal, reviewStatus = s.name, ' + \
		'citation = r.journal + " " + r.date + ";" + r.vol + "(" + r.issue + "):" + r.pgs, ' + \
		'short_citation = r._primary + ", " + r.journal + " " + r.date + ";" + r.vol + "(" + r.issue + "):" + r.pgs ' + \
		'from BIB_Refs r, BIB_ReviewStatus s ' + \
		'where r._ReviewStatus_key = s._ReviewStatus_key '

	if objectKey != 0:
	    cmd = cmd + 'and r._Refs_key = %s' % (objectKey)

	results = db.sql(cmd, 'auto')

	if objectKey == 0:

	    cacheBCP = open(outDir + '/%s.bcp' % (table), 'w')

	    for r in results:

		key = r['_Refs_key']

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
			       mgi_utils.prvalue(r['short_citation']) + COLDL + \
			       mgi_utils.prvalue(r['citation']) + LINEDL)
	        cacheBCP.flush()

	    cacheBCP.close()

        else:

	    db.sql(deleteSQL % (objectKey), None)

	    if pubmed.has_key(objectKey):
		pubmedID = pubmed[objectKey]['accID']
            else:
		pubmedID = 'null'

	    r = results[0]
	    db.sql(insertSQL % ( \
		mgi_utils.prvalue(objectKey), \
		mgi_utils.prvalue(jnum[objectKey]['numericPart']), \
		mgi_utils.prvalue(jnum[objectKey]['accID']), \
	        mgi_utils.prvalue(mgi[objectKey]['accID']), \
		mgi_utils.prvalue(pubmedID), \
	        mgi_utils.prvalue(r['reviewStatus']), \
		mgi_utils.prvalue(r['journal']), \
		mgi_utils.prvalue(r['short_citation']), \
		mgi_utils.prvalue(r['citation'])), None)

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
db.set_sqlLogFunction(db.sqlLogAll)
process(objectKey)
db.useOneConnection(0)
print '%s' % mgi_utils.date()

