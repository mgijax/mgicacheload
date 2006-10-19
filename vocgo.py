#!/usr/local/bin/python

'''
#
# Purpose:
#
# Create bcp file for VOC_GO_Cache
#
# Usage:
#	vocgo.py
#
# Processing:
#
# History
#
# 10/19/2006	lec
#	- TR 6821; new
#
#
'''

import sys
import os
import db
import mgi_utils

try:
    COLDL = os.environ['COLDELIM']
    LINEDL = '\n'
    table = os.environ['TABLE']
    outDir = os.environ['MGICACHEBCPDIR']
except:
    table = 'VOC_GO_Cache'

def createBCPfile():

	print 'Creating %s.bcp...' % (table)

	cacheBCP = open(outDir + '/%s.bcp' % (table), 'w')

	results = db.sql('select t._Term_key, n._DAG_key, t.term, a.accID, d.abbreviation ' + \
		'from VOC_Term t, ACC_Accession a, VOC_VocabDAG vd, DAG_Node n, DAG_DAG d ' + \
		'where t._Vocab_key = 4 ' + \
		'and t._Term_key = a._Object_key ' + \
		'and a._MGIType_key = 13 ' + \
		'and a.preferred = 1 ' + \
		'and t._Vocab_key = vd._Vocab_key ' + \
		'and t._Term_key = n._Object_key ' + \
		'and vd._DAG_key = n._DAG_key ' + \
		'and n._DAG_key = d._DAG_key ', 'auto')

	for r in results:
	    cacheBCP.write(mgi_utils.prvalue(r['_Term_key']) + COLDL + \
	    		   mgi_utils.prvalue(r['_DAG_key']) + COLDL + \
	    		   mgi_utils.prvalue(r['abbreviation']) + COLDL + \
	    		   mgi_utils.prvalue(r['accID']) + COLDL + \
	    		   mgi_utils.prvalue(r['term']) + LINEDL)
	cacheBCP.close()

#
# Main Routine
#

print '%s' % mgi_utils.date()
db.useOneConnection(1)
db.set_sqlLogFunction(db.sqlLogAll)
createBCPfile()
db.useOneConnection(0)
print '%s' % mgi_utils.date()

