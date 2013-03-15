#!/usr/local/bin/python

'''
#
# Purpose: Load GO Inferred-From text data into the Accession table
#
# Usage:
#	inferredFrom.py -Sserver -Ddatabase -Uuser -Ppasswordfile 
#		{ -Kmarkerkey | -BcreatedBy }
#
#	if objectkey == 0, then retrieve all inferred-from
#	if objectkey > 0, then retrieve inferred-from specified by key
#	if objectkey == -1, then run the checker
#	if createdBy is not None, then retrieve inferred-from specified by the created by login (ex."swissload")
#		
# History
#
# 03/01/2012	lec
#	- TR 10950
# 	'sp_kw' changed to 'uniprotkb-kw' : 111
#
# 11/29/2011	lec
#	- TR10919/replace 'genedb_spombe' witih 'pombase'
#
# 04/25/2011	lec
#	- TR10681/add NCBI_Gene
#
# 03/29/2011	lec
#	- TR10652/NCBI: to be replaced by RefSeq:
#
# 07/14/2010 - lec
#	- TR9962/add PANTHER/PTHR
#
# 08/27/2009 - lec
#	- TR9769/add logical DB "ChEBI"
#	- added 'uniprot' (same as 'uniprotkb')
#
# 08/18/2008 - lec
#	- TR9217; add Pfam
#
# 06/11/2008 - lec
#	- TR9057/fixes to original code
#
# 4/3/08 - jsb - introduced for TR8633 in MGI 4.B release
#
'''

import sys
import os
import getopt
import string
import re
import db
import mgi_utils
import accessionlib

objectKey = None
createdBy = None

execSQL = 'exec ACC_insertNoChecks %d,"%s",%d,"Annotation Evidence",-1,1,1'
eiErrorStatus = '%s     %s     %s\n'

# maps provider prefix to logical database key
# using lowercase
# ncbi = refseq = 27: both prefixes can be used
providerMap = {
	'mgi' : 1,
	'go' : 1,
	'embl' : 9,
	'ec' : 8,
	'pombase' : 115,
	'interpro' : 28,
	'pir' : 78,
	'pfam' : 119,
	'protein_id' : 13,
	'ncbi' : 27,
	'refseq' : 27,
	'rgd' : 47,
	'uniprotkb-kw' : 111,
	'sp_kw' : 111,
	'sgd' : 114,
	'uniprotkb' : 13,
	'uniprot' : 13,
	'chebi' : 127,
	'panther' : 147,
	'ncbi_gene' : 160,
	}

#
# checks for EMBL accession ids (see ACC_Accession_Insert trigger)
#
#	1 alpah, 5 numerics: [A-Z]     [0-9][0-9][0-9][0-9][0-9]
#	2 alpha, 6 numerics: [A-Z][A-Z][0-9][0-9][0-9][0-9][0-9][0-9]
#
embl_re1 = re.compile("^[A-Z]{1,1}[0-9]{5,5}$")
embl_re2 = re.compile("^[A-Z]{2,2}[0-9]{6,6}$")

# dictionary of existing cache
cacheIF = {}

def showUsage():
	#
	# Purpose:  Displayes the correct usage of this program and exists
	#
 
	usage = 'usage: %s\n' % sys.argv[0] + \
		'-S server\n' + \
		'-D database\n' + \
		'-U user\n' + \
		'-P password file\n' + \
		'{ -K object key | -B createdByName }\n'

	exit(1, usage)

def exit(status, message = None):
	#
	# requires:
	#	status, the numeric exit status (integer)
	#	message (string)
	#
	# effects:
	# Print message to stderr and exists
	#
	# returns:
	#

	if message is not None:
		sys.stderr.write('\n' + str(message) + '\n')

	db.useOneConnection()
	sys.exit(status)

def init():
    # requires: 
    #
    # effects: 
    # 1. Processes command line options
    # 2. Initializes local DBMS parameters
    # 3. Initializes global file descriptors/file names
    #
    # returns:
    #

	global objectKey, createdBy

	try:
		optlist, args = getopt.getopt(sys.argv[1:], 'S:D:U:P:K:B:')
	except:
		showUsage()

	server = db.get_sqlServer()
	database = db.get_sqlDatabase()
	user = None
	password = None

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
		elif opt[0] == '-B':
			createdBy = re.sub('"', '', opt[1])
		else:
			showUsage()

	if server is None or \
   	   database is None or \
   	   user is None or \
   	   password is None or \
   	   (objectKey is None and createdBy is None):
		showUsage()

	db.set_sqlLogin(user, password, server, database)
    	db.useOneConnection(1)

        # Log all SQL if runnning checker, loading all data or
	# running the load by a specific user
	if objectKey <= 0 or createdBy is not None:
        	db.set_sqlLogFunction(db.sqlLogAll)

def preCache():
	#
	# select the existing cache data into a temp table
	#
	# if objectKey >= 0
	# 	delete the existing cache data
	# 
	# elif objectKey == -1:
	# 	put existing cache-ed data into a dictionay
	#

	global cacheIF

	#
	# select existing cache data

	cmd = 'select a._Accession_key, a.accID, a._Object_key ' + \
		'into #toCheck ' + \
		'from ACC_Accession a, VOC_Annot v, VOC_Evidence e, MGI_User u ' + \
		'where a._MGIType_key = 25 ' + \
		'and v._AnnotType_key = 1000 ' + \
		'and v._Annot_key = e._Annot_key ' + \
		'and a._Object_key = e._AnnotEvidence_key ' + \
		'and e._CreatedBy_key = u._User_key '
        
	# select by object or created by

	if objectKey > 0:
		cmd = cmd + ' and v._Object_key = %s' % (objectKey)

	elif createdBy is not None:
		cmd = cmd + ' and u.login like "%s"' % (createdBy)

	db.sql(cmd, None)
	db.sql('create index idx1 on #toCheck(_Accession_key)', None)

	# delete existing cache data

	if objectKey >= 0 or createdBy is not None:
		db.sql('delete ACC_Accession from #toCheck d, ACC_Accession a ' + \
			'where d._Accession_key = a._Accession_key', None)

	# copy existing cache table accession keys

	elif objectKey == -1:
		results = db.sql('select * from #toCheck', 'auto')
		for r in results:
			key = r['_Object_key']
			value = r['accID']
			if not cacheIF.has_key(key):
			   cacheIF[key] = []
		        cacheIF[key].append(value)

def processCache():
	#
	# process the GO inferred-from data from the vocabulary table
	#
	# add the data to the cache table (accession table)
	# OR
	# write out the checking errors
	#

	# retrieve GO data in VOC_Evidence table

        cmd = 'select e._AnnotEvidence_key, e.inferredFrom, m.symbol, goID = ta.accID ' + \
		'from VOC_Annot a, VOC_Evidence e, MRK_Marker m, ACC_Accession ta, MGI_User u ' + \
		'where a._AnnotType_key = 1000 ' + \
		'and a._Annot_key = e._Annot_key ' + \
		'and e.inferredFrom is not null ' + \
		'and a._Object_key = m._Marker_key ' + \
		'and a._Term_key = ta._Object_key ' + \
		'and ta._LogicalDB_key = 31 ' + \
		'and ta._MGIType_key = 13 ' + \
		'and ta.preferred = 1 ' + \
		'and e._CreatedBy_key = u._User_key '

	# select data by specific marker or by created by

	if objectKey > 0:
		cmd = cmd + ' and a._Object_key = %s' % (objectKey)

	elif createdBy is not None:
		cmd = cmd + ' and u.login like "%s"' % (createdBy)

	results = db.sql(cmd, 'auto')
	eiErrors = ''

	for r in results:

		key = r['_AnnotEvidence_key']
		inferredFrom = r['inferredFrom']
		symbol = r['symbol']
		goID = r['goID']

		#
		# the accession ids are separated by '|' or ',' or none
		# split them up into a list
		#

                if inferredFrom.find('|') >= 0:
                	allAccIDs = inferredFrom.split('|')
                elif inferredFrom.find(',') >= 0:
                	allAccIDs = inferredFrom.split(',')
                else:
                	allAccIDs = [inferredFrom]

		#
		# for each accession id in the list of this marker...
		#

		for accID in allAccIDs:

			try:
				if accID == '':
				    continue

				# MGI, GO and RGD IDs are stored
				# with the MGI:,  GO: and RGD: prefixes
				# for all others, we do not store the ##: part

		    		fullAccID = accID
		    		tokens = accID.split(':')
		    		provider = tokens[0].lower()
		    		accIDPart = tokens[1]

		        	if provider != 'mgi' and provider != 'go' and provider != 'rgd':
					accID = accIDPart

				# for EMBL ids, check if accession id is valid

                        	if provider == 'embl':
                        		embl_result1 = embl_re1.match(accID)
                        		embl_result2 = embl_re2.match(accID)
					if embl_result1 is None and embl_result2 is None:
						eiErrors = eiErrors + eiErrorStatus % (symbol, goID, fullAccID)
						continue

				# load the id into the accession cache
				# for now, we will do this for a bulk load as well
				#if objectKey = 0 or createdBy is not None:

				if objectKey >= 0 or createdBy is not None:
					# by marker
					addIt = execSQL % (key, accID, providerMap[provider])
					db.sql(addIt, None)

				# will do this only when we implement a bcp strategy
				#elif objectKey == 0:

				# else, run the checker

				else:
					if key in cacheIF:
						if accID not in cacheIF[key]:
							eiErrors = eiErrors + eiErrorStatus % (symbol, goID, fullAccID)
					else:
						eiErrors = eiErrors + eiErrorStatus % (symbol, goID, fullAccID)

			except:
				eiErrors = eiErrors + eiErrorStatus % (symbol, goID, fullAccID)

	if eiErrors != '':
		# the EI will pick up the standard output via the ei/dsrc/PythonLib.d/PythonInferredFromCache code
		print '\nThe following errors exist in the inferred-from text:\n\n' + eiErrors

#
#
# Main Routine
#

init()
preCache()
processCache()
exit(0)

