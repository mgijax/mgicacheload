#!/usr/local/bin/python

# Purpose: to synchronize the accession IDs associated with a GO annotation
#	with the IDs specified in that annotation's "Inferred From" field
# Usage: see USAGE variable below
# History:
#	4/3/08 - jsb - introduced for TR8633 in MGI 4.B release

import sys
import getopt
import os
import db
import time
import string
import mgi_utils

USAGE = '''Usage: %s <parameters>
    Required parameters:
	-S <server>   : name of the database server
	-D <database> : name of the database within that server
	-U <user>     : database username
	-P <pwd file> : path to file containing that user's database password
    Optional parameters:
	-M <marker key>     : process all annotations for specified marker
	-A <annotation key> : process only specified annotation
	-v                  : run in verbose mode (more reporting output)
    Notes:
    	1. At most one of -M or -A may be specified.
	2. If neither -M or -A are specified, we process all GO annotations.
	3. At present, we use inline SQL rather than BCP for all updates.
	   This will be slower for a full refresh, but should not happen
	   often, so we'll take the performance hit and save the extra
	   development time.
''' % sys.argv[0]

MARKER_KEY = None		# string; single _Marker_keys to process
ANNOT_KEY = None		# string; single _Annot_key to process
VERBOSE = False			# boolean; run in verbose mode?
START_TIME = time.time()	# float; time in seconds at which script began
EVIDENCE_MGITYPE = None		# int; _MGIType_key for "Annotation Evidence"

# dictionary mapping evidence key to a dictionary which maps each id to its
# (provider) prefix
INFERRED_FROM = None

# list of dictionaries; each dictionary defines one _Object_key (really an
# _AnnotEvidence_key) and one accID
IDS = None

def bailout (
	err = None,		# string; error message to give to user
	showUsage = False	# boolean; show the Usage statement?
	):
	if showUsage:
		sys.stderr.write (USAGE)
	if err:
		sys.stderr.write ('Error: %s\n' % err)
	sys.exit(1)

def sql (
	cmd
	):
	try:
		results = db.sql (cmd, 'auto')
	except:
		bailout ('Failed on query: %s' % str(cmd))
	return results

def optionsToDict (
	options		# list of (option flag, value) pairs
	):
	dict = {}
	for (flag, value) in options:
		dict[flag] = value
	return dict

def message (
	msg		# string; message to send to user in verbose mode
	):
	if VERBOSE:
		sys.stderr.write ('%8.3f %s\n' % (time.time() - START_TIME,
			msg))
	return

def getMarkerKeys():
	cmd = '''SELECT DISTINCT _Object_key
		FROM VOC_Annot
		WHERE _AnnotType_key = 1000	-- GO/Marker'''

	results = sql(cmd)

	markers = []
	for row in results:
		markers.append (row['_Object_key'])
	return markers

def processCommandLine ():
	global VERBOSE, MARKER_KEY, ANNOT_KEY

	try:
		optlist, args = getopt.getopt (sys.argv[1:], 'S:D:U:P:M:A:v')
	except getopt.GetoptError:
		bailout ('Invalid command-line flag(s)', True)

	if len(args) > 0:
		bailout ('Too many command-line arguments', True)

	options = optionsToDict (optlist)

	if not options.has_key('-S'):
		bailout ('Must specify database server (-S)', True)
	if not options.has_key('-D'):
		bailout ('Must specify database name (-D)', True)
	if not options.has_key('-U'):
		bailout ('Must specify database user (-U)', True)
	if not options.has_key('-P'):
		bailout ('Must specify password file (-P)', True)
	if options.has_key('-M') and options.has_key('-A'):
		bailout ('Cannot specify both -M and -A', True)

	pwdFile = options['-P']
	if not os.path.exists(pwdFile):
		bailout ('Cannot find password file: %s' % pwdFile, True)
	try:
		fp = open(pwdFile, 'r')
		password = fp.readline().rstrip()
		fp.close()
	except:
		bailout ('Cannot read password file: %s' % pwdFile, True)

	db.set_sqlLogin (options['-U'], password, options['-S'],
		options['-D'])
	db.useOneConnection(1)

	# fast, easy query to check if login worked okay:
	sql ('SELECT COUNT(1) FROM MGI_dbInfo')

	if options.has_key('-v'):
		VERBOSE = True
		message ('Running against %s..%s as user %s' % (
			options['-S'], options['-D'], options['-U']))

	if options.has_key('-M'):
		MARKER_KEY = options['-M']
		message ('Running for marker %s' % MARKER_KEY)
	elif options.has_key('-A'):
		ANNOT_KEY = options['-A']
		message ('Running for annotation %s' % ANNOT_KEY)
	else:
		message ('Running for all annotated markers, start %s' % \
			mgi_utils.date())
		db.set_sqlLogFunction (db.sqlLogAll)
	return

def loadCaches():
	# load any data we are going to store in memory caches

	global EVIDENCE_MGITYPE, INFERRED_FROM, IDS

	# get the MGI Type for "Annotation Evidence"

	results = sql ('''SELECT _MGIType_key
		FROM ACC_MGIType
		WHERE name = "Annotation Evidence"''')

	if not results:
		bailout ('Cannot find _MGIType_key for "Annotation Evidence"')
	EVIDENCE_MGITYPE = results[0]['_MGIType_key']
	message ('Found MGI Type')

	# get data from VOC_Evidence.inferredFrom

	if ANNOT_KEY != None:
		INFERRED_FROM = getInferredFrom (annotKey = ANNOT_KEY)
	elif MARKER_KEY != None:
		INFERRED_FROM = getInferredFrom (markerKey = MARKER_KEY)
	else:
		INFERRED_FROM = getInferredFrom()
	message ('Looked up "inferredFrom" values')

	# get already-cached IDs from ACC_Accession

	bulkDeleteIds()		# remove ones with no evidence records first
	IDS = getIds()

	message ('Looked up existing cached IDs')
	return

def getInferredFrom (markerKey = None, annotKey = None):
	# returns dictionary mapping evidence key to a dictionary which maps
	# each id to its (provider) prefix

	# note that we must even retrieve inferredFrom pieces which are null,
	# so we will know when a user has removed existing IDs from that field

	cmd = '''SELECT va._Annot_key, 
			ve._AnnotEvidence_key,
			ve.inferredFrom
		FROM VOC_Annot va,
			VOC_Evidence ve
		WHERE va._AnnotType_key = 1000		-- GO/Marker
			AND va._Annot_key = ve._Annot_key %s'''

	if annotKey:
		cmd = cmd % ('AND va._Annot_key = %s' % annotKey)
	elif markerKey:
		cmd = cmd % ('AND va._Object_key = %s' % markerKey)
	else:
		# else: retreve all inferredFrom pieces
		cmd = cmd % ''

	results = sql (cmd)

	key2ids = {}

	for row in results:
		inferredFrom = row['inferredFrom']
		ids = []

		if inferredFrom != None:
			inferredFrom = inferredFrom.strip()
			if len(inferredFrom) > 0:
				if inferredFrom.find('|') >= 0:
					ids = inferredFrom.split('|')
				elif inferredFrom.find(',') >= 0:
					ids = inferredFrom.split(',')
				else:
					ids = [inferredFrom]

				ids = map (string.strip, ids)

		idDict = {}
		for id in ids:
			colonPos = id.find(':')
			if colonPos >= 0:
				prefix = id[:colonPos]
				if prefix != 'MGI':
					id = id[colonPos+1:]
			else:
				prefix = None

			idDict[id] = prefix

		key2ids[row['_AnnotEvidence_key']] = idDict

	return key2ids

def bulkDeleteIds():
	# delete all evidence IDs from the accession table which have had
	# their evidence lines removed from VOC_Evidence.  This is primarily
	# to help bulk loads like the SwissProt load, which do full drop-and-
	# reloads of evidence records.

	results = sql ('''select count(1)
		from ACC_Accession a
		where a._MGIType_key = %d
			and not exists (select 1
				from VOC_Evidence e
				where a._Object_key = e._AnnotEvidence_key)
		''' % (EVIDENCE_MGITYPE) )

	count = results[0]['']

	sql ('''delete ACC_Accession
		from ACC_Accession a
		where a._MGIType_key = %d
			and not exists (select 1
				from VOC_Evidence e
				where a._Object_key = e._AnnotEvidence_key)
		''' % (EVIDENCE_MGITYPE) )

	message ('bulk deleted %d IDs which had no evidence record' % count)
	return

def getIds():
	ids = []

	# if we specified an annotation key or a marker key, then we only want
	# to retrieve IDs for the relevant annotation evidence keys

	if (ANNOT_KEY != None) or (MARKER_KEY != None):
		evidenceKeys = INFERRED_FROM.keys()
		keySets = []
		while len(evidenceKeys) > 200:
			keySets.append ( evidenceKeys[:200] )
			evidenceKeys = evidenceKeys[200:]
		if evidenceKeys:
			keySets.append ( evidenceKeys )

		# retrieve existing cached IDs from ACC_Accession for each key

		for keySet in keySets:
			results = sql ('''SELECT accID,
					_Object_key
				FROM ACC_Accession
				WHERE _MGIType_key = %s
					AND _Object_key IN (%s)''' % (
						EVIDENCE_MGITYPE,
						','.join (map(str, keySet)) ))
			ids = ids + results

	# otherwise, retrieve all IDs for annotation evidence keys

	else:
		ids = sql ('''SELECT accID,
				_Object_key
			FROM ACC_Accession
			WHERE _MGIType_key = %s''' % EVIDENCE_MGITYPE)
	return ids

def diff ():
	# compare the string entered by the user (stored in VOC_Evidence's
	# inferredFrom field, and bundled in 'inferredFrom' parameter) with
	# the cached data currently in ACC_Accession 
	# note: destroys value of 'INFERRED_FROM' during processing

	global INFERRED_FROM

	# chunk the evidence keys into sets of up to 200, so we can be
	# flexible enough to handle any size 'inferredFrom'

	# retrieve existing cached IDs from ACC_Accession for each key

	toDelete = []

	for row in IDS:
		accID = row['accID']
		objectKey = row['_Object_key']

		# we are going to handle the diff in a single pass
		# through the set of results; for each accession ID...
		#   1. if not still in 'INFERRED_FROM', add it to a
		#	list of ones to delete
		#   2. if it is still there, remove it from there (in
		#	memory, not in the database)
		#   3. when we are done, anything which remains in
		#	'INFERRED_FROM' needs to be added

		if (not INFERRED_FROM.has_key(objectKey)) or \
			(not INFERRED_FROM[objectKey].has_key(accID)):
			toDelete.append ( (objectKey, accID) )
		else:
			del INFERRED_FROM[objectKey][accID]

	toAdd = []
	for (objectKey, idDict) in INFERRED_FROM.items():
		for (id, provider) in idDict.items():
			if len(id.strip()) > 0:
				toAdd.append ( (objectKey, id, provider) )

	message ('computed diff')

	return toDelete, toAdd

# fill in: object key, acc ID, logical DB key
INSERT_ACC = 'EXEC ACC_insert %d, "%s", %d, "Annotation Evidence", -1, 1, 1'

# fill in: object key, MGI Type key, one or more IDs
DELETE_ACC = '''DELETE FROM ACC_Accession
	WHERE _Object_key = %d
		AND _MGIType_key = %d
		AND accID IN ("%s")'''

# maps provider prefix to logical database key
providerMap = {
	'mgi' : 1,
	'uniprot' : 13,
	'uniprotkb' : 13,
	'ncbi' : 27,
	'embl' : 41,
	'interpro' : 28,
	'go' : 1,
	'ec' : 8,
	'sp_kw' : 13,
	'protein_id' : 13,
	'rgd' : 4,
	'pir' : 78,
	'refseq' : 27,
	}

def synchronize (
	toDelete,
	toAdd
	):
	cmds = []

	# one command for each additional ID for each key

	for (key, id, provider) in toAdd:
		if provider:
			lowerProv = provider.lower()
		else:
			lowerProv = ''

		if providerMap.has_key(lowerProv):
			cmds.append (INSERT_ACC % (
				key, id, providerMap[lowerProv]))
		else:
			sys.stderr.write (
				'Unknown prefix %s, did not add %s for %d\n' \
					% (provider, id, key) )

	# reformat 'toDelete' to be a dictionary where each key (an evidence
	# key) refers to a list of all IDs we should delete for it, to
	# collapse it down into as few delete statements as possible

	byKey = {}
	for (key, id) in toDelete:
		if byKey.has_key(key):
			byKey[key].append (id)
		else:
			byKey[key] = [id]

	# assumes small (less than 250 or so) number of ids per evidence key;
	# this is a valid assumption because of the size of the varchar field
	# VOC_Evidence.inferredFrom:

	for (key, idList) in byKey.items():
		cmds.append (DELETE_ACC % (key, EVIDENCE_MGITYPE,
			'","'.join (idList) ) )

	if cmds:
		# to not overwhelm sybase, pass along the commands in small
		# batches (suspicion that passing along a hundred thousand
		# might be problematic, but not proven)

		step = 100	# how many commands to process per batch
		i = 0

		while cmds:
			sql (cmds[:step])

			cmds = cmds[step:]
			i = i + 1
			if (i % 5) == 0:
				message ('%d IDs left to go' % len(cmds))

		message ('finished processing IDs')
	else:
		message ('no IDs to process')
	return

def main():
	processCommandLine()
	loadCaches()
	toDelete, toAdd = diff ()
	synchronize (toDelete, toAdd)
	return

if __name__ == '__main__':
	main()
