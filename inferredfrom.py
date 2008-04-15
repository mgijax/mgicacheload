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

MARKER_KEYS = None
ANNOT_KEY = None
VERBOSE = False
START_TIME = time.time()
EVIDENCE_MGITYPE = None

def bailout (
	err = None	# string; error message to give to user
	):
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
	global VERBOSE, MARKER_KEYS, ANNOT_KEY

	try:
		optlist, args = getopt.getopt (sys.argv[1:], 'S:D:U:P:M:A:v')
	except getopt.GetoptError:
		bailout ('Invalid command-line flag(s)')

	if len(args) > 0:
		bailout ('Too many command-line arguments')

	options = optionsToDict (optlist)

	if not options.has_key('-S'):
		bailout ('Must specify database server (-S)')
	if not options.has_key('-D'):
		bailout ('Must specify database name (-D)')
	if not options.has_key('-U'):
		bailout ('Must specify database user (-U)')
	if not options.has_key('-P'):
		bailout ('Must specify password file (-P)')
	if options.has_key('-M') and options.has_key('-A'):
		bailout ('Cannot specify both -M and -A')

	pwdFile = options['-P']
	if not os.path.exists(pwdFile):
		bailout ('Cannot find password file: %s' % pwdFile)
	try:
		fp = open(pwdFile, 'r')
		password = fp.readline().rstrip()
		fp.close()
	except:
		bailout ('Cannot read password file: %s' % pwdFile)

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
		MARKER_KEYS = [ options['-M'] ]
		message ('Running for marker %s' % MARKER_KEYS[0])
	elif options.has_key('-A'):
		ANNOT_KEY = options['-A']
		message ('Running for annotation %s' % ANNOT_KEY)
	else:
		MARKER_KEYS = getMarkerKeys()
		message ('Running for all %d annotated markers' % \
			len(MARKER_KEYS))
	return

def loadCaches():
	# load any data we are going to store in memory caches

	global EVIDENCE_MGITYPE

	results = sql ('''SELECT _MGIType_key
		FROM ACC_MGIType
		WHERE name = "Annotation Evidence"''')

	if not results:
		bailout ('Cannot find _MGIType_key for "Annotation Evidence"')
	EVIDENCE_MGITYPE = results[0]['_MGIType_key']

	message ('Populated memory cache')
	return

def getInferredFrom (markerKey = None, annotKey = None):
	# returns dictionary mapping evidence key to a dictionary which maps
	# each id to its (provider) prefix

	cmd = '''SELECT va._Annot_key, 
			ve._AnnotEvidence_key,
			ve.inferredFrom
		FROM VOC_Annot va,
			VOC_Evidence ve
		WHERE va._AnnotType_key = 1000		-- GO/Marker
			AND va._Annot_key = ve._Annot_key
			AND %s'''

	if annotKey:
		cmd = cmd % ('va._Annot_key = %s' % annotKey)
	elif markerKey:
		cmd = cmd % ('va._Object_key = %s' % markerKey)

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

def diff (
	inferredFrom		# dictionary mapping evidence key to [ids]
	):
	# compare the string entered by the user (stored in VOC_Evidence's
	# inferredFrom field, and bundled in 'inferredFrom' parameter) with
	# the cached data currently in ACC_Accession 
	# note: destroys value of 'inferredFrom' during processing

	# chunk the evidence keys into sets of up to 200, so we can be
	# flexible enough to handle any size 'inferredFrom'

	evidenceKeys = inferredFrom.keys()
	keySets = []
	while len(evidenceKeys) > 200:
		keySets.append ( evidenceKeys[:200] )
		evidenceKeys = evidenceKeys[200:]
	if evidenceKeys:
		keySets.append ( evidenceKeys )

	# retrieve existing cached IDs from ACC_Accession for each key

	toDelete = []

	for keySet in keySets:
		results = sql ('''SELECT accID,
				_Object_key
			FROM ACC_Accession
			WHERE _MGIType_key = %s
				AND _Object_key IN (%s)''' % (
					EVIDENCE_MGITYPE,
					','.join (map(str, keySet)) ) )

		for row in results:
			accID = row['accID']
			objectKey = row['_Object_key']

			# we are going to handle the diff in a single pass
			# through the set of results; for each accession ID...
			#   1. if not still in 'inferredFrom', add it to a
			#	list of ones to delete
			#   2. if it is still there, remove it from there (in
			#	memory, not in the database)
			#   3. when we are done, anything which remains in
			#	'inferredFrom' needs to be added

			if not inferredFrom[objectKey].has_key(accID):
				toDelete.append ( (objectKey, accID) )
			else:
				del inferredFrom[objectKey][accID]

	toAdd = []
	for (objectKey, idDict) in inferredFrom.items():
		for (id, provider) in idDict.items():
			if len(id.strip()) > 0:
				toAdd.append ( (objectKey, id, provider) )

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
	'MGI' : 1,
	'UniProt' : 13,
	'Uniprot' : 13,
	'UniProtKB' : 13,
	'NCBI' : 27,
	'EMBL' : 41,
	'InterPro' : 28,
	'GO' : 1,
	'EC' : 8,
	'SP_KW' : 13,
	'protein_id' : 13,
	'RGD' : 4,
	'PIR' : 78,
	'RefSeq' : 27,
	}

def synchronize (
	toDelete,
	toAdd
	):
	cmds = []

	# one command for each additional ID for each key

	for (key, id, provider) in toAdd:
		if providerMap.has_key(provider):
			cmds.append (INSERT_ACC % (
				key, id, providerMap[provider]))
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
			idList.join ('","') ))

# for debugging:
#	print '\n'.join (cmds)
#	print '-' * 60

	if cmds:
		sql(cmds)

	return

def main():
	processCommandLine()
	loadCaches()

	if ANNOT_KEY:
		infFrom = getInferredFrom (annotKey = ANNOT_KEY)
		toDelete, toAdd = diff (infFrom)
		synchronize (toDelete, toAdd)
		message ('Finished annotation %d' % ANNOT_KEY)
	else:
		i = 0
		total = len(MARKER_KEYS)

		for markerKey in MARKER_KEYS:
			infFrom = getInferredFrom (markerKey)
			toDelete, toAdd = diff (infFrom)
			synchronize (toDelete, toAdd)

			i = i + 1
			if (i % 500 == 0) or (i == total):
				message ('Finished %d of %d markers' % (i,
					total))
	return

if __name__ == '__main__':
	main()
