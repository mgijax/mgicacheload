#!/usr/local/bin/python

# Purpose: to examine the accession IDs associated with a GO annotation and
#	compare them to the IDs specified in that annotation's "Inferred From"
#	field (in VOC_Evidence)
# Usage: see USAGE variable below
# History:
#	5/2/08 - jsb - introduced for TR8633 in MGI 4.B release

import sys
import getopt
import os
import db
import time
import string
import mgi_utils

USAGE = '''Usage: %s <parameters>
    Checks that "inferred from" IDs are cached properly in ACC_Accession, and
    reported discrepancies to stderr.

    Required parameters:
	-S <server>   : name of the database server
	-D <database> : name of the database within that server
	-U <user>     : database username
	-P <pwd file> : path to file containing that user's database password
    Optional parameters:
	-M <marker key>     : verify all annotations for specified marker
	-A <annotation key> : verify only specified annotation
    Notes:
    	1. At most one of -M or -A may be specified.
	2. If neither -M or -A are specified, we verify all GO annotations.
''' % sys.argv[0]

MARKER_KEY = None		# string; single _Marker_keys to process
ANNOT_KEY = None		# string; single _Annot_key to process
START_TIME = time.time()	# float; time in seconds at which script began
EVIDENCE_MGITYPE = 25		# int; _MGIType_key for "Annotation Evidence"

# dictionary mapping evidence key to its VOC_Evidence.inferredFrom field
INFERRED_FROM = None

# dictionary mapping an evidence key to a space-delimited string containing
# its IDs from ACC_Accession
IDS = None

# dictionary mapping an evidence key to info about its relevant GO term and
# MGI marker
INFO = None

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
	sys.stderr.write ('%8.3f %s\n' % (time.time() - START_TIME, msg))
	return

def processCommandLine ():
	global MARKER_KEY, ANNOT_KEY

	try:
		optlist, args = getopt.getopt (sys.argv[1:], 'S:D:U:P:M:A:')
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
	return

def loadCaches():
	# load any data we are going to store in memory caches

	global INFERRED_FROM, IDS, INFO

	# get data from VOC_Evidence.inferredFrom

	if ANNOT_KEY != None:
		INFERRED_FROM = getInferredFrom (annotKey = ANNOT_KEY)
	elif MARKER_KEY != None:
		INFERRED_FROM = getInferredFrom (markerKey = MARKER_KEY)
	else:
		INFERRED_FROM = getInferredFrom()
	message ('Looked up "inferredFrom" values (%d evidence keys)' % \
		len(INFERRED_FROM))

	# get already-cached IDs from ACC_Accession

	IDS = getIds()

	message ('Looked up existing cached IDs (%d evidence keys)' % \
		len(IDS))

	# look up annotation data for each evidence record

	results = sql ('''SELECT e._AnnotEvidence_key,
				t.term,
				aa.accID AS termID,
				m.symbol,
				aa2.accID as markerID
			FROM VOC_Evidence e,
				VOC_Annot a,
				VOC_Term t,
				MRK_Marker m,
				ACC_Accession aa,
				ACC_Accession aa2
			WHERE a._AnnotType_key = 1000		-- GO/Marker
				AND a._Annot_key = e._Annot_key
				AND a._Term_key = t._Term_key
				AND a._Object_key = m._Marker_key
				AND a._Object_key = aa2._Object_key
				AND aa2._LogicalDB_key = 1	-- MGI
				AND aa2._MGIType_key = 2	-- marker
				AND aa2.preferred = 1
				AND a._Term_key = aa._Object_key
				AND aa._LogicalDB_key = 31	-- GO
				AND aa._MGIType_key = 13	-- vocab term
				AND aa.preferred = 1''')
	INFO = {}
	for row in results:
		INFO[row['_AnnotEvidence_key']] = row

	message ('Looked up annotation data (%d evidence keys)' % \
		len(INFO))
	return

def getInferredFrom (markerKey = None, annotKey = None):
	# returns dictionary mapping evidence key to a string (which is its
	# VOC_Evidence.inferredFrom field)

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
		key2ids[row['_AnnotEvidence_key']] = row['inferredFrom']

	return key2ids

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

	dict = {}
	for row in ids:
		key = row['_Object_key']
		if dict.has_key(key):
			dict[key] = dict[key] + ' ' + row['accID']
		else:
			dict[key] = row['accID']

	return dict

def diff ():
	# Compare the string entered by the user (stored in VOC_Evidence's
	# inferredFrom field, and bundled in 'inferredFrom' parameter) with
	# the cached data currently in ACC_Accession.  Write discrepancies
	# out to stderr.

	notCached = 'None in acc table for %s (%s) / %s (%s) / evidence key %d / (%s)'
	missed1 = '%s not in acc table for %s (%s) / %s (%s) / evidence key %d'
	extra1 = '%s not in evid table for %s (%s) / %s (%s) / evidence key %d'
	notKnown = 'None in evid table for %s (%s) / %s (%s) / evidence key %d'
	gone = 'None in evid table for evidence key %d'

	noIdInEvid = 0
	noIdInAcc = 0
	noKeyInEvid = 0
	noKeyInAcc = 0

	# first check the VOC_Evidence.inferredFrom data, that each ID is
	# represented in ACC_Accession

	for evKey in INFERRED_FROM.keys():
		inferredFrom = INFERRED_FROM[evKey]
		
		if IDS.has_key(evKey):
			storedIds = IDS[evKey]
		elif inferredFrom:
			info = INFO[evKey]
			message (notCached % (info['termID'], info['term'],
				info['markerID'], info['symbol'], evKey,
				inferredFrom))
			noKeyInAcc = noKeyInAcc + 1
			continue
		else:
			continue

		if inferredFrom.find('|') >= 0:
			ids = inferredFrom.split('|')
		elif inferredFrom.find(',') >= 0:
			ids = inferredFrom.split(',')
		else:
			ids = [ inferredFrom ]

		ids = map (string.strip, ids)

		for id in ids:
			origID = id
			colonPos = id.find(':')
			if colonPos >= 0:
				id = id[colonPos+1:]
			if storedIds.find (id) < 0:
				message (missed1 % (origID, info['termID'],
					info['term'], info['markerID'],
					info['symbol'], evKey))
				noIdInAcc = noIdInAcc + 1
	for evKey in IDS.keys():
		if not INFERRED_FROM.has_key (evKey):
			if INFO.has_key(evKey):
				info = INFO[evKey]
				message (notKnown % (info['termID'],
					info['term'], info['markerID'],
					info['symbol'], evKey))
				noKeyInEvid = noKeyInEvid + 1
			else:
				message (gone % evKey)
			continue
		else:
			inferredFrom = INFERRED_FROM[evKey]
			info = INFO[evKey]

		ids = IDS[evKey].split(' ')
		for id in ids:
			if inferredFrom.find (id) < 0:
				message (extra1 % (id, info['termID'],
					info['term'], info['markerID'],
					info['symbol'], evKey))
				noIdInEvid = noIdInEvid + 1
	if noKeyInAcc:
		message ('%d evidence keys missing from ACC_Accession' % \
			noKeyInAcc)
	if noIdInAcc:
		message ('%d IDs missing from ACC_Accession' % noIdInAcc)
	if noKeyInEvid:
		message ('%d evidence keys missing from VOC_Evidence' % \
			noKeyInEvid)
	if noIdInEvid:
		message ('%d extra IDs in ACC_Accession' % noIdInEvid)
	return

def main():
	processCommandLine()
	loadCaches()
	diff()
	return

if __name__ == '__main__':
	main()
