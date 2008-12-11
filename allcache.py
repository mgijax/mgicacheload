#!/usr/local/bin/python

# Purpose: update the contents of the ALL_Cache table, either fully or for
#	specific subsets as given in the command-line parameters
# Usage: see USAGE variable below

# History:
# 12/08/2008 - jsb - initial addition for gene trap LF (TR7493) release

import os
import sys
import getopt
import ignoreDeprecation
import db
import Profiler
import mgi_utils

###------------------------###
###--- Global Variables ---###
###------------------------###

if os.environ.has_key ('MGICACHEBCPDIR'):
	BCPFILE = os.environ['MGICACHEBCPDIR'] + '/ALL_Cache.bcp'
else:
	BCPFILE = './ALL_Cache.bcp'

USAGE = '''Usage: %s [-v] -S <server> -D <database> -U <user> -P <password file> [ -G <genotype key> || -A <allele key> || -M <marker key> ]
    Purpose:
    	Refresh the contents of the ALL_Cache database table.
    Required Parameters:
    	-S : database server
	-D : database
	-U : database username
	-P : file containing password for user specified by -U
    Optional Parameters:
	-v : verbose mode; gives profiling report at conclusion of run
	-G : genotype key; rebuilds cache for all alleles in that genotype
	-A : allele key; rebuilds cache for only the specified allele
	-M : marker key; rebuilds cache for all alleles of specified marker
    Notes:
    	1. At most one of -G, -A, and -M may be specified.
	2. If none of -G, -A, or -M are specified, then we will write out a
	   file which can be bcp-ed in to refresh the whole table.  Location:
	   %s
''' % (sys.argv[0], BCPFILE)

PROFILER = Profiler.Profiler()
SERVER = None
DATABASE = None
USER = None
PASSWORD = None
GENOTYPE_KEY = None
ALLELE_KEY = None
MARKER_KEY = None
VERBOSE = False
TRANSMISSION = None
EXISTS_AS = None

###-----------------###
###--- Functions ---###
###-----------------###

def bailout (message, showUsage = True):
	if showUsage:
		sys.stderr.write (USAGE + '\n')
	sys.stderr.write ('Error: %s\n' % message)
	sys.exit(1)
	return

def timestamp (message):
	PROFILER.stamp (message)
	return

def checkKey (table, field, key, name):
	if not key:
		return
	try:
		results = db.sql ('SELECT %s FROM %s WHERE %s = %d' % (
			field, table, field, key), 'auto')
	except:
		bailout ('Cannot query database %s..%s using given username and password file.' % (SERVER, DATABASE))

	if not results:
		bailout ('The given %s key (%d) is not in the database' % (
			name, key))
	return

def processCommandLine():
	global SERVER, DATABASE, USER, PASSWORD, GENOTYPE_KEY, VERBOSE
	global ALLELE_KEY, MARKER_KEY

	# extract command-line parameters

	try:
		optlist, args = getopt.getopt(sys.argv[1:], 'S:D:U:P:G:A:M:v')
	except getopt.GetoptError:
		bailout ('Invalid command-line option')

	# look for any extra parameters

	if len(args) != 0:
		bailout ('No extra command-line parameters are allowed.')

	# get values for the parameters we are expecting

	for (option, value) in optlist:
		if option == '-v':
			VERBOSE = True
		elif option == '-S':
			SERVER = value
		elif option == '-D':
			DATABASE = value
		elif option == '-U':
			USER = value
		elif option == '-P':
			try:
				fp = open (value, 'r')
				PASSWORD = fp.readline().strip()
				fp.close()
			except:
				bailout ('Cannot read file: %s' % value)
		elif option == '-G':
			try:
				GENOTYPE_KEY = int(value)
			except:
				bailout ('Non-integer genotype key: %s' % \
					value)
		elif option == '-A':
			try:
				ALLELE_KEY = int(value)
			except:
				bailout ('Non-integer allele key: %s' % value)
		elif option == '-M':
			try:
				MARKER_KEY = int(value)
			except:
				bailout ('Non-integer marker key: %s' % value)

	# ensure that required parameters were included

	missingParms = []
	for (value, flag) in [ (SERVER, '-S'), (DATABASE, '-D'),
			(USER, '-U'), (PASSWORD, '-P') ]:
		if not value:
			missingParms.append (flag)
	if missingParms:
		bailout ('Required parameters were missing: %s' % \
			', '.join (missingParms))

	# login to the database

	db.set_sqlLogin (USER, PASSWORD, SERVER, DATABASE)
	timestamp ('Processed command-line')

	# verify any keys provided

	checkKey ('GXD_Genotype', '_Genotype_key', GENOTYPE_KEY, 'genotype')
	checkKey ('MRK_Marker', '_Marker_key', MARKER_KEY, 'marker')
	checkKey ('ALL_Allele', '_Allele_key', ALLELE_KEY, 'allele')
	timestamp ('Verified keys')
	return

def finalReport():
	if VERBOSE:
		PROFILER.write()
	return

def getTransmission (term):
	global TRANSMISSION

	if not TRANSMISSION:
		cmd = '''SELECT t.term, t._Term_key, t.abbreviation
			FROM VOC_Vocab v, VOC_Term t
			WHERE v._Vocab_key = t._Vocab_key
				AND v.name = "Allele Transmission"'''
		results = db.sql (cmd, 'auto')
		TRANSMISSION = {}
		for row in results:
			TRANSMISSION[row['term']] = row['_Term_key']
			TRANSMISSION[row['abbreviation']] = row['_Term_key']
			TRANSMISSION[row['_Term_key']] = row['term']
			TRANSMISSION[row['term'].lower()] = row['_Term_key']

		timestamp ('Got %d transmission terms' % len(results))

	if TRANSMISSION.has_key(term):
		return TRANSMISSION[term]
	return None

def getExistsAs (term):
	global EXISTS_AS

	if not EXISTS_AS:
		cmd = '''SELECT t.term, t._Term_key, t.abbreviation
			FROM VOC_Vocab v, VOC_Term t
			WHERE v._Vocab_key = t._Vocab_key
				AND v.name = "Genotype Exists As"'''
		results = db.sql (cmd, 'auto')
		EXISTS_AS = {}
		for row in results:
			EXISTS_AS[row['term']] = row['_Term_key']
			EXISTS_AS[row['abbreviation']] = row['_Term_key']
			EXISTS_AS[row['_Term_key']] = row['term']
			EXISTS_AS[row['term'].lower()] = row['_Term_key']

		timestamp ('Got %d Exists As terms' % len(results))

	if EXISTS_AS.has_key(term):
		return EXISTS_AS[term]
	return None

def findAlleleKeys():
	alleles = []

	if ALLELE_KEY:
		alleles.append (ALLELE_KEY)

	elif GENOTYPE_KEY or MARKER_KEY:
		if GENOTYPE_KEY:
			cmd = '''SELECT DISTINCT _Allele_key
				FROM GXD_AlleleGenotype
				WHERE _Genotype_key = %d''' % GENOTYPE_KEY
		else:
			cmd = '''SELECT DISTINCT _Allele_key
				FROM ALL_Marker_Assoc
				WHERE _Marker_key = %d''' % MARKER_KEY

		results = db.sql (cmd, 'auto')
		for row in results:
			alleles.append (row['_Allele_key'])

		if not alleles:
			# specified genotype or marker has no alleles, so
			# nothing to do

			timestamp ('No alleles...  Finished.')
			finalReport()
			sys.exit(0)

	timestamp ('Got %d allele(s)' % len(alleles))
	return alleles

def splitList (items, size = 200):
	if len(items) <= size:
		return [ items ]
	return [ items[:size] ] + splitList(items[size:], size)

def handleDirectCuration (doAll, alleles):
	# rules 1 & 2 - find alleles which have references curated directly
	# to alleles for germ line transmission and chimera generation

	global TRANSMISSION

	# find the two association types we're going to be looking for

	cmd1 = '''SELECT _RefAssocType_key, assocType
		FROM MGI_RefAssocType
		WHERE _MGIType_key = 11				-- allele
			AND assocType IN ("Chimera Generation",
				"Germ Line Transmission")'''

	results = db.sql (cmd1, 'auto')

	# these values should be overridden with results from cmd1, but in
	# case the association types are not in the database, using bogus
	# initial values will still let the code work and just will not find
	# any curated reference assocations

	germLineKey = -500
	chimeraKey = -501

	for row in results:
		if row['assocType'] == 'Chimera Generation':
			chimeraKey = row['_RefAssocType_key']
		elif row['assocType'] == 'Germ Line Transmission':
			germLineKey = row['_RefAssocType_key']

	# augment the global TRANSMISSION dictionary

	TRANSMISSION[chimeraKey] = getTransmission("1stChimera")
	TRANSMISSION[germLineKey] = getTransmission("1stGermLine")

	# basic command to retrieve all reference assocations for alleles for
	# either of the two types we are seeking

	cmd = '''SELECT _Object_key AS _Allele_key,
			_RefAssocType_key,
			_Refs_key
		FROM MGI_Reference_Assoc
		WHERE _RefAssocType_key IN (%d, %d)''' % \
			(chimeraKey, germLineKey)

	# We will use a list of commands to retrieve the data from the
	# database, so that if we have a large number of alleles, we can break
	# them into multiple queries.

	cmds = []

	if doAll:
		cmds.append (cmd)	# retrieve for all alleles
	elif not alleles:
		return {}, alleles	# no alleles left to do
	else:
		cmds.append (cmd + ' AND _Object_key IN (%s)' % \
			','.join (map (str, alleles)) )

	results = db.sql (cmds, 'auto')
	dict = {}		# allele key -> best (assoc type, refs key,
				#	transmission key)

	for resultSet in results:
		for row in resultSet:
			alleleKey = row['_Allele_key']
			assocType = row['_RefAssocType_key']
			refsKey = row['_Refs_key']

			# a germ line association is preferred over a chimera
			# association

			if not dict.has_key (alleleKey):
				dict[alleleKey] = (assocType, refsKey,
					getTransmission(assocType))
			elif (dict[alleleKey][0] == chimeraKey) and \
					(assocType == germLineKey):
				dict[alleleKey] = (assocType, refsKey,
					getTransmission(assocType))

	# If we were given a list of alleles, then we need to return a new
	# list which contains alleles for which we didn't find data in this
	# set of curated references.  This is the remaining to-do list.

	newAlleles = []
	for key in alleles:
		if not dict.has_key(key):
			newAlleles.append (key)

	timestamp ('Got %d alleles w/ curated refs' % len(dict))

	return dict, newAlleles

def handleNoCellLines (doAll, alleles):
	# rule 3 - find alleles with no mutant cell lines which have IDs

	naKey = getTransmission ('Not Applicable')
	if not naKey:
		return {}, alleles	# Not Applicable key is not known

	cmd = '''SELECT a._Allele_key
		FROM ALL_Allele a
		WHERE NOT EXISTS (SELECT 1
			FROM ALL_Allele_CellLine c, ACC_Accession acc
			WHERE a._Allele_key = c._Allele_key
				AND c._MutantCellLine_key = acc._Object_key
				AND acc._MGIType_key = 28	-- cell line
				AND acc.private = 0)'''

	cmds = []
	if doAll:
		cmds.append (cmd)
	elif not alleles:
		return {}, alleles	# no alleles left to do
	else:
		cmds.append (cmd + ' AND a._Allele_key IN (%s)' % \
			','.join (map (str, alleles)) )

	results = db.sql (cmds, 'auto')
	dict = {}		# allele key -> best (assoc type, refs key,
				#	transmission key)

	for resultSet in results:
		for row in resultSet:
			dict[row['_Allele_key']] = (None, naKey, None)

	newAlleles = []
	for key in alleles:
		if not dict.has_key(key):
			newAlleles.append (key)

	timestamp ('Got %d alleles with no MCL IDs' % len(dict))

	return dict, newAlleles

def handleMPAnnotations (doAll, alleles):
	# rules 4 & 5 - MP annotations for mouse line & chimera genotypes

	cmd = '''SELECT a._Allele_key, br._Refs_key
		FROM GXD_AlleleGenotype a,
			GXD_Genotype g,
			VOC_Annot va,
			VOC_Evidence ve,
			BIB_Refs br,
			ACC_Accession aa
		WHERE g._ExistsAs_key = %s
			AND a._Genotype_key = g._Genotype_key
			AND g._Genotype_key = va._Object_key
			AND va._AnnotType_key = 1002		-- geno/MP
			AND va._Annot_key = ve._Annot_key
			AND ve._Refs_key = br._Refs_key
			AND br._Refs_key = aa._Object_key
			AND aa._MGIType_key = 1			-- ref
			AND aa._LogicalDB_key = 1		-- MGI
			AND aa.prefixPart = "J:"
			%s
		ORDER BY a._Allele_key, br.year, aa.numericPart'''

	if not doAll:
		if not alleles:
			return {}, []
		cmd = cmd % ('%d', 'AND a._Allele_key IN (%s)' % 
			','.join (map (str, alleles)))
	else:
		cmd = cmd % ('%d', '')

	cmd1 = cmd % getExistsAs ('Mouse Line')
	cmd2 = cmd % getExistsAs ('Chimeric')

	germLineKey = getTransmission('1stGermLine')
	chimeraKey = getTransmission('1stChimeric')

	dict = {}

	results = db.sql (cmd1, 'auto')
	for row in results:
		alleleKey = row['_Allele_key']
		if dict.has_key(alleleKey):
			continue
		else:
			dict[alleleKey] = (None,germLineKey, row['_Refs_key'])

	results = db.sql (cmd2, 'auto')
	for row in results:
		alleleKey = row['_Allele_key']
		if dict.has_key(alleleKey):
			continue
		else:
			dict[alleleKey] = (None, chimeraKey, row['_Refs_key'])

	newAlleles = []
	for key in alleles:
		if not dict.has_key(key):
			newAlleles.append (key)

	timestamp ('Got MP values for %d alleles' % len(dict))
	return dict, newAlleles

def handleMixed (doAll, alleles):
	cmd = '''SELECT a._Allele_key
		FROM ALL_Allele_CellLine a, ALL_CellLine c
		WHERE a._MutantCellLine_key = c._CellLine_key
			AND c.isMixed = 1'''

	if alleles:
		cmd = cmd + ' AND a._Allele_key IN (%s)' % \
			','.join (map (str, alleles) )

	results = db.sql (cmd, 'auto')

	dict = {}
	for row in results:
		dict[row['_Allele_key']] = 1
	return dict

def handleUnknowns (doAll, unknowns, curated, notApplicable, mpAnnot):
	dict = {}
	unknownKey = getTransmission ("Unknown")

	if not doAll:
		for key in unknowns:
			dict[key] = (None, unknownKey, None)
	else:
		results = db.sql('SELECT _Allele_key FROM ALL_Allele', 'auto')
		for row in results:
			alleleKey = row['_Allele_key']

			if curated.has_key(alleleKey):
				continue
			if notApplicable.has_key(alleleKey):
				continue
			if mpAnnot.has_key(alleleKey):
				continue

			dict[alleleKey] = (None, unknownKey, None)

	if dict:
		timestamp ('Got unknowns for %d alleles' % len(dict))
	return dict

def applyDifference (unified):
	cmd1 = 'SELECT * FROM ALL_Cache WHERE _Allele_key IN (%s)'

	alleles = unified.keys()

	changed = {}
	added = {}
	removed = {}

	added.update (unified)

	sublists = splitList (alleles)
	for sublist in sublists:
		results = db.sql (cmd1 % ','.join(map (str, sublist)), 'auto')

		for row in results:
			alleleKey = row['_Allele_key']
			transKey = row['_Transmission_key']
			refsKey = row['_TransmissionRefs_key']
			isMixed = row['isMixed']

			trio = (transKey, refsKey, isMixed)

			if not unified.has_key (alleleKey):
				removed[alleleKey] = trio
			elif unified[alleleKey] != trio:
				changed[alleleKey] = trio
				del added[alleleKey]
			else:
				del added[alleleKey]

	timestamp ('To do: %d remove, %d change, %d add' % \
		(len(removed), len(changed), len(added)) )

	cmds = []
	if removed:
		alleleKeys = removed.keys()
		for sublist in splitList(alleleKeys):
			cmds.append ('''DELETE FROM ALL_Cache
				WHERE _Allele_key IN (%s)''' % \
					','.join (map (str, sublist)) )
	if changed:
		cmd = '''UPDATE ALL_Cache
			SET _Transmission_key = %d,
				_TransmissionRefs_key = %s,
				isMixed = %d
			WHERE _Allele_key = %d'''

		for (key, (transKey, refsKey, isMixed)) in changed.items():
			if refsKey == None:
				refsKey = "null"
			cmds.append (cmd % (transKey, refsKey, isMixed, key))
	if added:
		cmd = 'INSERT ALL_Cache VALUES (%d, %d, %s, %d)'

		for (key, (transKey, refsKey, isMixed)) in added.items():
			if refsKey == None:
				refsKey = "null"
			cmds.append (cmd % (key, transKey, refsKey, isMixed))

	if cmds:
		db.sql (cmds, 'auto')
		timestamp ('Applied differences to database')
	else:
		timestamp ('No differences to apply')
	return

def writeBcp (unified):
	if os.environ.has_key('COLDELIM'):
		columnDelim = os.environ['COLDELIM']
	else:
		columnDelim = '\t'

	fp = open (BCPFILE, 'w')
	for (key, (transKey, refsKey, isMixed)) in unified.items():
		fp.write (mgi_utils.prvalue(key) + columnDelim + \
			mgi_utils.prvalue(transKey) + columnDelim + \
			mgi_utils.prvalue(refsKey) + columnDelim + \
			mgi_utils.prvalue(isMixed) + '\n')
	fp.flush()
	fp.close()

	timestamp ('Wrote %d alleles to bcp file' % len(unified))
	return

def main():
	doAll = False

	processCommandLine()
	alleles = findAlleleKeys()

	if not alleles:			# if empty [], must re-do whole table
		doAll = True
		alleles = [ [] ]
	else:
		alleles = splitList (alleles)

	curated = {}
	notApplicable = {}
	mpAnnot = {}
	mixed = {}

	unknowns = []

	for sublist in alleles:
		tmpCurated, sublist = handleDirectCuration (doAll, sublist)
		tmpNotApplicable, sublist = handleNoCellLines (doAll, sublist)
		tmpMpAnnot, sublist = handleMPAnnotations (doAll, sublist)
		tmpMixed = handleMixed (doAll, sublist)

		curated.update (tmpCurated)
		notApplicable.update (tmpNotApplicable)
		mpAnnot.update (tmpMpAnnot)
		mixed.update (tmpMixed)

		if sublist:
			unknowns = unknowns + sublist

	unknowns = handleUnknowns (doAll, unknowns, curated, notApplicable,
		mpAnnot)

	timestamp ('Got %d w/ mixed cell lines' % len(mixed))

	# compile into a unified dictionary by allele key

	temp = {}
	temp.update (unknowns)		# rule 6
	temp.update (mpAnnot)		# rules 4 & 5
	temp.update (notApplicable)	# rule 3
	temp.update (curated)		# rules 1 & 2

	unified = {}

	for (key, (assocType, transmissionKey, refsKey)) in temp.items():
		if mixed.has_key (key):
			unified[key] = (transmissionKey, refsKey, 1)
		else:
			unified[key] = (transmissionKey, refsKey, 0)

	timestamp ('Unified results for %d alleles' % len(unified))

	if not doAll:
		applyDifference (unified)
	else:
		writeBcp (unified)

	finalReport()
	return

###--------------------###
###--- Main Program ---###
###--------------------###

if __name__ == '__main__':
	main()
