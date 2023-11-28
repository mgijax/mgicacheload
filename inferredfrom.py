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
# 11/27/2023    lec
#       - wts2-1155/GOC taking over GOA mouse, GOA human, etc.
#
# 01/30/2015	jsb
#	- TR 11914: make protein_ids go to RefSeq (27), not SWISS-PROT (13)
#
# 08/26/2013	lec
#	- TR 11359/add 'pr:' : 135 = ldb
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
import re
import db
import loadlib
import accessionlib

db.setTrace()

user = os.environ['MGD_DBUSER']
passwordFileName = os.environ['MGD_DBPASSWORDFILE']
outputDir = os.environ['MGICACHEBCPDIR']

accTable = 'ACC_Accession'
accFile = ''            # file descriptor
accFileName = outputDir + '/' + accTable + '.bcp'
accKey = 0              # ACC_Accession._Accession_key
mgiTypeKey = 25         # Annotation Evidence
loaddate = loadlib.loaddate
createdByKey = 1001

objectKey = -1
createdBy = None

eiErrorStatus = '%s     %s     %s     %s\n'

# maps provider prefix to logical database key
# using lowercase
# ncbi = refseq = 27: both prefixes can be used
providerMap = {
        'mgi' : 1,
        'go' : 1,
        'arba' : 228,
        'chebi' : 127, 
        'complexportal' : 211,
        'ec' : 8,
        'embl' : 9,
        'ensembl' : 214, # needs new url
        'ensembl_geneid' : 214,
        'hgnc' : 64,
        'interpro' : 28,
        'ncbi_gene' : 160, 
        'pir' : 78,
        'pr' : 135,
        'pombase' : 115,
        'protein_id' : 27,
        'refseq' : 27,
        'rgd' : 47,
        'rhea' : 229,
        'rnacentral' : 204,
        'sgd' : 114,
        'unipathway' : 230,
        'uniprotkb' : 13,
        'uniprotkb-kw' : 111,
        'uniprotkb-subcell' : 227,
        'unirule' : 231,
        }

#
#        'gb' : 9,
#        'genbank' : 9,
#        'ncbi' : 27,
#        'panther' : 147,
#        'sp_kw' : 111,
#        'uniprot' : 13,

#
# ignore these providers
#providerIgnore = [
#        'cgd',
#        'dictybase',
#        'ecogene',
#        'fb',
#        'pmid',
#        'tair',
#        'uniprotid',
#        'wb',
#        'zfin'
#]
providerIgnore = [
        'xenbase',
]

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
        #	message (str.
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

        global accFile, accKey
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
                        password = str.strip(open(opt[1], 'r').readline())
                elif opt[0] == '-K':
                        objectKey = int(opt[1])
                elif opt[0] == '-B':
                        createdBy = re.sub('"', '', opt[1])
                else:
                        showUsage()

        if server is None or \
           database is None or \
           user is None or \
           password is None or \
           (objectKey < 0 and createdBy is None):
                showUsage()

        db.set_sqlLogin(user, password, server, database)
        db.useOneConnection(1)

        # Log all SQL if runnning checker, loading all data or
        # running the load by a specific user
        if objectKey <= 0 or createdBy is not None:
                pass
                #db.set_sqlLogFunction(db.sqlLogAll)

        results = db.sql('select max(_Accession_key) + 1 as maxKey from ACC_Accession', 'auto')
        accKey = results[0]['maxKey']

        try:
            accFile = open(accFileName, 'w')
        except:
            exit(1, 'Could not open file %s\n' % accFileName)

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

        cmd = '''select a._Accession_key, a.accID, a._Object_key, c.pubmedID
                INTO TEMPORARY TABLE toCheck 
                from ACC_Accession a, VOC_Annot v, VOC_Evidence e, MGI_User u, BIB_Citation_Cache c
                where a._MGIType_key = 25 
                and v._AnnotType_key = 1000 
                and v._Annot_key = e._Annot_key 
                and a._Object_key = e._AnnotEvidence_key 
                and e._CreatedBy_key = u._User_key 
                and e._Refs_key = c._Refs_key
                '''
        
        # select by object or created by

        if objectKey > 0:
            cmd = cmd + ' and v._Object_key = %s' % (objectKey)

        elif createdBy is not None:
            cmd = cmd + ' and u.login like \'%s\'' % (createdBy)

        db.sql(cmd, None)
        db.sql('create index idx1 on toCheck(_Accession_key)', None)

        # delete existing cache data

        if objectKey >= 0 or createdBy is not None:
            db.sql('delete from ACC_Accession using toCheck d where d._Accession_key = ACC_Accession._Accession_key', None)
            db.commit()

        # copy existing cache table accession keys

        elif objectKey == -1:
            results = db.sql('select * from toCheck', 'auto')
            for r in results:
                key = r['_Object_key']
                value = r['accID']
                if key not in cacheIF:
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

        global accKey

        # retrieve GO data in VOC_Evidence table

        cmd = '''select e._AnnotEvidence_key, e.inferredFrom, m.symbol, ta.accID as goID, c.pubmedID
                from VOC_Annot a, VOC_Evidence e, MRK_Marker m, ACC_Accession ta, MGI_User u, BIB_Citation_Cache c
                where a._AnnotType_key = 1000 
                and a._Annot_key = e._Annot_key 
                and e.inferredFrom is not null 
                and a._Object_key = m._Marker_key 
                and a._Term_key = ta._Object_key 
                and ta._LogicalDB_key = 31 
                and ta._MGIType_key = 13 
                and ta.preferred = 1 
                and e._CreatedBy_key = u._User_key
                and e._Refs_key = c._Refs_key
                '''

        # select data by specific marker or by created by

        if objectKey > 0:
                cmd = cmd + ' and a._Object_key = %s' % (objectKey)

        elif createdBy is not None:
                cmd = cmd + ' and u.login like \'%s\'' % (createdBy)

        results = db.sql(cmd, 'auto')
        eiErrors = ''

        for r in results:

                key = r['_AnnotEvidence_key']
                inferredFrom = r['inferredFrom']
                symbol = r['symbol']
                goID = r['goID']
                if r['pubmedID'] != None:
                    pubmedID = r['pubmedID']
                else:
                    pubmedID = ""

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

                                # MGI, GO, RGD, PR IDs are stored
                                # with the MGI:,  GO: and RGD: prefixes
                                # for all others, we do not store the ##: part

                                fullAccID = accID
                                tokens = accID.split(':')
                                provider = tokens[0].lower()
                                accIDPart = tokens[1]

                                if accIDPart == '':
                                        eiErrors = eiErrors + eiErrorStatus % (symbol, goID, fullAccID, pubmedID)
                                        continue

                                if provider not in ['mgi', 'go', 'rgd', 'pr']:
                                        accID = accIDPart

                                if provider in providerIgnore:
                                        # just skip it
                                        #print 'skip: ', provider
                                        continue

                                # for EMBL ids, check if accession id is valid

                                if provider == 'embl':
                                        embl_result1 = embl_re1.match(accID)
                                        embl_result2 = embl_re2.match(accID)
                                        if embl_result1 is None and embl_result2 is None:
                                                eiErrors = eiErrors + eiErrorStatus % (symbol, goID, fullAccID, pubmedID)
                                                continue

                                if objectKey >= 0 or createdBy is not None:
                                        # by marker
                                        (prefixPart, numericPart) = accessionlib.split_accnum(accID)
                                        if numericPart == None:
                                                numericPart = ''
                                        accFile.write('%s|%s|%s|%s|%s|%d|%d|1|1|%s|%s|%s|%s\n' \
                                                % (accKey, accID, prefixPart, numericPart, providerMap[provider], 
                                                   key, mgiTypeKey, createdByKey, createdByKey, loaddate, loaddate))
                                        accKey = accKey + 1

                                # else, run the checker

                                else:
                                        if key in cacheIF:
                                                if accID not in cacheIF[key]:
                                                        eiErrors = eiErrors + eiErrorStatus % (symbol, goID, fullAccID, pubmedID)
                                        else:
                                                eiErrors = eiErrors + eiErrorStatus % (symbol, goID, fullAccID, pubmedID)

                        except Exception as e:
                                #print(e)
                                eiErrors = eiErrors + eiErrorStatus % (symbol, goID, fullAccID, pubmedID)

        # commit after all records have been processed
        db.commit()

        if eiErrors != '':
                print('\nThe following errors exist in the inferred-from text:\n\n' + eiErrors)

        accFile.close()

#
#
# Main Routine
#

init()
preCache()
processCache()
exit(0)
