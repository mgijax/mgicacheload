#!/bin/csh -f

#
# Usage:  bibcitation.csh
#
# History
#
# 08/10/2017	lec
#	- TR12250/Lit Triage
#	
# 10/19/2006	lec
#	- TR 6812;new
#

cd `dirname $0` && source ./Configuration

setenv TABLE BIB_Citation_Cache
setenv OBJECTKEY 0

setenv LOG	${MGICACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

# Drop indexes
${SCHEMADIR}/index/${TABLE}_drop.object | tee -a ${LOG}

cat - <<EOSQL | ${PG_DBUTILS}/bin/doisql.csh $0
select * from BIB_reloadCache(-1);
EOSQL

# Create indexes
${SCHEMADIR}/index/${TABLE}_create.object | tee -a ${LOG}

cat - <<EOSQL | ${PG_DBUTILS}/bin/doisql.csh $0
select count(*) from BIB_Refs;
select count(*) from BIB_Citation_Cache;
EOSQL

date | tee -a ${LOG}
