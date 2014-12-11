#!/bin/csh -f

#
# Usage:  imgcache.csh
#
# History
#
# 10/19/2006	lec
#	- TR 6812;new
#

cd `dirname $0` && source ./Configuration

setenv TABLE IMG_Cache
setenv OBJECTKEY 0

setenv LOG	${MGICACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

# Create the bcp file
if ( ${DB_TYPE} == "postgres" ) then
./imgcache.py -S${PG_DBSERVER} -D${PG_DBNAME} -U${PG_DBUSER} -P${PG_1LINE_PASSFILE} -K${OBJECTKEY} |& tee -a ${LOG}
else
./imgcache.py -S${MGD_DBSERVER} -D${MGD_DBNAME} -U${MGD_DBUSER} -P${MGD_DBPASSWORDFILE} -K${OBJECTKEY} |& tee -a ${LOG}
endif

# Exit if there was a failure creating the bcp file
if ( $status ) then
    echo 'Image cache load failed' | tee -a ${LOG}
    exit 1
endif

# Exit if bcp file is empty
if ( -z ${MGICACHEBCPDIR}/${TABLE}.bcp ) then
    echo 'BCP File is empty' | tee -a ${LOG}
    exit 0
endif

# Truncate table
${SCHEMADIR}/table/${TABLE}_truncate.object | tee -a ${LOG}

# Drop indexes
${SCHEMADIR}/index/${TABLE}_drop.object | tee -a ${LOG}

# BCP new data into tables
${BCP_CMD} ${TABLE} ${MGICACHEBCPDIR} ${TABLE}.bcp ${COLDELIM} ${LINEDELIM} ${PG_DB_SCHEMA} | tee -a ${LOG}

# Create indexes
${SCHEMADIR}/index/${TABLE}_create.object | tee -a ${LOG}

date | tee -a ${LOG}
