#!/bin/csh -f

#
# Usage:  vocgo.csh
#
# History
#
# 10/19/2006	lec
#	- TR 6812;new
#

cd `dirname $0` && source ./Configuration

setenv TABLE VOC_GO_Cache
setenv OBJECTKEY 0

setenv LOG	${MGICACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

# Create the bcp file
${PYTHON} ./vocgo.py | tee -a ${LOG}

# Exit if bcp file is empty

if ( -z ${MGICACHEBCPDIR}/${TABLE}.bcp ) then
echo 'BCP File is empty' | tee -a ${LOG}
exit 0
endif

# truncate table

${SCHEMADIR}/table/${TABLE}_truncate.object | tee -a ${LOG}

# Drop indexes
${SCHEMADIR}/index/${TABLE}_drop.object | tee -a ${LOG}

# BCP new data into tables
${BCP_CMD} ${TABLE} ${MGICACHEBCPDIR} ${TABLE}.bcp ${COLDELIM} ${LINEDELIM} ${PG_DB_SCHEMA} | tee -a ${LOG}

# Create indexes
${SCHEMADIR}/index/${TABLE}_create.object | tee -a ${LOG}

date | tee -a ${LOG}
