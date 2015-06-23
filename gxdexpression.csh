#!/bin/csh -f

#
# Usage:  gxdexpression.csh
#
# History
#
# 02/05/2015	kstone
#	-Initial add
#

cd `dirname $0` && source ./Configuration

echo "DB_TYPE = $DB_TYPE"

setenv TABLE GXD_Expression
setenv OBJECTKEY 0

setenv LOG	${MGICACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

# Create the bcp file
echo "---Creating BCP file"
./gxdexpression.py -S${MGD_DBSERVER} -D${MGD_DBNAME} -U${MGD_DBUSER} -P${MGD_DBPASSWORDFILE} -K${OBJECTKEY} |& tee -a ${LOG}

# Exit if bcp file is empty
echo "---Ensure BCP file is not empty"
setenv BCPFILE ${MGICACHEBCPDIR}/${TABLE}.bcp
echo "${BCPFILE}"
if ( ! -e ${BCPFILE} || -z ${BCPFILE} ) then
    echo 'BCP File is empty' | tee -a ${LOG}
    exit 0
endif

# truncate table
echo "---Truncating table"
${SCHEMADIR}/table/${TABLE}_truncate.object | tee -a ${LOG}

# Drop indexes
${SCHEMADIR}/index/${TABLE}_drop.object | tee -a ${LOG}

# BCP new data into tables
${BCP_CMD} ${TABLE} ${MGICACHEBCPDIR} ${TABLE}.bcp ${COLDELIM} ${LINEDELIM} ${PG_DB_SCHEMA} | tee -a ${LOG}

# Create indexes
${SCHEMADIR}/index/${TABLE}_create.object | tee -a ${LOG}

date | tee -a ${LOG}
