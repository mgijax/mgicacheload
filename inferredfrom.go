#!/bin/csh -f

#
# Usage:  inferredfrom.go
#

cd `dirname $0` && source ./Configuration

setenv TABLE ACC_Accession

setenv LOG	${MGICACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

${PYTHON} ./inferredfrom.py -S${MGD_DBSERVER} -D${MGD_DBNAME} -U${MGD_DBUSER} -P${MGD_DBPASSWORDFILE} -B"GO_%" |& tee -a ${LOG}
${BCP_CMD} ${TABLE} ${MGICACHEBCPDIR} ${TABLE}.bcp ${COLDELIM} ${LINEDELIM} ${PG_DB_SCHEMA} |& tee -a ${LOG}

date | tee -a ${LOG}
