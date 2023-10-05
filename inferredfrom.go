#!/bin/csh -f

#
# Usage:  inferredfrom.go
#

cd `dirname $0` && source ./Configuration

setenv LOG	${MGICACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

${PYTHON} ./inferredfrom.py -S${MGD_DBSERVER} -D${MGD_DBNAME} -U${MGD_DBUSER} -P${MGD_DBPASSWORDFILE} -B"GO_Central" |& tee -a ${LOG}

date | tee -a ${LOG}
