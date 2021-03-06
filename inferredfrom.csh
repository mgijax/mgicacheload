#!/bin/csh -f

#
# Usage:  inferredfrom.csh
#
# History
#
# 04/29/2008	lec
#	- TR 8633;new
#

cd `dirname $0` && source ./Configuration

setenv OBJECTKEY 0

setenv LOG	${MGICACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

${PYTHON} ./inferredfrom.py -S${MGD_DBSERVER} -D${MGD_DBNAME} -U${MGD_DBUSER} -P${MGD_DBPASSWORDFILE} -K${OBJECTKEY} |& tee -a ${LOG}

date | tee -a ${LOG}
