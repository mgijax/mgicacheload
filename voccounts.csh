#!/bin/csh

#
# Usage:  voccounts.csh
# Purpose: refreshes VOC_Annot_Count_Cache with annotation counts for vocab
#    terms; does a full delete-and-reload
# History
#    09/19/2008 - jsb - added for TR9267
#

cd `dirname $0` && source ./Configuration

setenv LOG	${MGICACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}
echo "-- drop indexes" | tee -a ${LOG}
${MGD_DBSCHEMADIR}/index/VOC_Annot_Count_Cache_drop.object | tee -a ${LOG}

date | tee -a ${LOG}
echo "-- update table rows" | tee -a ${LOG}

cat - <<EOSQL | doisql.csh ${MGD_DBSERVER} ${MGD_DBNAME} $0 | tee -a ${LOG}
exec VOC_Cache_Counts
go
EOSQL

date | tee -a ${LOG}
echo "-- create indexes" | tee -a ${LOG}
${MGD_DBSCHEMADIR}/index/VOC_Annot_Count_Cache_create.object | tee -a ${LOG}

date | tee -a ${LOG}
echo "-- finished" | tee -a ${LOG}