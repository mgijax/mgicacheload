#!/bin/csh

#
# Usage:  vocmarker.csh
# Purpose: refreshes VOC_Allele_Cache with marker/term associations; does a
#    full delete-and-reload
# History
#    06/10/2010 - mhall - added for TR9983
#

cd `dirname $0` && source ./Configuration

setenv LOG	${MGICACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}
echo "-- drop indexes" | tee -a ${LOG}
${MGD_DBSCHEMADIR}/index/VOC_Allele_Cache_drop.object | tee -a ${LOG}

date | tee -a ${LOG}
echo "-- update table rows" | tee -a ${LOG}

cat - <<EOSQL | doisql.csh ${MGD_DBSERVER} ${MGD_DBNAME} $0 | tee -a ${LOG}
exec VOC_Cache_Alleles
go
EOSQL

date | tee -a ${LOG}
echo "-- create indexes" | tee -a ${LOG}
${MGD_DBSCHEMADIR}/index/VOC_Allele_Cache_create.object | tee -a ${LOG}

date | tee -a ${LOG}
echo "-- finished" | tee -a ${LOG}
