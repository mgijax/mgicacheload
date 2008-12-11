#!/bin/csh

# Usage:  allcache.csh
#
# History
# 12/10/2008 - jsb - added for gene trap LF release (TR7493)
#	(copied imgcache.csh and modified it)

cd `dirname $0` && source ./Configuration

setenv TABLE ALL_Cache

setenv LOG	${MGICACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

# Create the bcp file
./allcache.py -S${MGD_DBSERVER} -D${MGD_DBNAME} -U${MGD_DBUSER} -P${MGD_DBPASSWORDFILE} -v |& tee -a ${LOG}

# Exit if there was a failure creating the bcp file
if ( $status ) then
    echo 'Allele cache load failed' | tee -a ${LOG}
    exit 1
endif

# Exit if bcp file is empty
if ( -z ${MGICACHEBCPDIR}/${TABLE}.bcp ) then
    echo 'BCP File is empty' | tee -a ${LOG}
    exit 0
endif

# Truncate table
${MGD_DBSCHEMADIR}/table/${TABLE}_truncate.object | tee -a ${LOG}

# Drop indexes
${MGD_DBSCHEMADIR}/index/${TABLE}_drop.object | tee -a ${LOG}

# BCP new data into tables
${MGI_DBUTILS}/bin/bcpin.csh ${MGD_DBSERVER} ${MGD_DBNAME} ${TABLE} ${MGICACHEBCPDIR} ${TABLE}.bcp ${COLDELIM} ${LINEDELIM} | tee -a ${LOG}

# Create indexes
${MGD_DBSCHEMADIR}/index/${TABLE}_create.object | tee -a ${LOG}

date | tee -a ${LOG}
