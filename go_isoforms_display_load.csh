#!/bin/csh -f

#
# Usage:  go_isoforms_display_load.csh
#
# History
#
# 10/04/2015	kstone
#	-Initial add
#

cd `dirname $0` && source ./Configuration

setenv LOG	${MGICACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

# Run the cache load
echo "---Running isoforms display link note cache load"
./go_isoforms_display_load.py  |& tee -a ${LOG} || exit 1;


date | tee -a ${LOG}
