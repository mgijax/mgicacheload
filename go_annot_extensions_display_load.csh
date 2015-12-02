#!/bin/csh -f

#
# Usage:  go_annot_extensions_display_load.csh
#
# History
#
# 09/17/2015	kstone
#	-Initial add
#

cd `dirname $0` && source ./Configuration

setenv LOG	${MGICACHELOGDIR}/`basename $0`.log
rm -rf $LOG
touch $LOG

date | tee -a ${LOG}

# Run the cache load
echo "---Running annotation extensions display link note cache load"
./go_annot_extensions_display_load.py  |& tee -a ${LOG} || exit 1;


date | tee -a ${LOG}
