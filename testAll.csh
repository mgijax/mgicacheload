#!/bin/csh -f

echo "Running all mgi cache loads"
foreach load ( vocgo.csh imgcache.csh inferredfrom.csh bibcitation.csh )
    setenv DB_TYPE sybase
    ./$load
    setenv DB_TYPE postgres
    ./$load
end

echo "Performing test"
python ${MGI_DBUTILS}/bin/comparePostgresTable.py VOC_GO_Cache IMG_Cache BIB_Citation_Cache

# only do count check on the following tables
python ${MGI_DBUTILS}/bin/comparePostgresTable.py -c ACC_Accession

echo "Tests successful"
