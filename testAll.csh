#!/bin/csh -f

echo "Running all mgi cache loads"
foreach load ( vocgo.csh )
    setenv DB_TYPE sybase
    ./$load
    setenv DB_TYPE postgres
    ./$load
end

echo "Performing test"
python ${MGD_DBUTILS}/bin/comparePostgresTable.py VOC_GO_Cache

echo "Tests successful"
