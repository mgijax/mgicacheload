#!/bin/csh -f

#
# Run all unit tests
#

echo "Running gxdexpression_tests"
/usr/local/bin/python gxdexpression_tests.py
if ( $status ) then
    exit 1
endif

echo "Running go_annot_extensions_tests"
/usr/local/bin/python go_annot_extensions_tests.py
if ( $status ) then
    exit 1
endif

