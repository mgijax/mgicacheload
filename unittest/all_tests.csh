#!/bin/csh -f

#
# Run all unit tests
#

echo "Running gxdexpression_tests"
/usr/local/bin/python gxdexpression_tests.py
if ( $status ) then
    exit 1
endif

