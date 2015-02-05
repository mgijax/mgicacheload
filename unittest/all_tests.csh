#!/bin/csh -f

#
# Run all unit tests
#

echo "Running gxdexpression_tests"
python gxdexpression_tests.py
if ( $status ) then
    exit 1
endif

