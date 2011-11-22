#!/bin/csh -f

#
# This is a wrapper script that is called via the runCommand from pixel DB
# when an image submission is done.  It will call the image cache load to
# update the image cache table for a specific J-Number.  This is needed to
# set up the proper environment for the cache load to run in.
#
# Usage:  imgcache_pixeldb.csh
#
# History
#
# 3/23/2007	dbm
#	- new
#

cd `dirname $0` && source ./Configuration

# Create the bcp file
./imgcache.py $*
