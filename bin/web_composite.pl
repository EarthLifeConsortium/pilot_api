#!/opt/local/bin/perl
# 
# Paleobiology Data Services
# 
# This application provides data services that query the Paleobiology Database
# (MySQL version).  It is implemented using the Perl Dancer framework.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>

use lib './lib';

use Dancer;
use Dancer::Plugin::Database;
use Dancer::Plugin::StreamData;
use Template;
use Web::DataService;

use CompositeService;
use Main;
    
dance;

