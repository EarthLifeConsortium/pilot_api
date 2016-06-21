#!/opt/local/bin/perl
# 
# Neotoma Data Service
# 
# This program reads configuration information from the file 'config.yml' and
# then launches the 'starman' web server to provide a data service for the
# Neotoma database.
# 
# The relevant configuration parameters are:
# 
# port - port on which to listen
# workers - how many active data service processes to maintain
# 



use strict;

use Dancer ':script';


my $PORT = config->{port}|| 3050;
my $WORKERS = config->{workers} || 5;
my $ACCESS_LOG = config->{access_log} || 'access_log';
my $ERROR_LOG = config->{error_log} || 'error_log';

unless ( $ACCESS_LOG =~ qr{/} )
{
    $ACCESS_LOG = "logs/$ACCESS_LOG";
}

unless ( $ERROR_LOG =~ qr{/} )
{
    $ERROR_LOG = "logs/$ERROR_LOG";
}

exec('/opt/local/bin/starman', 
     '--listen', ":$PORT", '--workers', $WORKERS, '--access-log', $ACCESS_LOG, '--error-log', $ERROR_LOG,
     'bin/web_composite.pl')
    
    or die "Could not run program /opt/local/bin/starman: $!";







