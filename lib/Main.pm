# 
# Neotoma Example Data Service
# 
# This application configures a data service that queries the Neotoma
# Database (MySQL version).  It is implemented using the Perl Dancer
# framework.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>

use strict;

use Dancer qw(:syntax);


# If we were called from the command line with one or more arguments, then
# assume that we have been called for debugging purposes.  This does not count
# the standard options accepted by Dancer, such as "--confdir" and "--port",
# which are handled before we ever get to this point.

BEGIN {

    Web::DataService->VERSION(0.26);
    
    set log => 'warning';
    
    # If the environment variable "DSDEBUG" is true, then pretend that we got
    # a command-line argument.
    
    # If we were given a command-line argument, figure out what to do with it.
    
    if ( defined $ARGV[0] || defined $ENV{DSDEBUG} )
    {
	my $cmd = '';
	$cmd = lc $ARGV[0] if $ARGV[0];
	$cmd ||= 'debug' if $ENV{DSDEBUG};
	
	# If the first command-line argument specifies an HTTP method
	# (i.e. 'get') then set Dancer's apphandler to 'Debug'.  This will
	# cause Dancer to process a single request using the command-line
	# arguments and then exit.
	
	# In this case, the second argument must be the route path.  The third
	# argument if given should be a query string
	# 'param=value&param=value...'.  Any subsequent arguments should be of
	# the form 'var=value' and are used to set environment variables that
	# would otherwise be set by Plack from HTTP request headers.
	
	if ( $cmd eq 'get' || $cmd eq 'head' || $cmd eq 'put' || $cmd eq 'post' || $cmd eq 'delete' )
	{
	    set apphandler => 'Debug';
	    set logger => 'console';
	    set show_errors => 0;
	    
	    Web::DataService->set_mode('debug', 'one_request');
	    $Web::DataService::ONE_PROCESS = 1;
	}
	
	# If the command-line argument is 'diag' then set a flag to indicate
	# that Web::DataService should print out information about the
	# configuration of this data service application and then exit.  This
	# function can be used to debug the configuration.
	
	# This option is deliberately made available only via the command-line
	# for security reasons.
	
	elsif ( $cmd eq 'diag' )
	{
	    set apphandler => 'Debug';
	    set logger => 'console';
	    set show_errors => 0;
	    set startup_info => 0;
	    
	    Web::DataService->set_mode('diagnostic');
	    
	    # We need to alter the first argument to 'get' so that the Dancer
	    # routing algorithm will recognize it.
	    
	    $ARGV[0] = 'GET';
	}
	
	# Otherwise, if the command-line argument is 'debug' then we run in
	# the regular mode (accepting requests from a network port) but put
	# Web::DataService into debug mode.  This will cause debugging output
	# to be printed to STDERR for eqch requests.  If the additional
	# argument 'oneproc' is given, then set the 'ONE_PROCESS' flag.  This
	# tells the data operation modules that it is safe to use permanent
	# rather than temporary tables for some operations, so that we can
	# debug what is going on.
	
	elsif ( $cmd eq 'debug' )
	{
	    Web::DataService->set_mode('debug');
	    $Web::DataService::ONE_PROCESS = 1 if defined $ARGV[1] and lc $ARGV[1] eq 'oneproc';
	}
    }
}

# A single route is all we need in order to handle all requests.

any qr{.*} => sub {
    
    if ( exists params->{noheader} )
    {
	params->{header} = "no";
    }
    elsif ( exists params->{textresult} )
    {
	params->{save} = "no";
    }
    
    if ( request->path =~ qr{^([\S]+)/([\d]+)[.](\w+)$}xs )
    {
	my $newpath = "$1/single.$3";
	my $id = $2;
	
	params->{id} = $id;
	forward($newpath);
    }
    
    return Web::DataService->handle_request(request);
};


# If an error occurs, we want to generate a Web::DataService response rather
# than the default Dancer response.  In order for this to happen, we need the
# following two hooks:

hook on_handler_exception => sub {
    
    var(error => $_[0]);
};

hook after_error_render => sub {
    
    Web::DataService->error_result(var('error'), var('wds_request'));
};


