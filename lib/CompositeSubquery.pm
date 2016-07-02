# The following package serves as a base for the classes that implement the interfaces to the
# various subservices.  This package is based on setting up coroutines to carry out HTTP requests
# and process the results.  Whenever one is through with its current activity, control will shift
# to another.

package CompositeSubquery;

use CompositeQuery;

use Carp qw(carp croak);
use Scalar::Util qw(weaken);

use AnyEvent;
use AnyEvent::Strict;
use AnyEvent::HTTP;
use AE;

use namespace::clean;


# new_subquery ( cq, args )
# 
# Create a new subquery object and associate it with the CompositeQuery object given by $cq. The
# parameter %args collects up all of the remaining arguments.
# 
# Arguments accepted include:
# 
# format		Format in which the subquery results are expected.
#			   Defaults to 'json'.
# init_method		Method for generating a subquery url
# proc_method		Method for processing subquery responses
# secondary		If true, then this is a secondary query whose
#			  results will be used in formulating a primary
#			  query or interpreting its results.
# parser		If given, then this must be an object that implements
#			  a method 'feed' that collects body text and returns
#			  a list of one or more records.
# 
# The argument 'init_method' is required, the others are optional.

sub new_subquery {
    
    my ($class, $cq, %args) = @_;
    
    # Check arguments and set defaults
    
    no strict 'refs';
    
    my $label = $args{label} || ${"${class}::SERVICE_LABEL"} || 'unknown';
    
    croak "you must specify an initialization method" unless $args{init_method};
    croak "you may not specify both 'comp_method' and 'proc_method'" 
	if $args{proc_method} && $args{comp_method};
    croak "you must specify a CompositeQuery object as the first argument"
	unless ref $cq eq 'CompositeQuery';
    
    # Create a new object to represent the subquery.
    
    my $subquery = { label => $label, 
		     seq => ++$cq->{sequence},
		     cq => $cq, 
		     format => $args{format} || 'json',
		     init_method => $args{init_method},
		     status => 'CREATED',
		     records => [ ],
		     warnings => [ ],
		   };
    
    bless $subquery, $class;
    
    # Weaken the reference to the CompositeQuery object, because it stores a
    # reference to this and all of the other subqueries and we need to avoid a
    # circular data structure so that these records will be destroyed when
    # they go out of scope.
    
    weaken($subquery->{cq});
    
    # Add other properties, if the appropriate arguments are given.
    
    $subquery->{main} = 1 unless $args{secondary};
    $subquery->{proc_method} = $args{proc_method} if $args{proc_method};
    $subquery->{parser} = $args{parser} if $args{parser};
    $subquery->{args} = $args{args} if $args{args};
    
    # Add this query to the CombinedQuery object.
    
    $cq->add_subquery($subquery);
    
    $subquery->debug("CREATED");
    
    # Create a condition variable that will be signaled when this query is done. This can be
    # watched by other coroutines that may depend upon the results of this subquery.
    
    $subquery->{cv_done} = AE::cv;
    
    # Create a second condition variable with a callback routine that will get this query
    # rolling. We need to do this because the initialization phase for this query may involve
    # running one or more secondary queries and getting the results back before a URL for this
    # query can be generated. So we set up a condition variable for eqch query and immediately
    # signal it. Then, whenever we are waiting for some query result, the event loop can
    # immediately start out some other query that doesn't depend on that result.
    
    $subquery->{cv_init} = AE::cv;
    $subquery->{cv_init}->cb($subquery->_generate_callback('_init_phase'));
    # { $subquery->_init_phase($args{init_method}) };
    $subquery->{cv_init}->send;
    
    # Return the subquery.
    
    my $a = 1;	# we can stop here when debugging
    
    return $subquery;
}


# This routine is only in place for debugging purposes, in case we need to
# track the destruction of the various objects at the end of a CompositeQuery
# execution.

sub DESTROY {
    
    my ($subquery) = @_;
    
    # print STDERR "DESTROYING subquery $subquery->{label} ($subquery)\n";
}


# _init_phase ( subquery, init_method, request )
# 
# This routine will be called by the event loop, once for each subquery. It calls the subquery's
# initialization method. If that method returns a valid URL, then it will initiate an HTTP request
# using that URL. This routine will then return control to the event loop.

sub _init_phase {
    
    my ($subquery) = @_;
    
    my $cq = $subquery->{cq};
    
    $subquery->debug("INIT");
    $subquery->{status} = 'INIT';
    
    # Call the initialization method, and pass a reference to the request object associated with
    # the compound query. This will contain information about the parameters passed to the overall
    # query, which can be used to generate the subquery URL.
    
    my $init_method = $subquery->{init_method};
    my $request = $subquery->request;
    
    my ($url) = $subquery->$init_method($request);
    
    # If no URL was generated, then we abort the query.  This is not necessarily due to an error
    # condition, because this particular subquery may simply not be needed in order to satisfy the
    # compound query.  If an error condition occurrs, it is the responsibility of the
    # initialization method to add a warning message to the request object and/or throw an
    # exception which will abort the whole compound query.
    
    unless ( $url )
    {
	$subquery->debug("ABORT, NO URL");
	$subquery->{status} = 'ABORT';
	return $cq->done_subquery($subquery);
    }
    
    # Otherwise, we get ready to send off the subrequest.  First, we see if the subquery has a
    # method for generating a parser object.  If so, call it and store the result.
    
    if ( $subquery->can('generate_parser') )
    {
	$subquery->{parser} = $subquery->generate_parser($request, $url);
    }
    
    # Store the URL and set the status of the subquery to 'GET' indicating that a request is about
    # to be sent off.
    
    $subquery->debug("URL = $url");
    $subquery->{url} = $url;
    $subquery->{status} = 'GET';
    
    # Generate an HTTP request on the specified URL.  The routine that does this takes the
    # following arguments:
    
    my @args; # = ( GET => $url );
    
    # If we were given a processing method ('proc_method') then we generate a callback that will
    # in turn call that method.
    
    push @args, on_body => $subquery->_generate_callback('_proc_phase')
	if $subquery->{proc_method};
    
    # The final argument is a callback that will be called when the request is complete.
    
    push @args, $subquery->_generate_callback('_comp_phase');
    
    # Send off the request.  The object returned by this call is stored in the subrequest object;
    # if it is ever destroyed or goes out of scope, then all data structures and callbacks
    # associated with the request will be destroyed as well.
    
    $subquery->{guard} = http_request(GET => $url, @args);
    
    # When we get here, the subrequest has been launched.  We now return control to the event
    # loop, which can hand it off to other coroutines which may need to execute in order for the
    # composite query to be completed.
    
    my $a = 1;	# we can stop here when debugging
}


# _generate_callback ( method )
# 
# Generate a callback on the specified method.  The value of $subquery stored in the closure of
# this callback is weakened, so that we don't have a circular reference to prevent the subquery
# object from being deallocated when it goes out of scope.

sub _generate_callback {
    
    my ($subquery, $method) = @_;
    
    return unless $method;
    
    weaken($subquery);
    return sub { return $subquery->$method(@_) if defined $subquery; };
}


# _proc_phase ( body, headers )
# 
# This routine will be called by the event loop, whenever data is received back in response to a
# subrequest.  It may be called multiple times, with different chunks of the response body.  The
# second argument will be a hashref of the response headeres (See AnyEvent::HTTP for more
# details). 
# 
# The job of this routine is to call the processing method ('proc_method') if one has been defined
# for this subquery.

sub _proc_phase {
    
    my $subquery = shift;
    
    my $method = $subquery->{proc_method};
    
    # We have nothing to do unless we were given a non-empty body chunk, 
    
    return unless defined $_[0] && $_[0] ne '';
    
    # Call the processing method to handle this chunk of the response body.
    
    $subquery->$method(@_);
    
    # Return 1, so the request will not be aborted.
    
    return 1;
}


# _comp_phase ( body, headers )
# 
# This routine will be called once by the event loop, when the subrequest is complete.  The first
# argument will be the response body, or possibly a final chunk of it.  The second will be a
# hashref of response headers.

sub _comp_phase {
    
    my $subquery = shift;
    
    my $method = $subquery->{comp_method} || $subquery->{proc_method};
    my $cq = $subquery->{cq};
    
    # Set the status of this subrequest to completed, and record the HTTP status code and reason
    # string from the response.
    
    $subquery->{status} = 'COMP';
    $subquery->{http_status} = $_[1]->{Status};
    $subquery->{http_reason} = $_[1]->{Reason};
    
    # If the status indicates one of the following conditions (see the documentation for AnyEvent::HTTP) then
    # try to recover.
    
    if ( $subquery->{http_status} =~ qr{ ^ 59 [567] $ }xs )
    {
	# If we have tried enough times already, then give up.
	
	$subquery->{retries}++;
	
	if ( $subquery->{retries} > $cq->{retries} )
	{
	    my $tries = $cq->{retries} || 1;
	    $subquery->add_warning("Error $subquery->{http_status} after trying subrequest $tries times");
	    $subquery->add_warning("Bad request '$subquery->{url}'");
	    $cq->done_subquery($subquery);
	    return;
	}
	
	# Otherwise, reset the subquery and try again.
	
	$subquery->{status} = 'INIT';
	$subquery->{records} = [ ];
	$subquery->{warnings} = [ ];
	
	my $init_method = $subquery->{init_method};
	my $request = $subquery->request;
	
	$subquery->{cv_init} = AE::cv; # { $subquery->_init_phase($init_method, $request); };
	$subquery->{cv_init}->cb($subquery->_generate_callback('_init_phase'));
	$subquery->debug("RETRY $subquery->{retries}");
	
	# $subquery->{cv_init}->send;
	push @{$cq->{retry_queue}}, $subquery;
	weaken $cq->{retry_queue}[-1];
    }
    
    # Otherwise, we handle the response.  We do this even if the HTTP status indicates an error,
    # because in that case the response body might still have error messages that we can make use
    # of and/or report.
    
    else
    {
	# If a processing method or completion method is defined for this subquery, and if we have
	# part or all of the response body, then call that method.
	
	if ( $method && defined $_[0] && $_[0] ne '' )
	{
	    $subquery->$method(@_);
	}
	
	# Now call 'done_subquery'. This will decrement the condition variable that coordinates the
	# coroutines for the composite query that is currently being executed, so that the
	# composite query will finish properly when all of the subrequests are done.
	
	$cq->done_subquery($subquery);
    }
}


# request ( )
# 
# Return the request object associated with this subquery.

sub request {
    
    my ($subquery) = @_;
    
    return $subquery->{cq}{request};
}


# add_record ( record )
# 
# Add a data record to this subquery.  This method is designed to be called by the processing
# methods that are defined by the various interface classes.

sub add_record {
    
    my ($subquery, $record) = @_;
    
    push @{$subquery->{records}}, $record;
}


sub process_records {
    
    my ($subquery, $method) = @_;
    
    foreach my $r ( @{$subquery->{records}} )
    {
	$subquery->$method($r);
    }
    
    return scalar(@{$subquery->{records}});
}


sub filter_records {
    
    my ($subquery, $method) = @_;
    
    my @filtered;
    
    foreach my $r ( @{$subquery->{records}} )
    {
	push @filtered, $r if $subquery->$method($r);
    }
    
    $subquery->{records} = \@filtered;
    
    return scalar(@{$subquery->{records}});
}


# records ( )
# 
# Return a list of all the data records added to this subquery.

sub records {
    
    my ($subquery) = @_;
    
    return @{$subquery->{records}};
}


# add_warning ( message )
# 
# Add a warning message to this subquery.  This can be later reported to the end user to tell them
# if something went wrong during the execution of the composite query.

sub add_warning {

    my ($subquery, $message) = @_;
    
    push @{$subquery->{warnings}}, $message;
}


# warnings ( )
# 
# Return a list of all warnings that were added to this subquery.

sub warnings {
    
    my ($subquery) = @_;
    
    return @{$subquery->{warnings}};
}


# debug ( message )
# 
# Print out the specified debugging message.

sub debug {
    
    my ($subquery, $line) = @_;
    
    my $label = $subquery->{label} . ' ' . $subquery->{seq};
    
    $subquery->{cq}->debug("[$label] $line");
}


1;
