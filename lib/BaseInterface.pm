# 
# Base class for constituent subservices
# 


use strict;

package BaseInterface;

use Carp qw(carp croak);
use Scalar::Util qw(weaken);
use Try::Tiny;

use AnyEvent;
use AnyEvent::Strict;
use AnyEvent::HTTP;
use AE;

use JSON::SL;


# new_subquery ( cq, args )
# 
# Create a new subquery and associate it with the specified CompositeQuery
# object. The parameter %args collects up all of the remaining arguments.
# 
# Arguments accepted include:
# 
# request		Must be a valid Web::DataService request object
# format		Defaults to 'json'
# init_method		Method for generating a subquery url
# proc_method		Method for processing subquery responses
# secondary		If true, then this is a secondary query whose
#			  results will be used in formulating or interpreting
#			  the results of a primary query
# parser		If given, then this must be an object that implements
#			  a method 'feed' that collects body text and returns
#			  a list of one or more records
# 
# The arguments 'request' and 'init_method' are required, the others are optional.

sub new_subquery {
    
    my ($class, $cq, %args) = @_;
    
    my $format = $args{format} || 'json';
    my $label;
    my $request = $cq->{request};
    my $seq = ++$cq->{sequence};
    
    no strict 'refs';
    
    $label = $args{label} || ${"${class}::SERVICE_LABEL"} || 'unknown';
    $label = "$label $seq" if $args{secondary};
    
    croak "you must specify a request" unless ref $request eq 'REQ::CompositeData';
    croak "you must specify an initialization method" unless $args{init_method};
    
    # Create a new object to represent the subquery.
    
    my $subquery = { label => $label, 
		     cq => $cq, 
		     format => $format,
		     init_method => $args{init_method},
		     status => 'CREATED',
		     records => [ ],
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
    
    # Add this query to the CombinedQuery object.
    
    $cq->add_query($subquery);
    
    $subquery->debug("CREATED");
    
    # Create a condition variable that will be signaled when this query is
    # done. This can be watched by other coroutines that may depend upon the
    # results of this subquery.
    
    $subquery->{cv_done} = AE::cv;
    
    # Create a second condition variable with a callback routine that will get
    # this query rolling. We need to do this because the initialization phase
    # for this query may involve running one or more secondary queries and
    # getting the results back before a URL for this query can be
    # generated. So we set up a condition variable for eqch query and
    # immediately signal it. Then, whenever we are waiting for some query
    # result, the event loop can immediately start out some other query tha
    # tdoesn't depend on that result.
    
    $subquery->{cv_init} = AE::cv { $subquery->init_phase($args{init_method}, $request) };
    $subquery->{cv_init}->send;
    
    # Return the subquery.
    
    return $subquery;
}


# This routine is only in place for debugging purposes, in case we need to
# track the destruction of the various objects at the end of a CompositeQuery
# execution.

sub DESTROY {
    
    my ($subquery) = @_;
    
    # print STDERR "DESTROYING subquery $subquery->{label} ($subquery)\n";
}


sub init_phase {
    
    my ($subquery, $init_method, $request) = @_;
    
    my $label = $subquery->{label};
    my $cq = $subquery->{cq};
    
    $subquery->debug("INIT");
    $subquery->{status} = 'INIT';
    
    my ($url, $parser) = $subquery->$init_method($request);
    
    unless ( $url )
    {
	$subquery->debug("ABORT, NO URL");
	$subquery->{status} = 'ABORT';
	return $cq->done_query($subquery);
    }
    
    else
    {
	$url =~ s/ /%20/g;
    }
    
    unless ( $parser )
    {
	$parser = $subquery->generate_parser($request);
    }
    
    $subquery->debug("URL = $url");
    $subquery->{url} = $url;
    $subquery->{parser} = $parser;
    $subquery->{status} = 'GET';
    
    $subquery->{guard} =
	http_request ( GET => $url,
		       on_body => $subquery->generate_callback('proc_phase'),
		       $subquery->generate_callback('comp_phase') );
    
    my $a = 1;	# we can stop here when debugging
}


sub generate_callback {
    
    my ($subquery, $method) = @_;
    
    weaken($subquery);
    return sub { return $subquery->$method(@_) if defined $subquery; };
}


sub proc_phase {
    
    my ($subquery, $body, $headers) = @_;
    
    my $method = $subquery->{proc_method};
    my $request = $subquery->{cq}{request};
    
    return unless $method;
    return unless defined $body && $body ne '';
    
    # $subquery->debug("CHUNK");
    
    push @{$subquery->{records}}, $subquery->$method($request, $body, $headers);
    return 1;
}


sub comp_phase {
    
    my ($subquery, $body, $headers) = @_;
    
    my $method = $subquery->{comp_method};
    my $request = $subquery->{cq}{request};
    my $cq = $subquery->{cq};
    
    $subquery->{status} = 'COMP';
    $subquery->{http_status} = $headers->{Status};
    $subquery->{http_reason} = $headers->{Reason};
    
    if ( $subquery->{http_status} eq '596' )
    {
	my $init_method = $subquery->{init_method};
	
	$subquery->{status} = 'INIT';
	$subquery->{retries}++;
	$subquery->{records} = [];
	$subquery->{cv_init} = AE::cv { $subquery->init_phase($init_method, $request); };
	$subquery->debug("RETRY $subquery->{retries}");
	
	$subquery->{cv_init}->send;
    }
    
    elsif ( $subquery->{proc_method} && defined $body && $body ne '' )
    {
	my $method = $subquery->{proc_method};
	
	push @{$subquery->{records}}, $subquery->$method($request, $body, $headers);
	$cq->done_query($subquery);
    }
    
    else
    {
	$cq->done_query($subquery);
    }
}


# sub new_subquery_old {
    
#     my ($class, %options) = @_;
    
#     no strict 'refs';
    
#     croak "you must specify a request" unless ref $options{request} eq 'REQ::CompositeData';
#     croak "you must specify a URL" unless defined $options{url} && $options{url} ne '';
    
#     my $label = ${"${class}::SERVICE_LABEL"};
    
#     croak "you must specify a label" unless defined $label && $label ne '';
    
#     my $format = $options{type} || 'json';
    
#     $options{url} =~ s/ /%20/g;
    
#     my $subquery = { label => $label, 
# 		     format => $format, 
# 		     request => $options{request},
# 		     url => $options{url} };
    
#     $subquery->{parser} = $options{parser} if $options{parser};
    
#     return bless $subquery, $class;
# }


sub process_json {
    
    my ($subquery, $chunk) = @_;

    return unless defined $chunk && $chunk ne '';
    
    my $parser = $subquery->{parser};
    my $request = $subquery->{request};
    
    my @records;
    
    try {
	@records = $parser->feed($chunk);
    }
    catch {
	$subquery->{parse_error} = $_[0];
	$subquery->{bad_response} //= $chunk;
    };
    
    return @records;
}


sub debug {
    
    my ($subquery, $line) = @_;
    
    my $ds = $subquery->{cq}{request}{ds};
    my $label = $subquery->{label};
    $ds->debug_line("[$label] $line") if $ds;
}


package CompositeQuery;

use Scalar::Util qw(weaken isweak);

# new ( request, timeout )
# 
# Add a composite query associated with the specified Web::DataService request
# and the specified timeout.

sub new {
    
    my ($class, $request, $timeout) = @_;
    
    # Create a new CompositeQuery instance.
    
    my $self = { request => $request,
		 cv_finished => AE::cv,
		 start => AE::time,
		 timeout => $timeout,
		 queries => [ ],
	       };
    
    bless $self, $class;
    
    # Increment the cv_finished condition variable once to start off the
    # composite query. This variable will be incremented once more for each
    # subquery initiated, and decremented when each one finishes. It will also
    # be decremented once all of the subqueries have been initiated. Thus,
    # when it reaches zero the composite query is done.
    
    $self->{cv_finished}->begin;
    
    # If a timeout was specified, add a timer that will fire at the
    # appropriate time and signal that the entire query is done.
    
    if ( $timeout )
    {
	$self->{tm_fallback} = AE::timer 5, 5, $self->generate_timer_callback($timeout);
    }
    
    return $self;
}


sub generate_timer_callback {
    
    my ($self, $timeout) = @_;
    weaken($self);
    
    return sub { 
	return unless defined $self;
	my $elapsed = AE::time - $self->{start};
	# print STDERR "TICK $elapsed\n"; 
	if ( $elapsed > $timeout )
	{
	    $self->{cv_finished}->send('TIMEOUT');
	}
    };
}


sub DESTROY {
    
    my ($cq) = @_;
    
    # print STDERR "DESTROYING complex query ($cq)\n";
}


sub add_query {
    
    my ($cq, $sq) = @_;
    
    push @{$cq->{queries}}, $sq;
    $cq->{cv_finished}->begin;
}


sub done_query {
    
    my ($cq, $sq) = @_;
    
    $cq->{cv_finished}->end;
    # $cq->{queries} = [ grep { $_ != $sq } @{$cq->{queries}} ];
}


sub run {
    
    my ($cq) = @_;
    
    # The following statement balances the call to $self->{cv_finished}->begin
    # in &new above. After that, the condition variable cv_finished will be
    # signaled whenever all of the subqueries have completed.
    
    $cq->{cv_finished}->end;
    
    # Enter the event loop until this condition variable is signaled. Since we
    # have not specified an event loop to use, AnyEvent::Loop will be used by
    # default.
    
    my $reason = $cq->{cv_finished}->recv;
    
    # The $reason will be defined only if the fallback timeout was tripped.
    # Ultimately, it doesn't matter why the run ended.
    
    $reason ||= '';
    $cq->debug("[CQ] condition '$reason'");
    
    $cq->debug("==========\nDone with run.");
    
    # Clean up the timer, condition variable, and any leftover data structures
    # from the subqueries.
    
    $cq->{tm_fallback} = undef;
    $cq->{cv_finished} = undef;
    
    foreach my $sq ( @{$cq->{queries}} )
    {
	$sq->{guard} = undef;
    }
    
    return;
}


sub results {
    
    my ($cq) = @_;
    
    my @records;
    
    foreach my $q ( @{$cq->{queries}} )
    {
	next unless $q->{main};
	
	my $label = $q->{label};
	my $count = scalar(@{$q->{records}});
	my $skipped = $q->{removed};
	
	$cq->debug("[$label] FOUND $count records");
	$cq->debug("[$label] SKIPPED $skipped records") if $skipped;
	
	if ( @{$q->{records}} )
	{
	    push @records, @{$q->{records}};
	}
	
	if ( $q->{status} eq 'COMP' && defined $q->{http_status} && $q->{http_status} ne '200' )
	{
	    my $request = $cq->{request};
	    $request->add_warning("Error received from $label: $q->{http_status} $q->{http_reason}");
	}
    }
    
    return @records;
}


sub summarize_urls {
    
    my ($cq) = @_;
    
    my $request = $cq->{request};
    my @summary_fields;
    my %summary_values;
    
    foreach my $q ( @{$cq->{queries}} )
    {
	next unless $q->{main};
	next unless $q->{status} eq 'COMP';
	
	push @summary_fields, { field => $q->{label}, name => "$q->{label} URL" };
	$summary_values{$q->{label}} = $q->{url};
    }
    
    $request->{summary_field_list} = \@summary_fields;
    $request->summary_data(\%summary_values);
}


sub debug {
    
    my ($cq, $line) = @_;
    
    my $ds = $cq->{request}{ds};
    $ds->debug_line($line);
}


1;
