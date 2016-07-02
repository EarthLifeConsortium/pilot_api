# 
# CompositeQuery - make multiple HTTP requests in the process of satisfying a composite request
# 
# This package enables the execution of multiple HTTP requests as coroutines,
# all in a single thread.


use strict;

package CompositeQuery;

use AnyEvent;
use AE;
use Scalar::Util qw(weaken reftype blessed);
use Carp qw(carp croak);

my %OPT = ( 'timeout' => 1,
	    'retries' => 3,
	  );

use namespace::clean;


# new ( request, timeout )
# 
# Create a composite query object associated with the specified
# Web::DataService request and the specified timeout.  The queries made using
# this object will add records to the request.
# 
# If the specified timeout elapses, then the composite operation will be
# aborted and a warning is added to the request.  This provides a time limit
# for the composite query to be fulfilled, and prevents problems if a subquery
# hangs indefinitely.  If the timeout is given as 0, then the operation will
# be allowed to continue until all of the subqueries have returned or have
# timed out at another software layer.

sub new {
    
    my ($class, $request, $options) = @_;
    
    # Check the parameters.
    
    $options ||= {};
    
    croak "The second argument, if given, must be an options hash"
	if defined $options && ref $options ne 'HASH';
    
    # Create a new CompositeQuery instance.  We record the time at which this
    # operation is initiated, so that the timer can check to see whether the
    # specified timeout has expired.
    
    my $self = { request => $request,
		 start => AE::time,
		 timeout => 0,
		 retries => 0,
		 queries => [ ],
		 warnings => [ ],
		 retry_queue => [ ],
	       };
    
    bless $self, $class;
    
    # Store the option values.

    foreach my $k ( keys %$options )
    {
	croak "Invalid option '$k'" unless $OPT{$k};

	$self->{$k} = $options->{$k};
    }
    
    # Weaken the reference to $request, in case the request also stores a reference to this
    # object. We want to avoid a circular data structure.
    
    weaken($self->{request});
    
    # Create a condition variable that will be used to coordinate the various
    # coroutines that will be used to execute the subqueries that will satisfy
    # this composite operation.
    
    $self->{cv_finished} = AE::cv;
	
    # Increment this condition variable once to start off the composite
    # query. This variable will be incremented once more for each subquery
    # initiated, and decremented when each one finishes. It will also be
    # decremented once all of the subqueries have been initiated. Thus, when
    # it reaches zero all of the subqueries are done.
    
    $self->{cv_finished}->begin;
    
    # If a timeout was specified, add a timer that will fire at the
    # appropriate time and signal that the entire query is done (regardless of
    # whether all the subqueries have finished).  The timer firing time is not
    # reliable, so we just fire it every 5 seconds and check each time to see
    # whether the timeout has elapsed.
    
    if ( $self->{timeout} )
    {
	$self->{tm_fallback} = AE::timer 3, 3, $self->generate_timer_callback($self->{timeout});
    }
    
    return $self;
}


# generate_timer_callback ( timeout )
# 
# Generate a callback which will fire periodically so that we can check on the
# progress of the query.  This timeout routine needs to do two things:
#
# 1) check whether the overall timeout has elapsed.  If it has, then we signal
#    the cv_finished condition variable which causes the entire composite
#    query to be immediately terminated.
#
# 2) check whether any subqueries need to be retried.  If so, then fire off
#    another attempt.

sub generate_timer_callback {
    
    my ($self, $timeout) = @_;
    
    # Weaken this reference, which persists as a closure on the returned
    # subroutine.  That prevents a chain of circular strong references, which
    # would keep the composite query object from being destroyed when if it
    # goes out of scope before being resolved.
    
    weaken($self);
    
    # Return a callback subroutine that will have the desired effect. The
    # reference to $self persists as a closure on this subroutine.
    
    return sub {
	
	return unless defined $self;
	
	my $elapsed = AE::time - $self->{start};
	$self->debug("TICK $elapsed");
	
	if ( $elapsed > $timeout )
	{
	    $self->{cv_finished}->send('TIMEOUT');
	}
	
	else
	{
	    while ( my $subquery = shift @{$self->{retry_queue}} )
	    {
		$subquery->{cv_init}->send if ref $subquery && ref $subquery->{cv_init};
	    }
	}
    };
}


# This routine is here only for debugging purposes; you can uncomment the
# print statement if you want to check that the object has been properly
# destroyed at the end of the composite query operation.

sub DESTROY {
    
    my ($cq) = @_;
    
    # print STDERR "DESTROYING complex query ($cq)\n";
}


# add_subquery ( subquery )
# 
# This routine should be called once for each subquery that is executed as
# part of the combined query operation. It adds the subquery to the list
# associated with this operation, and increments the condition variable
# cv_finished.

sub add_subquery {
    
    my ($cq, $sq) = @_;
    
    push @{$cq->{queries}}, $sq;
    $cq->{cv_finished}->begin;
}


# done_subquery ( subquery )
# 
# This routine should be called once when each subquery finishes.  It
# decrements the condition variable cv_finished; once the count reaches zero,
# the composite query is over.
# 
# We don't remove the subquery record from the list, because (a) there is no
# need and (b) we may want to wait until all of the subqueries have finished
# to extract its results.

sub done_subquery {
    
    my ($cq, $sq) = @_;
    
    $cq->{cv_finished}->end;
}


# run ( )
# 
# Run the composite query.  This routine should be called after all of the
# main subqueries have been created.  Additional secondary subqueries may be
# generated during processing of these main queries.
# 
# This routine will turn control over to the event loop (which by default will
# be AnyEvent::Loop) so that the coroutines responsible for the various
# queries can run.  When all have completed, this routine will return.

sub run {
    
    my ($cq) = @_;
    
    # The following statement balances the call to $self->{cv_finished}->begin
    # in &new above. After that, the condition variable cv_finished will reach
    # zero whenever all of the subqueries have completed.
    
    $cq->{cv_finished}->end;
    
    # Enter the event loop until this condition variable is signaled. Unless
    # some other event loop has been included in the main program,
    # AnyEvent::Loop will be used by default.  If the timeout for the
    # composite query expires, then the condition variable will be triggered
    # regardless of the status of the subqueries.
    
    my $reason = $cq->{cv_finished}->recv;
    
    # The $reason will be defined only if the timeout was tripped.  In that
    # case, add a warning to the end user who originally initiated the query.
    # Note: this warning will only be visible if the calling package also
    # calls the 'warnings' method.
    
    $reason ||= '';
    $cq->debug("[CQ] condition '$reason'");
    
    if ( $reason eq 'TIMEOUT' )
    {
	push @{$cq->{warnings}}, "This request timed out before one or more of the subqueries was finished.";
	push @{$cq->{warnings}}, "The results may be incomplete.";
    }
    
    $cq->debug("==========\nDone with run.");
    
    # Clean up the timer, condition variable, and all of the data structures
    # associated with the HTTP requests for the subqueries.  The subquery
    # records themselves are preserved, including the result records, error
    # messages, etc.
    
    $cq->{tm_fallback} = undef;
    $cq->{cv_finished} = undef;
    
    foreach my $sq ( @{$cq->{queries}} )
    {
	$sq->{guard} = undef;
    }
    
    # Return to the main program. At this point, the results of the various
    # subqueries have been received, and preliminary processing has been done
    # on them.  Note: the results may be incomplete, if one or more subqueries
    # had a long latency and the fallback timeout tripped.
    
    my $a = 1;	# we can stop here when debugging
    
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
	
	$q->debug("FOUND $count records");
	$q->debug("SKIPPED $skipped records") if $skipped;
	
	if ( @{$q->{records}} )
	{
	    push @records, @{$q->{records}};
	}
    }
    
    return @records;
}


sub warnings {

    my ($cq) = @_;
    
    my @warnings;
    
    foreach my $q ( @{$cq->{queries}} )
    {
	my $label = $q->{label};
	
	if ( $q->{status} eq 'COMP' && defined $q->{http_status} && $q->{http_status} ne '200' )
	{
	    $q->add_warning("$q->{http_status} $q->{http_reason}");
	}
	
	push @warnings, map { "$label: $_" } $q->warnings;
    }
    
    return @warnings;
}


sub urls {
    
    my ($cq, $report_all) = @_;
    
    my @result;
    
    foreach my $q ( @{$cq->{queries}} )
    {
	unless ( $report_all )
	{
	    next unless $q->{main};
	    next unless $q->{status} eq 'COMP';
	}
	
	push @result, [$q->{label}, $q->{status}, $q->{url}];
    }

    return @result;
}


# sub summarize_urls {
    
#     my ($cq) = @_;
    
#     my $request = $cq->{request};
#     my @summary_fields;
#     my %summary_values;
    
#     foreach my $q ( @{$cq->{queries}} )
#     {
# 	next unless $q->{main};
# 	next unless $q->{status} eq 'COMP';
	
# 	push @summary_fields, { field => $q->{label}, name => "$q->{label} URL" };
# 	$summary_values{$q->{label}} = $q->{url};
#     }
    
#     $request->{summary_field_list} = \@summary_fields;
#     $request->summary_data(\%summary_values);
# }


# debug_mode ( boolean )
# 
# If this method is called with an argument that is true, turn on debugging messages.  Otherwise,
# turn them off.

sub debug_mode {
    
    my ($cq, $debug_mode) = @_;
    
    $cq->{debug_mode} = $debug_mode;
}


sub debug {
    
    my ($cq, $line) = @_;
    
    # If the CompositeQuery was set up with a Web::DataService request, call the debug_line
    # method.
    
    if ( ref $cq->{request} && reftype $cq->{request} eq 'HASH' && blessed $cq->{request}{ds} &&
	 $cq->{request}{ds}->can('debug_line') )
    {
	$cq->{request}{ds}->debug_line($line) if $;
    }
    
    # Otherwise, if debug mode is on, then just print out the
    # message to STDERR.  Otherwise, ignore the message.
    
    elsif ( $cq->{debug_mode} )
    {
	print STDERR "[Composite] $line\n";
    }
}


1;
