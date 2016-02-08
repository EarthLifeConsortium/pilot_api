#
# OccurrenceData.pm
# 
# A classs that returns information from the Neotoma database about the
# values necessary to properly handle the data returned by other queries.
# 
# Author: Michael McClennen

package CompositeData;

use strict;

use AnyEvent;
use AnyEvent::Strict;
use AnyEvent::HTTP;
use JSON::SL;
use LWP::UserAgent;
use HTTP::Validate qw(:validators);
use Carp qw(carp croak);

our (@REQUIRES_ROLE) = qw(CommonData);

use ExternalIdent qw(VALID_IDENTIFIER generate_identifier);

use Moo::Role;


# Initialization
# --------------

# initialize ( )
# 
# This routine is called once by the Web::DataService module, to initialize this
# output class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # Output blocks and sets
    # ======================
    
    $ds->define_block('1.0:occs:basic' => 
	{ output => 'database',
	  com_name => 'sdb', neotoma_name => 'Database', pbdb_name => 'database' },
	    "The source database from which this occurrence was retrieved.",
	{ set => 'occurrence_no', from => 'OccurID', if_field => 'OccurID' },
	{ output => 'occurrence_no',
	  com_name => 'oid', neotoma_name => 'OccurrenceID', pbdb_name => 'occurrence_no' },
	    "A unique identifier assigned to this occurrence.",
	{ output => 'record_type', 
	  com_name => 'typ', neotoma_name => 'RecordType', pbdb_name => 'record_type' },
	    "The type of this record. The value will be C<Occurrence> for the I<Neotoma>",
	    "vocabulary, C<occurrence> for the I<PaleoBioDB> vocabulary, and C<occ> for",
	    "the I<Compact> vocabulary.",
	{ output => 'DatasetID',
	  com_name => 'dst', neotoma_name => 'DatasetID', pbdb_name => 'dataset_no' },
	    "The dataset with which this occurrence is associated. This field will",
	    "be empty for occurrences from PaleoBioDB.",
	{ set => 'accepted_name', from => 'TaxonName', if_field => 'OccurID' },
	{ output => 'accepted_name',
	  com_name => 'tna', neotoma_name => 'TaxonName', pbdb_name => 'accepted_name' },
	    "The taxonomic name by which this occurrence is identified.",
	{ set => 'accepted_no', from => 'TaxonID', if_field => 'OccurID' },
	{ output => 'accepted_no',
	  com_name => 'tid', neotoma_name => 'TaxonID', pbdb_name => 'acepted_no' },
	    "The unique identifier of this taxonomic name in the source database.",
	{ output => 'AgeOlder', 
	  com_name => 'eag', neotoma_name => 'AgeOlder', pbdb_name => 'max_age' },
	    "The maximum of the age range to which this occurrence has been dated.",
	    "Note that occurrences from PaleoBioDB are generally dated stratigraphically,",
	    "while occurrences from Neotoma are generally dated using radiometric or",
	    "other techniques.",
	{ output => 'AgeYounger',
	  com_name => 'lag', neotoma_name => 'AgeYounger', pbdb_name => 'min_age' },
	    "The minimum of the age range to which this occurrence has been dated.",
	{ output => 'AgeUnit',
	  com_name => 'agu', neotoma_name => 'AgeUnit', pbdb_name => 'age_unit' },
	{ set => 'SiteName', from => 'collection_name', if_field => 'collection_name' },
	# { output => 'SiteName',
	#   com_name => 'cna', neotoma_name => 'SiteName', pbdb_name => 'collection_name' },
	#     "The name of the site (SiteName for Neotoma, collection_name for",
	#     "PaleoBioDB) where this occurrence is located.",
	{ set => 'collection_no', from => 'SiteID', if_field => 'SiteID' },
	{ output => 'collection_no',
	  com_name => 'cid', neotoma_name => 'SiteID', pbdb_name => 'collection_no' },
	    "The identifier of the site (SiteID for Neotoma, collection_no",
	    "for PaleoBioDB) where this occurrence is located.",
	);
    
    $ds->define_set('1.0:occs:basic_map' => 
	{ value => 'loc', maps_to => '1.0:occs:loc' },
	    "The geographic location of the occurrence");
    
    $ds->define_block('1.0:occs:loc' =>
	{ set => 'collection_name', from => 'SiteName', if_field => 'OccurID' },
	{ output => 'collection_name', neotoma_name => 'SiteName', com_name => 'cnn',
	  pbdb_name => 'collection_name' },
	    "The name of the site at which the occurrence is located.  This",
	    "reports the C<SiteName> from Neotoma occurrences, and the",
	    "C<collection_name> for PaleoBioDB occurrences.",
	{ set => '*', code => \&NeotomaInterface::process_coords, if_field => 'OccurID' },	
	{ output => 'lng', neotoma_name => 'Longitude', com_name => 'lng', pbdb_name => 'lng' },
	    "The longitude of the site at which the occurrence is located.",
	{ output => 'lat', neotoma_name => 'Latitude', com_name => 'lat', pbdb_name => 'lat' },
	    "The latitude of the site at which the occurrence is located.",
	{ output => 'cc', com_name => 'cc2', pbdb_name => 'country', neotoma_name => 'Country' },
	    "The country in which the occurrence is located, encoded as",
	    "L<ISO-3166-1 alpha-2|https://en.wikipedia.org/wiki/ISO_3166-1_alpha-2>",
	{ output => 'state', com_name => 'stp', pbdb_name => 'state', neotoma_name => 'State' },
	    "The state or province in which the collection is located, if known");
    
    $ds->define_set('1.0:subservices' =>
	{ value => 'PaleoBioDB' },
	{ value => 'pbdb' },
	{ value => 'p' },
	    "Report all matching results from the Paleobiology Database",
	{ value => 'Neotoma' },
	{ value => 'n' },
	    "Report all matching results from Neotoma");
    
    # Orders
    # ======
    
    $ds->define_set('1.0:occs:order' =>
	{ value => 'ageolder' },
	    "Results are ordered chronologically by the older age bound, oldest",
	    "to youngest unless you add C<.asc>.",
	{ value => 'ageolder.asc', undocumented => 1 },
	{ value => 'ageolder.desc', undocumented => 1 },
	{ value => 'ageyounger' },
	    "Results are ordered chronologically by the younger age bound, oldest",
	    "to youngest unless you add C<.asc>.",
	{ value => 'ageyounger.asc', undocumented => 1 },
	{ value => 'ageyounger.desc', undocumented => 1 });
    
    # Parameter rulesets
    # ==================
    
    $ds->define_ruleset('1.0:service_selector' =>
	{ param => 'ds', valid => '1.0:subservices', list => ',' },
	    "Select occurrences from the following subservice or",
	    "subservices.  This parameter allows you to select",
	    "the underlying datasets to be queried, and",
	    "also specifies the order in which the results are reported.",
	    "Example: C<ds=p,n>.  The value of this parameter must be",
	    "a comma-separated list of one or more of the following:");
    
    $ds->define_ruleset('1.0:occs:selector' => 
	{ param => 'occ_id', valid => VALID_IDENTIFIER('OCC'), list => ',' },
	    "Return information about the specified occurrence or occurrences.",
	    "The value of this parameter must be a comma-separated list of",
	    "occurence identifiers. If the identifiers do not include a prefix",
	    "to identify the data service, you must specify a data service",
	    "using the parameter B<C<ds>>.",
 	{ param => 'taxon_name', valid => ANY_VALUE },
	    "Select occurrences identified to the specified taxonomic name.",
	{ param => 'base_name', valid => ANY_VALUE },
	    "Select occurrences identified to the specified taxonomic name, including",
	    "all subtaxa.",
	{ param => 'match_name', valid => ANY_VALUE },
	    "Select occurrences identified to a taxonomic name matching the",
	    "specified pattern, which may include C<%> and C<_> as wildcards.",
	{ at_most_one => ['taxon_name', 'base_name', 'match_name'] },
	{ param => 'site_id', valid => VALID_IDENTIFIER('CST'), list => ',' },
	    "Select occurrences from the given site, specified by site identifier.",
	{ param => 'bbox', valid => \&valid_bbox },
	    "Select occurrences bounded by the specified coordinates, given as:",
	    "C<lngW,latS,lngE,latN>");
    
    $ds->define_ruleset('1.0:occs:specifier' =>
	{ param => 'occ_id', valid => VALID_IDENTIFIER('OCC'), alias => 'id' },
	    "Return information about the specified occurence.  If the",
	    "occurrence identifier does not include a prefix specifying",
	    "which database it belongs to, you must also specify the",
	    "parameter B<C<ds>>.",
	{ optional => 'ds', valid => '1.0:subservices' },
	    "Select the database in which the desired occurrence",
	    "is located.");
    
    $ds->define_set('1.0:timerules' =>
	{ value => 'contain' },
	    "Select only records whose temporal locality is strictly contained in the specified time range.",
	    "This is the most restrictive rule.",
	{ value => 'major' },
	    "Select only records for which at least 50% of the temporal locality range falls within the specified",
	    "time range. This is less restrictive than C<contain>, and is the default unless a different rule",
	    "is specified.",
	{ value => 'buffer' },
	    "Select only records whose temporal locality overlaps the specified time range and is contained",
	    "within a buffer zone around the specified range. This zone is set by default to 20% of the",
	    "time range, and can be explicitly set to any value using the B<C<timebuffer>> parameter.",
	{ value => 'overlap' },
	    "Select only records whose temporal locality overlaps the specified time range by any amount.",
	    "This is the most permissive rule.");
    
    $ds->define_set('1.0:age_units' =>
	{ value => 'ma' },
	    "Report ages in Ma",
	{ value => 'ybp' },
	    "Report ages in years before present");
    
    $ds->define_ruleset('1.0:age_selector' =>
	{ param => 'min_age', valid => DECI_VALUE(0) },
	    "Return only records whose temporal locality is at least this old, specified in years before present.",
	{ param => 'max_age', valid => DECI_VALUE(0) },
	    "Return only records whose temporal locality is at most this old, specified in years before present.",
	{ param => 'min_ma', valid => DECI_VALUE(0) },
	    "Return only records whose temporal locality is at least this old, specified in Ma.",
	{ param => 'max_ma', valid => DECI_VALUE(0) },
	    "Return only records whose temporal locality is at most this old, specified in Ma.",
	{ at_most_one => ['min_age', 'min_ma'] },
	{ at_most_one => ['max_age', 'max_ma'] },
	{ param => 'timerule', valid => '1.0:timerules' },
	    "Resolve temporal locality according to the specified rule, as listed below.  This",
	    "rule is applied to determine which occurrences will be selected if",
	    "you also specify an age range using any of the parameters listed immediately above.",
	{ param => 'timebuffer', valid => ANY_VALUE },
	    "The value of this parameter sets the size of the buffer when evaluating",
	    "time range queries. It implicitly sets the timerule to C<buffer>, and is",
	    "ignored if you specify any other value for the B<C<timerule>> parameter. You can",
	    "specify this either as an absolute value (using the units in which the",
	    "range is specified) or as a percentage of the",
	    "specified time range. You can specify two values separated by commas,",
	    "in which case the first will apply to the older age bound and the second",
	    "to the younger. Examples: C<timebuffer=25%>, C<timebuffer=1.5,2.0>");
    
    $ds->define_ruleset('1.0:occs:list' =>
	"You must specify at least one of the following parameters:",
	{ require => '1.0:occs:selector' },
	">>You can also specify any of the following, to further filter the result list:",
	{ allow => '1.0:age_selector' },
	">>The following parameter is optional, and selects one or more of the available",
	"data services to be queried. If not specified, all will be queried and the",
	"results will be presented in the order received.",
	{ allow => '1.0:service_selector' },
	">>The following parameters control how the results will be presented:",
	{ optional => 'order', valid => '1.0:occs:order', list => ',' },
	    "Return the results in the specified order.  If this parameter is not",
	    "given, then the results will be returned by default in the order received",
	    "from the component databases. The value of this parameter must be",
	    "a comma-separated list of one or more of the following:",
	{ optional => 'ageunit', valid => '1.0:age_units' },
	    "Report the age ranges for occurrences using the specified units.",
	    "If this parameter is not given, then each range will be reported",
	    "according to the default unit for its database: Ma for PaleoBioDB",
	    "and ybp for Neotoma.",
	{ optional => 'SPECIAL(show)', valid => '1.0:occs:basic_map' },
	    "Select one or more optional output blocks.  The value of this parameter can be",
	    "one or more of the following as a comma-separated list:",
	{ allow => '1.0:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
    
    $ds->define_ruleset('1.0:occs:single' =>
	"You must specify the following parameters:",
	{ require => '1.0:occs:specifier' },
	"The following parameters control how the results will be presented:",
	{ optional => 'ageunit', valid => '1.0:age_units' },
	    "Report the age ranges for occurrences using the specified units.",
	    "If this parameter is not given, then each range will be reported",
	    "according to the default unit for its database: Ma for PaleoBioDB",
	    "and ybp for Neotoma.",
	{ optional => 'SPECIAL(show)', valid => '1.0:occs:basic_map' },
	    "Select one or more optional output blocks.  The value of this parameter can be",
	    "one or more of the following as a comma-separated list:",
	{ allow => '1.0:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request");
	
}


# occs_list ( )
# 
# Lists of fossil occurrences

sub occs_list {

    my ($request) = @_;
    
    # Do some initial parameter checking.
    
    unless ( defined $request->{output_vocab} && $request->{output_vocab} ne '' &&
	     $request->{output_vocab} ne 'null' )
    {
	die "400 You must specify a vocabulary using the 'vocab' parameter.\n";
    }
    
    $request->ds_param();    
    $request->time_params();
    
    # First create URLs for the various constituent services, based on the
    # parameters given to this operation.
    
    my (%url, %obj, @raw, @subqueries, %status, %reason, %pending);
    
    my $ds = $request->ds;
    # my @constituents = ;
    
    # We loop over the available constituent modules.  For each one which can
    # do the 'make_occs_list' method, we generate a subquery object. This
    # method generates the appropriate URL for the subquery.
    
    foreach my $subservice ( @CompositeService::SERVICES )
    {
	if ( $subservice->can('subquery_occs_list') )
	{
	    my $subquery = $subservice->subquery_occs_list($request);
	    
	    if ( $subquery )
	    {
		push @subqueries, $subquery;
		$ds->debug_line("Generated subquery for $subquery->{label}: $subquery->{url}");
	    }
	}
    }
    
    # Now fire off each of these subqueries and collect up the
    # results. We start by setting up a condition variable that will be
    # triggered when all of the queries are finished, and a timer which will
    # trigger that condition variable if the overall timeout expires.
    
    my $subquery_condition = AnyEvent->condvar;
    
    my $fallback = AnyEvent->timer (
	after => $CompositeService::TIMEOUT,
        cb => sub { $subquery_condition->send("TIMEOUT"); } );
    
    # We then call 'begin' on this condition variable, according to the
    # AnyEvent documentation: https://metacpan.org/pod/AnyEvent#METHODS FOR PRODUCERS
    # This will increment a counter associated with the condition
    # variable, and is matched by the 'end' that comes after the subquery
    # loop.
    
    $subquery_condition->begin;
    
    # We now loop over the list of subquery objects. We fire off each query using
    # AnyEvent::HTTP, with callbacks to process the result data as it comes
    # in. We do this on a chunk-by-chunk basis, because the subquery results
    # may be very long. We are planning for future development in which we may want to
    # send the results back to the client as they are received rather than
    # collecting them all up and processing them together.
    
    foreach my $sq (@subqueries)
    {
	my $url = $sq->{url};
	next unless $url;
	
	my $label = $sq->{label};
	
	# We call 'begin' for each subquery, which will increment the counter
	# associated with the condition variable.
	
	$subquery_condition->begin;
	
	# Initiate each query, with two callbacks. The first handles each
	# chunk of incoming data, and the second is called on query
	# completion. This second callback in turn calls 'end' on the subquery
	# condition variable, which decrements the counter to indicate
	# completion of this query. The first argument passed to each callback
	# holds body data, which is passed to the process_occs_list routine.
	
	$sq->{subrequest} =
	http_request ( GET => $url,
		       on_body => 
		       sub { push @raw, $sq->process_occs_list($request, $_[0]);
		       	     # $ds->debug_line("GOT CHUNK: $label");
			     # $ds->debug_line($_[0]);
			     return 1;
			 },
		       sub { my ($body, $headers) = @_;
			     $ds->debug_line("COMPLETE: $label");
			     # $ds->debug_line($body) if $body;
			     $status{$label} //= $headers->{Status};
			     $reason{$label} //= $headers->{Reason};
			     push @raw, $sq->process_occs_list($request, $body)
				 if defined $body && $body ne '';
			     $subquery_condition->end; } );
    }
    
    # Finally, we call 'end' on the condition variable to balance the initial
    # call to 'begin'.  At this point, whenever all of the subqueries
    # complete, the counter will return to zero and the condition variable
    # will be automatically signaled.
    
    $subquery_condition->end;
    
    # The following call will block until the condition variable is signaled
    # when the final query completes (or alternatively when the fallback
    # timeout expires).
    
    my $event = $subquery_condition->recv;
    
    # At this point, we have the results of all subqueries. Unless, that is,
    # the fallback timeout expired in which case we have all of the results
    # that we are going to get...
    
    my $count = @raw;
    # $ds->debug_line("Found $count results");
    # $ds->debug_line("Event = $event") if $event;
    
    # If any of the queries returned a status code indicating non-success,
    # then add a warning to our result.
    
    foreach my $name ( keys %status )
    {
	if ( $status{$name} !~ /^2\d\d/ )
	{
	    $ds->debug_line("Status $name: $status{$name} $reason{$name}");
	    $request->add_warning("Error received from $name: $status{$name} $reason{$name}");
	}
    }
    
    # If we are running in debug mode, report how many records we received and
    # how many were filtered out.
    
    if ( $request->{pbdb_count} )
    {
	$ds->debug_line("FOUND PBDB: $request->{pbdb_count} records");
    }
    
    if ( $request->{pbdb_removed} )
    {
	$ds->debug_line("REMOVED PBDB: $request->{pbdb_count} records");
    }
    
    if ( $request->{neotoma_count} )
    {
	$ds->debug_line("FOUND NEOTOMA: $request->{neotoma_count} records");
    }
    
    if ( $request->{neotoma_removed} )
    {
	$ds->debug_line("REMOVED NEOTOMA: $request->{neotoma_removed} records")
    }
    
    # Process ages and other fields
    
    foreach my $r (@raw)
    {
	$request->process_one_record($r);
    }
    
    # If we were requested to sort the results, do so now.
    
    my $order_sub = $request->generate_order_sub();
    
    if ( ref $order_sub eq 'CODE' )
    {
	my @sorted = sort $order_sub @raw;
	$request->add_result(@sorted);
    }
    
    else
    {
	$request->add_result(@raw);
    }
    
    my $a = 1;	# we can stop here when debugging
    
}


# occs_single ( )
# 
# Single fossil occurrence

sub occs_single {

    my ($request) = @_;
    
    
}


# ds_param ( )
# 
# Check and store the values of the ds parameter.

sub ds_param {
    
    my ($request) = @_;
    
    if ( my @ds_list = $request->clean_param_list('ds') )
    {
	foreach my $ds ( @ds_list )
	{
	    if ( $ds =~ qr{^n}xsi )
	    {
		push @{$request->{ds_list}}, 'neotoma' unless $request->{ds_hash}{neotoma};
		$request->{ds_hash}{neotoma} = 1;
	    }
	    
	    elsif ( $ds =~ qr{^p}xsi )
	    {
		push @{$request->{ds_list}}, 'pbdb' unless $request->{ds_hash}{pbdb};
		$request->{ds_hash}{pbdb} = 1;
	    }
	}
    }
    
    if ( ref $request->{ds_list} eq 'ARRAY' && @{$request->{ds_list}} == 1 )
    {
	$request->{ds_single} = $request->{ds_list}[0];
    }
    
    if ( ref $request->{ds_list} eq 'ARRAY' && @{$request->{ds_list}} > 1 )
    {
	foreach my $i (0..$#{$request->{ds_list}})
	{
	    $request->{ds_sort}{$request->{ds_list}[$i]} = $i;
	}
    }
}


# time_params ( )
# 
# Check and store the values of the time parameters.

my ($VALID_AGE) = qr{ ^ (?: \d+ | \d+ [.] \d* | \d* [.] \d+ ) $ }xs;

sub time_params {
    
    my ($request) = @_;
    
    # First check the max and min parameters.
    
    my $max_age = $request->clean_param('max_ma');
    my $max_age_unit = 'ma';
    
    unless ( $max_age )
    {
	$max_age = $request->clean_param('max_age');
	$max_age_unit = 'ybp';
    }
    
    if ( defined $max_age && $max_age ne '' )
    {
	die "400 Invalid age '$max_age', must be a decimal number greater than zero\n"
	    unless $max_age =~ $VALID_AGE && $max_age > 0;
	
	$request->{my_max_age} = $max_age;
	$request->{my_max_unit} = $max_age_unit;
    }
    
    my $min_age = $request->clean_param('min_ma' );
    my $min_age_unit = 'ma';
    
    unless ( $min_age )
    {
	$min_age = $request->clean_param('min_age');
	$min_age_unit = 'ybp';
    }
    
    if ( $min_age )
    {
	die "400 Invalid age '$min_age', must be a decimal number\n"
	    unless $min_age =~ $VALID_AGE;
	
	$request->{my_min_age} = $min_age;
	$request->{my_min_unit} = $min_age_unit;
    }
    
    # Now compute the age range, if defined.
    
    my ($max_years, $min_years, $range);
    
    if ( $min_age )
    {
	$min_years = $min_age_unit eq 'ma' ? $min_age * 1000000 : $min_age;
    }
    
    if ( $max_age )
    {
	$max_years = $max_age_unit eq 'ma' ? $max_age * 1000000 : $max_age;
	$range = $min_years ? $max_years - $min_years : $max_years;
    }
    
    $request->{my_age_range} = $range if $range;
    
    # Then deal with the timerule parameter

    my $timerule = $request->clean_param('timerule');
    
    # Then deal with the timebuffer parameter
    
    my $timebuffer = $request->clean_param('timebuffer');
    my ($oldbuffer, $youngbuffer);
    
    if ( defined $timebuffer && $timebuffer ne '' )
    {
	if ( $timebuffer =~ qr{ ^ ( \d+ | \d+ [.] \d* | \d* [.] \d+ ) ( [%]? )
				  (?: \s*,\s* ( \d+ | \d+ [.] \d* | \d* [.] \d+ ) ( [%]? ) )? $ }xs )
	{
	    my $old = $1;
	    my $old_pct = $2;
	    my $young = $3;
	    my $young_pct = $4;
	    
	    unless ( defined $young && $young ne '' )
	    {
		$young = $old;
		$young_pct = $old_pct;
	    }
	    
	    
	    if ( $range )
	    {
		if ( $old_pct )
		{
		    $oldbuffer =  $old * $range / 100;
		}
		
		else
		{
		    $oldbuffer = $max_age_unit eq 'ma' ? $old * 1000000 : $old;
		}
		
		if ( $young_pct )
		{
		    $youngbuffer = $young * $range / 100;
		}
		
		else
		{
		    $youngbuffer = $min_age_unit eq 'ma' ? $young * 1000000 : $young;
		}
	    }
	    
	    elsif ( $min_years )
	    {
		if ( $young_pct )
		{
		    $youngbuffer = $young * $min_years / 100;
		}
		
		else
		{
		    $youngbuffer = $min_age_unit eq 'ma' ? $young * 1000000 : $young;
		}
	    }
	}
	
	else
	{
	    die "400 Invalid value for 'timebuffer'\n";
	}
	
	if ( defined $timerule && $timerule ne 'buffer' && $timerule ne '' )
	{
	    die "400 The parameter 'timebuffer' cannot be used with timerule '$timerule'\n";
	}
	
	$timerule = 'buffer';
	$request->{my_old_buffer} = $oldbuffer;
	$request->{my_young_buffer} = $youngbuffer;
    }
    
    $request->{my_timerule} = $timerule || 'major';
}


# time_filter ( )
# 
# Check to make sure that each record satisfies the specified timerule.  This
# check only needs to be done on Neotoma records, and only if the timerule is
# 'major' (the default).

sub time_filter {
    
    my ($request, $record) = @_;
    
    # The check only applies if the timerule is 'major'.
    
    my $timerule = $request->{my_timerule};
    
    return 1 unless defined $timerule;
    
    # The check also applies only at least one non-zero age bound was specified.
    
    return 1 unless $request->{my_max_age} || $request->{my_min_age};
    
    # If the timerule was 'major', only accept the record if more than 50% of its
    # age range overlaps the specified filter age range.
    
    if ( $timerule eq 'major' )
    {
	my $recordspan = $record->{age_older} - $record->{age_younger};
	my $overlap;
    
	if ( $recordspan == 0 )
	{
	    return if $request->{my_max_age} && $record->{age_older} > $request->{my_max_age};
	    return if $request->{my_min_age} && $record->{age_younger} < $request->{my_min_age};
	    return 1;
	}
    
	elsif ( $request->{my_max_age} )
	{
	    return unless $record->{age_older};
	
	    $record->{age_younger} ||= 0;
	    $record->{my_min_age} ||= 0;
	
	    if ( $record->{age_older} > $request->{my_max_age} )
	    {
		if ( $record->{age_younger} < $request->{my_min_age} )
		{
		    $overlap = $request->{my_max_age} - $request->{my_min_age};
		}
	    
		else
		{
		    $overlap = $request->{my_max_age} - $record->{age_younger};
		}
	    }
	
	    elsif ( $record->{age_younger} < $request->{my_min_age} )
	    {
		$overlap = $record->{age_older} - $request->{my_min_age};
	    }
	
	    else
	    {
		$overlap = $record->{age_older} - $record->{age_younger};
	    }
	}
    
	elsif ( $record->{age_older} > $request->{my_min_age} )
	{
	    $overlap = $record->{age_older} - $request->{my_min_age};
	}
    
	else
	{
	    $overlap = 0;
	}
    
	return ( $overlap / $recordspan ) >= 0.5;
    }
    
    # If the timerule is 'buffer', we can assume that the record overlaps the
    # specified interval because the subservice call makes sure of that.  We
    # simply need to make sure that it lies within the buffer region.
    
    elsif ( $timerule eq 'buffer' )
    {
	my $max_bound = $request->{my_max_age};
	
	if ( $max_bound )
	{
	    $max_bound *= 1000000 if defined $request->{my_max_unit} && 
		$request->{my_max_unit} eq 'ma';
	    
	    $max_bound += $request->{my_old_buffer} if $request->{my_old_buffer};
	    
	    return unless $record->{age_older} <= $max_bound;
	}
	
	my $min_bound = $request->{my_min_age};
	
	if ( $min_bound )
	{
	    $min_bound *= 1000000 if defined $request->{my_min_unit} &&
		$request->{my_min_unit} eq 'ma';
	    
	    $min_bound -= $request->{my_young_buffer} if $request->{my_young_buffer};
	    
	    if ( $min_bound > 0 )
	    {
		return unless $record->{age_younger} >= $min_bound;
	    }
	}
	
	return 1;
    }
    
    # For any other time rule, accept all records.
    
    else
    {
	return 1;
    }
}


sub generate_order_sub {

    my ($request) = @_;
    
    my @orders = $request->clean_param_list('order');
    
    return sub {
	
	foreach my $o (@orders)
	{
	    if ( $o eq 'ageolder.asc' )
	    {
		return -1 unless defined $b->{age_older};
		return 1 unless defined $a->{age_older};
		return -1 if $a->{age_older} < $b->{age_older};
		return 1 if $a->{age_older} > $b->{age_older};
	    }
	    
	    elsif ( $o eq 'ageolder' || $o eq 'ageolder.desc' )
	    {
		return -1 unless defined $b->{age_older};
		return 1 unless defined $a->{age_older};
		return -1 if $a->{age_older} > $b->{age_older};
		return 1 if $a->{age_older} < $b->{age_older};
	    }
	    
	    elsif ( $o eq 'ageyounger.asc' )
	    {
		return -1 unless defined $b->{age_younger};
		return 1 unless defined $a->{age_younger};
		return -1 if $a->{age_younger} < $b->{age_younger};
		return 1 if $a->{age_younger} > $b->{age_younger};		
	    }
	    
	    elsif ( $o eq 'ageyounger' || $o eq 'ageyounger.desc' )
	    {
		return -1 unless defined $b->{age_younger};
		return 1 unless defined $a->{age_younger};
		return -1 if $a->{age_younger} > $b->{age_younger};
		return 1 if $a->{age_younger} < $b->{age_younger};
	    }
	}
	
	if ( ref $request->{ds_sort} eq 'HASH' )
	{
	    return $request->{ds_sort}{$a->{ds_key}} <=> $request->{ds_sort}{$b->{ds_key}};
	}
	
	return 0;
    };
}


my %neotoma_ids = (OccurID => 'OCC',
		   SiteID => 'SITE',
		   TaxonID => 'TXN',
		   DatasetID => 'DST');
			 

sub process_one_record {

    my ($request, $record) = @_;
    
    if ( defined $record->{OccurID} )
    {
    	$record->{database} = 'Neotoma';
	$record->{ds_key} = 'neotoma';
	
	foreach my $k ( keys %neotoma_ids )
	{
	    $record->{$k} = ExternalIdent::generate_identifier('neotoma', $neotoma_ids{$k}, 
							       $record->{$k});
	}
	
	# $record->{OccurID} = ExternalIdent::generate_identifier('neotoma', 'OCC', $record->{OccurID});
	# $record->{SiteID} = ExternalIdent::generate_identifier('neotoma', 'SITE', $record->{SiteID});
	# $record->{TaxonID} = ExternalIdent::generate_identifier('neotoma', 'TXN', $record->{TaxonID});
	# $record->{DataSetID} = 
    }
    elsif ( defined $record->{occurrence_no} )
    {
    	$record->{database} = 'PaleoBioDB';
	$record->{ds_key} = 'pbdb';
	$record->{occurrence_no} = ExternalIdent::generate_identifier('pbdb', 'OCC', $record->{occurrence_no});
	$record->{collection_no} = ExternalIdent::generate_identifier('pbdb', 'COL', $record->{collection_no});
	$record->{accepted_no} = ExternalIdent::generate_identifier('pbdb', 'TXN', $record->{accepted_no});
    }
    else
    {
    	$record->{database} = 'Unknown';
	$record->{ds_key} = 'unknown';
    }
    
    if ( $request->{output_vocab} eq 'pbdb' )
    {
    	$record->{record_type} = 'occurrence';
    }
    elsif ( $request->{output_vocab} eq 'neotoma' )
    {
    	$record->{record_type} = 'Occurrence';
    }
    else
    {
    	$record->{record_type} = 'occ';
    }
    
}



sub process_age {

    my ($request, $record) = @_;
    
    if ( $record->{occurrence_no} && ! $record->{OccurID} )
    {
	$record->{AgeOlder} = $record->{max_ma};
	$record->{AgeYounger} = $record->{min_ma};
	$record->{YearsOlder} = $record->{max_ma} * 1E6 if $record->{max_ma};
	$record->{YearsYounger} = $record->{min_ma} * 1E6 if $record->{min_ma};
	$record->{AgeUnit} = 'Ma';
	
	if ( $request->clean_param('ageunit') eq 'ybp' )
	{
	    $record->{AgeYounger} *= 1E6 if defined $record->{AgeYounger};
	    $record->{AgeOlder} *= 1E6 if defined $record->{AgeOlder};
	    $record->{AgeUnit} = 'ybp';
	}
    }
    
    elsif ( $record->{OccurID} )
    {
	$record->{YearsOlder} = $record->{AgeOlder};
	$record->{YearsYounger} = $record->{AgeYounger};
	
	if ( $request->clean_param('ageunit') eq 'ma' )
	{
	    $record->{AgeYounger} *= 1E-6 if defined $record->{AgeYounger};
	    $record->{AgeOlder} *= 1E-6 if defined $record->{AgeOlder};
	    $record->{AgeUnit} = 'Ma';
	}
	
	else
	{
	    $record->{AgeUnit} = 'ybp';
	}
    }
}


# check for valid bbox parameter values

sub valid_bbox {
    
    my ($value) = @_;
    
    my (@coords) = split /\s*,\s*/, $value;
    
    unless ( @coords == 4 )
    {
	return { error => "the value of {param} must contain four decimal coordinates: w,s,n,e" };
    }
    
    foreach my $coord ( @coords )
    {
	unless ( $coord =~ qr{ ^ [-]? (?: \d+ [.] \d? | \d* [.] \d+ | \d+ ) $ }xsi )
	{
	    return { error => "invalid coordinate '$coord' in {param}" };
	}
    }
    
    return { value => join(',', @coords) };
}


1;
