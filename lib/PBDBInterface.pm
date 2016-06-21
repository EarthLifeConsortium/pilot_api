

use strict;

package PBDBInterface;

use JSON::SL;
use Try::Tiny;
use URLParam;

use parent 'CompositeSubquery';

our ($SERVICE_LABEL) = 'PaleoBioDB';


sub init_occs_list {
    
    my ($subquery, $request) = @_;
    
    my @params;
    
    # First check to make sure we were asked to return PBDB results.  If not,
    # then we have nothing to do.
    
    if ( ref $request->{ds_hash} eq 'HASH' )
    {
	return unless $request->{ds_hash}{pbdb};
    }
    
    # First check for a taxon name parameter
    
    if ( my $name = $request->clean_param('base_name') )
    {
	push @params, url_param("base_name", $name);
    }
    
    elsif ( $name = $request->clean_param('taxon_name') )
    {
	push @params, url_param("taxon_name", $name);
    }
    
    elsif ( $name = $request->clean_param('match_name') )
    {
	push @params, url_param("match_name", $name);
    }
    
    elsif ( my $id = $request->clean_param('base_id') || $request->clean_param('taxon_id') )
    {
	my $param = 'base_id';
	$param = 'taxon_id' if $request->param_given('taxon_id');
	
	if ( ref $id eq 'Composite::ExtIdent' )
	{
	    unless ( $id->{domain} )
	    {
		$request->add_warning("The value of '$param' cannot be interpreted because it is ambiguous as to which database it belongs to");
		return;
	    }
	    
	    elsif ( $id->{domain} eq 'pbdb' )
	    {
		push @params, url_param("base_id", $id);
	    }
	    
	    elsif ( $id->{domain} eq 'neotoma' )
	    {
		$request->add_warning("The PaleoBioDB cannot be queried for taxa identified by Neotoma identifiers");
		return;
	    }
	}
	
	else
	{
	    push @params, url_param("base_id", $id);
	}
    }
    
    # Check for bbox parameter if any
    
    if ( my $bbox = $request->clean_param('bbox') )
    {
	my ($x1,$y1,$x2,$y2) = split(/,/, $bbox);
	
	push @params, url_param("lngmin", $x1);
	push @params, url_param("lngmin", $x2);
	push @params, url_param("lngmin", $y1);
	push @params, url_param("lngmin", $y2);
    }
    
    if ( my @occ_ids = $request->clean_param_list('occ_id') )
    {
	my @pbdb_ids;
	
	foreach my $id ( @occ_ids )
	{
	    if ( ref $id eq 'Composite::ExtIdent' )
	    {
		if ( $id->{domain} eq 'pbdb' || ( $id->{domain} eq '' &&
						  $request->{ds_single} eq 'pbdb' ) )
		{
		    if ( $id->{type} eq 'occ' || $id->{type} eq '' || $id->{type} eq 'unk' )
		    {
			push @pbdb_ids, "occ:$id->{num}";
		    }
		    
		    else
		    {
			$request->add_warning("Invalid object type '$id->{type}' for parameter " .
					      "'occ_id': must be 'occ' to indicate a PaleoBioDB occurrence.");
		    }
		}
	    }
	    
	    elsif ( ! ref $id && $id > 0 && $request->{ds_single} eq 'pbdb' )
	    {
		push @pbdb_ids, $id;
	    }
	    
	    elsif ( defined $id && $id ne '' )
	    {
		$request->add_warning("Invalid identifier '$id' for parameter 'occ_id'");
	    }
	}
	
	# If we have at least one valid PaleoBioDB occurrence id, add that
	# parameter to the query.
	
	if ( @pbdb_ids )
	{
	    my $id_list = join(',', @pbdb_ids);
	    push @params, url_param("occ_id", $id_list);
	}
	
	# Otherwise return false, since there will be no matching records from
	# the PaleoBioDB.
	
	else
	{
	    return;
	}
    }
    
    # Now check for site_id parameter

    if ( my @site_ids = $request->clean_param_list('site_id') )
    {
	my @pbdb_ids;
	
	foreach my $id ( @site_ids )
	{
	    if ( ref $id eq 'Composite::ExtIdent' )
	    {
		if ( $id->{domain} eq 'pbdb' || ( $id->{domain} eq '' &&
						  $request->{ds_single} eq 'pbdb' ) )
		{
		    if ( $id->{type} eq 'col' || $id->{type} eq '' || $id->{type} eq 'unk' )
		    {
			push @pbdb_ids, "col:$id->{num}";
		    }
		    
		    else
		    {
			$request->add_warning("Invalid object type '$id->{type}' for parameter " .
					      "'site_id': must be 'col' to indicate a PaleoBioDB collection.");
		    }
		}
	    }
	    
	    elsif ( ! ref $id && $id > 0 && $request->{ds_single} eq 'pbdb' )
	    {
		push @pbdb_ids, $id;
	    }
	    
	    elsif ( defined $id && $id ne '' )
	    {
		$request->add_warning("Invalid identifier '$id' for parameter 'site_id'");
	    }
	}
	
	# If we have at least one valid PaleoBioDB occurrence id, add that
	# parameter to the query.
	
	if ( @pbdb_ids )
	{
	    my $id_list = join(',', @pbdb_ids);
	    push @params, url_param("coll_id", $id_list);
	}
	
	# Otherwise return false, since there will be no matching records from
	# the PaleoBioDB.
	
	else
	{
	    return;
	}
    }
    
    # Now check for time parameters
    
    my $max_age = $request->{my_max_age};
    my $max_ageunit = $request->{my_max_unit};
    
    if ( $max_age )
    {
	$max_age /= 1000000 if defined $max_ageunit && $max_ageunit eq 'ybp';
	push @params, "max_ma=$max_age";
    }
    
    my $min_age = $request->{my_min_age};
    my $min_ageunit = $request->{my_min_unit};
    
    if ( $min_age )
    {
	$min_age /= 1000000 if defined $min_ageunit && $min_ageunit eq 'ybp';
	push @params, "min_ma=$min_age";
    }
    
    # Check that at least one parameter was given, so that we don't ask for the
    # entire set of occurrences in the database.
    
    unless ( @params )
    {
	die "400 You must specify at least one parameter";
    }
    
    # Now add more time parameters if specified
    
    my $timerule = $request->{my_timerule};
    
    die "error, my_timerule not set" unless defined $request->{my_timerule};
    
    if ( $timerule eq 'overlap' || $timerule eq 'contain' )
    {
	push @params, "timerule=$timerule";
    }
    
    elsif ( $timerule eq 'buffer' )
    {
	push @params, "timerule=buffer";
	
	my $oldbuffer = $request->{my_old_buffer};
	my $youngbuffer = $request->{my_young_buffer};
	my $range = $request->{my_age_range};
	
	if ( defined $oldbuffer && $oldbuffer ne '' )
	{
	    $oldbuffer /= 1000000;
	}
	
	elsif ( $range )
	{
	    $oldbuffer = 0.2 * $range;
	}
	
	push @params, url_param("timebuffer", $oldbuffer);
	
	if ( defined $youngbuffer && $youngbuffer ne '' && $youngbuffer ne $oldbuffer )
	{
	    $youngbuffer /= 1000000;
	    push @params, url_param("latebuffer", $youngbuffer);
	}
    }
    
    # Then add other necessary parameters:
    
    push @params, "vocab=pbdb";

    if ( $request->has_block('loc') )
    {
	push @params, "show=loc,coords,coll";
    }
    else
    {
	push @params, "show=coll";
    }
    
    # Create the necessary objects to execute a query on the PaleoBioDB and
    # parse the results.
    
    # my $json_parser = JSON::SL->new(10);
    # $json_parser->set_jsonpointer(["/status_code", "/errors", "/warnings", "/records/^"]);
    
    # $subquery->{parser} = $json_parser;
    
    my $url = $request->ds->config_value('pbdb_base') . 'occs/list.json?';
    $url .= join('&', @params);
    
    return $url;
}


sub init_occs_single {

    my ($subquery, $request) = @_;
    
    my @params;
    
    # First check for the occ_id && ds parameters
    
    if ( ref $request->{ds_hash} eq 'HASH' )
    {
	return unless $request->{ds_hash}{pbdb};
    }
    
    my @occ_ids = $request->clean_param_list('occ_id');
    
    my @pbdb_ids;
    
    foreach my $id ( @occ_ids )
    {
	if ( ref $id eq 'Composite::ExtIdent' )
	{
	    if ( $id->{domain} eq 'pbdb' || ( $id->{domain} eq '' &&
					      $request->{ds_single} eq 'pbdb' ) )
	    {
		if ( $id->{type} eq 'occ' || $id->{type} eq '' || $id->{type} eq 'unk' )
		{
		    push @pbdb_ids, "occ:$id->{num}";
		}
		
		else
		{
		    $request->add_warning("Invalid object type '$id->{type}' for parameter " .
					  "'occ_id': must be 'occ' to indicate a PaleoBioDB occurrence.");
		}
	    }
	}
	
	elsif ( ! ref $id && $id > 0 && $request->{ds_single} eq 'pbdb' )
	{
	    push @pbdb_ids, $id;
	}
	
	elsif ( defined $id && $id ne '' )
	{
	    $request->add_warning("Invalid identifier '$id' for parameter 'occ_id'");
	}
    }
    
    # If we have at least one valid PaleoBioDB occurrence id, add that
    # parameter to the query.
    
    if ( @pbdb_ids )
    {
	my $id_list = join(',', @pbdb_ids);
	push @params, url_param("occ_id", $id_list);
    }
    
    # Otherwise return false, since there will be no matching records from
    # the PaleoBioDB.
    
    else
    {
	return;
    }
    
    # Then add other necessary parameters:
    
    push @params, "vocab=pbdb";

    if ( $request->has_block('loc') )
    {
	push @params, "show=loc,coords,coll";
    }
    else
    {
	push @params, "show=coll";
    }
    
    # Create the necessary objects to execute a query on the PaleoBioDB and
    # parse the results.
    
    # my $json_parser = JSON::SL->new(10);
    # $json_parser->set_jsonpointer(["/status_code", "/errors", "/warnings", "/records/^"]);
    
    my $url = $request->ds->config_value('pbdb_base') . 'occs/single.json?';
    $url .= join('&', @params);
    
    # my $subquery = $subservice->new_subquery( url => $url, parser => $json_parser,
    # 					      request => $request );
    
    return $url;
}


sub process_occs_list {
    
    my $subquery = shift;
    
    $subquery->process_json(@_);
    
    my $count = scalar($subquery->records);
    my $request = $subquery->request;
    
    # my $message = "Got PBDB response chunk: $count records";
    # $message .= " $wcount warnings" if $wcount;
    # $message .= " STATUS $subquery->{status}" if $subquery->{http_status} && $subquery->{status} ne '200';
    
    # $subquery->ds->debug_line($message);
    
    # if ( my @warnings = $subquery->warnings )
    # {
    # 	$request->add_warning(@warnings);
    # }
    
    $request->{pbdb_count} += $count;
    
    # Process the results
    
    my $ageunit = $request->clean_param('ageunit');
    
    $subquery->process_records('process_pbdb_age', $ageunit);
    
    # foreach my $r (@records)
    # {

    # 	process_pbdb_age($request, $r, $ageunit);
    # }
    
    # return @records;
    
    my $a = 1;	# we can stop here when debugging
}


# sub process_occs_list {
    
#     my ($subquery, $request, $body, $headers) = @_;
    
#     my @extracted = $subquery->process_json($body);
#     my (@records, @warnings);
    
#     foreach my $r (@extracted)
#     {
# 	if ( $r->{Path} =~ /records/ )
# 	{
# 	    push @records, $r->{Value};
# 	}
	
# 	elsif ( $r->{Path} =~ /status/ )
# 	{
# 	    # $subquery->{status} = $r->{Value};
# 	}
	
# 	elsif ( $r->{Path} =~ /warnings|errors/ && ref $r->{Value} eq 'ARRAY' )
# 	{
# 	    push @warnings, map { "PaleoBioDB: $_" } @{$r->{Value}};
# 	}
#     }
    
#     my $count = scalar(@records);
#     my $wcount = scalar(@warnings);
    
#     my $message = "Got PBDB response chunk: $count records";
#     $message .= " $wcount warnings" if $wcount;
#     $message .= " STATUS $request->{status}" if $request->{status} && $request->{status} ne '200';
    
#     # $request->ds->debug_line($message);
    
#     $request->add_warning(@warnings) if @warnings;
    
#     $request->{pbdb_count} += scalar(@records);
    
#     # Process the results and return them

#     my $ageunit = $request->clean_param('ageunit');
    
#     foreach my $r (@records)
#     {
# 	process_pbdb_age($request, $r, $ageunit);
#     }
    
#     return @records;
# }


sub init_fetch_taxon {
    
    my ($subquery, $request) = @_;
    
    my $args = $subquery->{args};
    
    die "No arguments were specified for init_fetch_taxon\n"
	unless ref $args eq 'HASH';
    
    my $taxon_no = $args->{taxon_no};
    
    die "You must specify ???";
    
}


sub process_pbdb_age {

    my ($subquery, $record, $ageunit) = @_;
    
    if ( defined $record->{max_ma} || defined $record->{min_ma} )
    {
	$record->{AgeUnit} = 'Ma';
	$record->{AgeOlder} = $record->{max_ma};
	$record->{AgeYounger} = $record->{min_ma};
	$record->{age_older} = $record->{max_ma} * 1E6 if defined $record->{max_ma} && $record->{max_ma} ne '';
	$record->{age_younger} = $record->{min_ma} * 1E6 if defined $record->{min_ma} && $record->{min_ma} ne '';
	
	if ( defined $ageunit && $ageunit eq 'ybp' )
	{
	    $record->{AgeUnit} = 'ybp';
	    $record->{AgeYounger} *= 1E6 if defined $record->{AgeYounger};
	    $record->{AgeOlder} *= 1E6 if defined $record->{AgeOlder};
	}
    }
}


# generate_parser ( request, format )
# 
# This method must return a parser object which will be used to parse the
# subquery response, or else the undefined value if no parser is needed or
# none is available.

sub generate_parser {
    
    my ($subquery, $request) = @_;
    
    my $json_parser = JSON::SL->new(10);
    $json_parser->set_jsonpointer(["/status_code", "/errors", "/warnings", "/records/^"]);
    
    return $json_parser;
}


# process_json ( body, headers )
# 
# This method does the primary decoding a of JSON-format response.  We need a
# separate method in each subservice interface class, because each subservice
# returns a particular data structure with particular keys to indicate data
# records and error or warning messages.

sub process_json {
    
    my ($subquery, $body, $headers) = @_;
    
    # There is nothing to do unless we actually have a chunk of the response
    # body to work with.
    
    return unless defined $body && $body ne '';
    
    # Grab the parser object, which was generated for this subrequest by the
    # 'generate_parser' method above.  This is a streaming parser, so we can
    # pass it the response body one chunk at a time.
    
    my $parser = $subquery->{parser};
    
    # Feed the response chunk we were given to the parser and extract a list
    # of response parts that we are interested in.  If an error occurs, then add
    # a warning message.
    
    my @extracted;
    
    try {
        @extracted = $parser->feed($body);
    }
    catch {
        $subquery->add_warning("could not decode JSON response");
    };
    
    # Go through the list.  Everything under 'records:' is a data record.
    # Anything under 'warnings:' or 'errors:' we treat as a warning message.
    
    foreach my $r (@extracted)
    {
	if ( $r->{Path} =~ /records/ )
	{
	    $subquery->add_record($r->{Value});
	}
	
	elsif ( $r->{Path} =~ /status/ )
	{
	    # $subquery->{status} = $r->{Value};
	}
	
	elsif ( $r->{Path} =~ /warnings|errors/ && ref $r->{Value} eq 'ARRAY' )
	{
	    foreach my $w (@{$r->{Value}})
	    {
		$subquery->add_warning($w);
	    }
	}
    }
}

1;
