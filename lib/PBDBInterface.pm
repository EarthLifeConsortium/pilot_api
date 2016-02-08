

use strict;

package PBDBInterface;

use JSON::SL;
use Try::Tiny;


use parent 'BaseInterface';

our ($SERVICE_LABEL) = 'pbdb';


sub subquery_occs_list {
    
    my ($subservice, $request) = @_;
    
    my @params;
    
    # First check for a taxon name parameter
    
    if ( my $name = $request->clean_param('base_name') )
    {
	push @params, "base_name=$name";
    }
    
    elsif ( $name = $request->clean_param('taxon_name') )
    {
	push @params, "taxon_name=$name";
    }
    
    elsif ( $name = $request->clean_param('match_name') )
    {
	push @params, "match_name=$name";
    }
    
    # Check for bbox parameter if any
    
    if ( my $bbox = $request->clean_param('bbox') )
    {
	my ($x1,$y1,$x2,$y2) = split(/,/, $bbox);
	
	push @params, "lngmin=$x1";
	push @params, "lngmax=$x2";
	push @params, "latmin=$y1";
	push @params, "latmax=$y2";
    }
    
    # Then check for the occ_id && ds parameters
    
    if ( ref $request->{ds_hash} eq 'HASH' )
    {
	return unless $request->{ds_hash}{pbdb};
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
	    push @params, "occ_id=$id_list";
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
	    push @params, "coll_id=$id_list";
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
	
	push @params, "timebuffer=$oldbuffer";
	
	if ( defined $youngbuffer && $youngbuffer ne '' && $youngbuffer ne $oldbuffer )
	{
	    $youngbuffer /= 1000000;
	    push @params, "latebuffer=$youngbuffer";
	}
    }
    
    # Then add other necessary parameters:
    
    push @params, "vocab=pbdb";
    push @params, "show=loc,coords" if $request->has_block('loc');
    
    # Create the necessary objects to execute a query on the PaleoBioDB and
    # parse the results.
    
    my $json_parser = JSON::SL->new(10);
    $json_parser->set_jsonpointer(["/status_code", "/errors", "/warnings", "/records/^"]);
    
    my $url = $request->ds->config_value('pbdb_base') . 'occs/list.json?';
    $url .= join('&', @params);
    
    my $subquery = $subservice->new_subquery( url => $url, parser => $json_parser,
					      request => $request );
    
    return $subquery;
}


sub subquery_occs_single {

    my ($subservice, $request) = @_;
    
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
	push @params, "occ_id=$id_list";
    }
    
    # Otherwise return false, since there will be no matching records from
    # the PaleoBioDB.
    
    else
    {
	return;
    }
    
    # Then add other necessary parameters:
    
    push @params, "vocab=pbdb";
    push @params, "show=loc,coords" if $request->has_block('loc');
    
    # Create the necessary objects to execute a query on the PaleoBioDB and
    # parse the results.
    
    my $json_parser = JSON::SL->new(10);
    $json_parser->set_jsonpointer(["/status_code", "/errors", "/warnings", "/records/^"]);
    
    my $url = $request->ds->config_value('pbdb_base') . 'occs/list.json?';
    $url .= join('&', @params);
    
    my $subquery = $subservice->new_subquery( url => $url, parser => $json_parser,
					      request => $request );
    
    return $subquery;
}


sub process_occs_list {
    
    my $subquery = shift;
    
    my @extracted = $subquery->process_json($_[1]);
    my (@records, @warnings);
    my $request = $subquery->{request};
    
    foreach my $r (@extracted)
    {
	if ( $r->{Path} =~ /records/ )
	{
	    push @records, $r->{Value};
	}
	
	elsif ( $r->{Path} =~ /status/ )
	{
	    # $subquery->{status} = $r->{Value};
	}
	
	elsif ( $r->{Path} =~ /warnings|errors/ && ref $r->{Value} eq 'ARRAY' )
	{
	    push @warnings, map { "PaleoBioDB: $_" } @{$r->{Value}};
	}
    }
    
    my $count = scalar(@records);
    my $wcount = scalar(@warnings);
    
    my $message = "Got PBDB response chunk: $count records";
    $message .= " $wcount warnings" if $wcount;
    $message .= " STATUS $request->{status}" if $request->{status} && $request->{status} ne '200';
    
    # $request->ds->debug_line($message);
    
    $request->add_warning(@warnings) if @warnings;
    
    $request->{pbdb_count} += scalar(@records);
    
    # Process the results and return them

    my $ageunit = $request->clean_param('ageunit');
    
    foreach my $r (@records)
    {
	process_pbdb_age($request, $r, $ageunit);
    }
    
    return @records;
}


sub process_pbdb_age {

    my ($request, $record, $ageunit) = @_;
    
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



1;
