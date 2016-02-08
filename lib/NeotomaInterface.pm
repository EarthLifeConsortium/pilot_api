

use strict;

package NeotomaInterface;

use JSON::SL;


use parent 'BaseInterface';

our ($SERVICE_LABEL) = 'neotoma';


my ($VALID_AGE) = qr{ ^ (?: \d+ | \d+ [.] \d* | \d* [.] \d+ ) $ }xs;

sub subquery_occs_list {
    
    my ($subservice, $request) = @_;
    
    my @params;
    
    # Process the taxon name parameter, if any
    
    if ( my $name = $request->clean_param('base_name') )
    {
	push @params, "taxonname=$name";
	push @params, "nametype=base";
    }
    
    elsif ( $name = $request->clean_param('taxon_name') )
    {
	push @params, "taxonname=$name";
	push @params, "nametype=tax";
    }
    
    elsif ( $name = $request->clean_param('match_name') )
    {
	push @params, "taxonname=$name";
	push @params, "nametype=match";
    }
    
    # Check for bbox parameter if any
    
    if ( my $bbox = $request->clean_param('bbox') )
    {
	push @params, "coords=$bbox";
    }
    
    # Then check for the occ_id && ds parameters
    
    if ( ref $request->{ds_hash} eq 'HASH' )
    {
	return unless $request->{ds_hash}{neotoma};
    }
    
    if ( my @occ_ids = $request->clean_param_list('occ_id') )
    {
	my @neotoma_ids;
	
	foreach my $id ( @occ_ids )
	{
	    if ( ref $id eq 'Composite::ExtIdent' )
	    {
		if ( $id->{domain} eq 'neotoma' || ( $id->{domain} eq '' &&
						     $request->{ds_single} eq 'neotoma' ) )
		{
		    if ( $id->{type} eq 'occ' || $id->{type} eq '' || $id->{type} eq 'unk' )
		    {
			push @neotoma_ids, $id->{num};
		    }
		    
		    else
		    {
			$request->add_warning("Invalid object type '$id->{type}' for parameter " .
					      "'occ_id': must be 'occ' to indicate a Neotoma occurrence.");
		    }
		}
	    }
	    
	    elsif ( ! ref $id && $id > 0 && $request->{ds_single} eq 'neotoma' )
	    {
		push @neotoma_ids, $id;
	    }
	    
	    elsif ( defined $id && $id ne '' )
	    {
		$request->add_warning("Invalid identifier '$id' for parameter 'occ_id'");
	    }
	}
	
	# If we found valid neotoma identifiers, then add the specified
	# parameter. 
	
	if ( @neotoma_ids )
	{
	    my $id_list = join(',', @neotoma_ids);
	    push @params, "occurid=$id_list";
	}
	
	# If we did not find any, also return false. In either of these cases,
	# there will be no matching records from the Neotoma database.
	
	else
	{
	    return;
	}
    }
    
    # Then check for site_id parameter
    
    if ( my @site_ids = $request->clean_param_list('site_id') )
    {
	my @neotoma_ids;
	
	foreach my $id ( @site_ids )
	{
	    if ( ref $id eq 'Composite::ExtIdent' )
	    {
		if ( $id->{domain} eq 'neotoma' || ( $id->{domain} eq '' &&
						     $request->{ds_single} eq 'neotoma' ) )
		{
		    if ( $id->{type} eq 'sit' || $id->{type} eq '' || $id->{type} eq 'unk' )
		    {
			push @neotoma_ids, $id->{num};
		    }
		    
		    else
		    {
			$request->add_warning("Invalid object type '$id->{type}' for parameter " .
					      "'site_id': must be 'sit' to indicate a Neotoma site.");
		    }
		}
	    }
	    
	    elsif ( ! ref $id && $id > 0 && $request->{ds_single} eq 'neotoma' )
	    {
		push @neotoma_ids, $id;
	    }
	    
	    elsif ( defined $id && $id ne '' )
	    {
		$request->add_warning("Invalid identifier '$id' for parameter 'site_id'");
	    }
	}
	
	# If we found valid neotoma identifiers, then add the specified parameter.
	
	if ( @neotoma_ids )
	{
	    my $id_list = join(',', @neotoma_ids);
	    push @params, "siteid=$id_list";
	}
	
	# If we did not find any, also return false. In either of these cases,
	# there will be no matching records from the Neotoma database.
	
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
	$max_age *= 1000000 if defined $max_ageunit && $max_ageunit eq 'ma';
	push @params, "ageold=$max_age";
    }
    
    my $min_age = $request->{my_min_age};
    my $min_ageunit = $request->{my_min_unit};
    
    if ( $min_age )
    {
	$min_age *= 1000000 if defined $min_ageunit && $min_ageunit eq 'ma';
	push @params, "ageyoung=$min_age";
    }
    
    # Make sure we have at least one parameter, so that we don't ask for the
    # entire set of occurrences in the database.
    
    unless ( @params )
    {
	die "400 You must specify at least one parameter";
    }
    
    # If the timerule is to be other than the default, add the necessary
    # parameters: 
    
    die "error, my_timerule not set" unless defined $request->{my_timerule};
    
    if ( $request->{my_timerule} ne 'contain' )
    {
	push @params, "agedocontain=0";
    }
    
    # Create the necessary objects to execute a query on the Neotoma database
    # and parse the results.
    
    my $json_parser = JSON::SL->new(10);
    $json_parser->set_jsonpointer(["/success", "/message", "/data/^"]);
    # $json_parser->noqstr(1);
    # $json_parser->nopath(1);
    
    my $url = $request->ds->config_value('neotoma_base') . 'occurrences?';
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
	return unless $request->{ds_hash}{neotoma};
    }
    
    my @occ_ids = $request->clean_param_list('occ_id');
    
    my @neotoma_ids;
    
    foreach my $id ( @occ_ids )
    {
	if ( ref $id eq 'Composite::ExtIdent' )
	{
	    if ( $id->{domain} eq 'neotoma' || ( $id->{domain} eq '' &&
						 $request->{ds_single} eq 'neotoma' ) )
	    {
		if ( $id->{type} eq 'occ' || $id->{type} eq '' || $id->{type} eq 'unk' )
		{
		    push @neotoma_ids, $id->{num};
		}
		
		else
		{
		    $request->add_warning("Invalid object type '$id->{type}' for parameter " .
					  "'occ_id': must be 'occ' to indicate a Neotoma occurrence.");
		}
	    }
	}
	
	elsif ( ! ref $id && $id > 0 && $request->{ds_single} eq 'neotoma' )
	{
	    push @neotoma_ids, $id;
	}
	
	elsif ( defined $id && $id ne '' )
	{
	    $request->add_warning("Invalid identifier '$id' for parameter 'occ_id'");
	}
    }
    
    # If we found valid neotoma identifiers, then add the specified
    # parameter. 
    
    if ( @neotoma_ids )
    {
	my $id_list = join(',', @neotoma_ids);
	push @params, "occurid=$id_list";
    }
    
    # If we did not find any, also return false. In either of these cases,
    # there will be no matching records from the Neotoma database.
    
    else
    {
	return;
    }
    
    # Create the necessary objects to execute a query on the Neotoma database
    # and parse the results.
    
    my $json_parser = JSON::SL->new(10);
    $json_parser->set_jsonpointer(["/success", "/message", "/data/^"]);
    # $json_parser->noqstr(1);
    # $json_parser->nopath(1);
    
    my $url = $request->ds->config_value('neotoma_base') . 'occurrences?';
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
	if ( $r->{Path} =~ /data/ )
	{
	    push @records, $r->{Value};
	}
	
	elsif ( $r->{Path} =~ /success/ )
	{
	   push @warnings, "Neotoma: Request failed" unless $r->{Value};
	}
	
	elsif ( $r->{Path} =~ /message/ )
	{
	    push @warnings, "Neotoma: $r->{Value}";
	}
    }
    
    my $count = scalar(@records);
    my $wcount = scalar(@warnings);
    
    my $message = "Got NEOTOMA response chunk: $count records";
    $message .= " $wcount warnings" if $wcount;
    $message .= " STATUS $request->{status}" if $request->{status} && $request->{status} ne '200';
    
    # $request->ds->debug_line($message);
    
    $request->add_warning(@warnings) if @warnings;
    
    # Before we return the results, see if we might need to filter the results.
    
    $request->{neotoma_count} += scalar(@records);
    
    my $ageunit = $request->clean_param('ageunit');
    
    foreach my $r (@records)
    {
	process_neotoma_age($request, $r, $ageunit);
    }
    
    if ( $request->{my_timerule} eq 'major' || $request->{my_timerule} eq 'buffer' )
    {
	@records = grep { $request->time_filter($_) } @records;
	
	if ( scalar(@records) < $count )
	{
	    my $diff = $count - scalar(@records);
	    $request->{neotoma_removed} += $diff;
	}
    }
    
    return @records;
}


sub process_neotoma_age {
    
    my ($request, $record, $ageunit) = @_;
    
    $record->{AgeOlder} //= $record->{Age};
    $record->{AgeYounger} //= $record->{Age};
    
    if ( defined $record->{AgeOlder} || defined $record->{AgeYounger} )
    {
	$record->{AgeUnit} = 'ybp';
	$record->{age_older} = $record->{AgeOlder};
	$record->{age_younger} = $record->{AgeYounger};
	
	if ( defined $ageunit && $ageunit eq 'ma' )
	{
	    $record->{AgeUnit} = 'Ma';
	    $record->{AgeOlder} /= 1E6 if defined $record->{AgeOlder} && $record->{AgeOlder} ne '';
	    $record->{AgeYounger} /= 1E6 if defined $record->{AgeYounger} && $record->{AgeOlder} ne '';
	}
    }
}


my ($VALID_COORD) = qr{ ^ [-]? \d+ [.] \d* $ }xs;

sub process_coords {
    
    my ($request, $record) = @_;
    
    # If we have two valid longitude coordinates, average them. Otherwise, use
    # the west if it is non-empty and the east one otherwise.
    
    if ( defined $record->{LongitudeEast} && 
         $record->{LongitudeEast} =~ $VALID_COORD && 
	 defined $record->{LongitudeWest} &&
	 $record->{LongitudeWest} =~ $VALID_COORD )
    {
	$record->{lng} = ( $record->{LongitudeEast} + $record->{LongitudeWest} ) / 2;
    }
    
    elsif ( defined $record->{LongitudeWest} && $record->{LongitudeWest} ne '' )
    {
	$record->{lng} = $record->{LongitudeWest};
    }
    
    else
    {
	$record->{lng} = $record->{LongitudeEast};
    }
    
    # If we have two valid latitude coordinates, average them. Otherwise, use
    # the south if it is non-empty and the north one otherwise.
    
    if ( defined $record->{LatitudeNorth} && 
         $record->{LatitudeNorth} =~ $VALID_COORD && 
	 defined $record->{LatitutdeSouth} &&
	 $record->{LatitutdeSouth} =~ $VALID_COORD )
    {
	$record->{lat} = ( $record->{LatitudeNorth} + $record->{LatitutdeSouth} ) / 2;
    }
    
    elsif ( defined $record->{LatitutdeSouth} && $record->{LatitutdeSouth} ne '' )
    {
	$record->{lat} = $record->{LatitutdeSouth};
    }
    
    else
    {
	$record->{lat} = $record->{LatitudeNorth};
    }   
}

# sub process_occs_list_old {
    
#     my ($subquery, $chunk) = @_;
    
#     next unless defined $chunk && $chunk ne '';
    
#     my $parser = $subquery->{parser};
#     my $request = $subquery->{request};
    
#     my @records = map { $_->{Value} } $parser->feed($chunk);
#     my $count = scalar(@records);
    
#     $request->ds->debug_line("Got NEOTOMA response chunk: $count records");
    
#     return @records;
# }

1;
