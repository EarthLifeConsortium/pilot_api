

use strict;

package NeotomaInterface;

use JSON::SL;
use Try::Tiny;
use URLParam;

use parent 'CompositeSubquery';

our ($SERVICE_LABEL) = 'Neotoma';


my ($VALID_AGE) = qr{ ^ (?: \d+ | \d+ [.] \d* | \d* [.] \d+ ) $ }xs;

sub init_occs_list {
    
    my ($subquery, $request) = @_;
    
    my @params;
    
    # First check which databases we are supposed to be querying.
    
    if ( ref $request->{ds_hash} eq 'HASH' )
    {
	return unless $request->{ds_hash}{neotoma};
    }
    
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
    
    elsif ( my $id = $request->clean_param('base_id') || $request->clean_param('taxon_id') )
    {
	my $nametype = 'base';
	$nametype = 'tax' if $request->param_given('taxon_id');
	
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
		if ( PBDBInterface->can('init_taxa_single') )
		{
		    my $cq = $subquery->{cq};
		    my $sq = PBDBInterface->new_subquery($cq, secondary => 1,
							 init_method => 'init_taxa_single',
							 proc_method => 'proc_taxa_list');
		    
		    $sq->{cv_done}->recv;
		    
		    my ($taxon_record) = @{$sq->{records}};
		    
		    unless ( $sq->{http_status} eq '200' && ref $taxon_record )
		    {
			my $reason = $sq->{http_reason} || 'unknown response';
			$request->add_warning("Error looking up taxon '$id': $reason");
			return;
		    }
		    
		    my $taxon_name = $taxon_record->{taxon_name};
		    push @params, "taxonname=$taxon_name";
		    push @params, "nametype=$nametype";
		}
	    }
	    
	    elsif ( $id->{domain} eq 'neotoma' )
	    {
		
		push @params, "taxonids=$id->{num}";
		push @params, "nametype=$nametype";
	    }
	}
	
	else
	{
	    push @params, "taxonids=$id";
	    push @params, "nametype=$nametype";
	}
    }
    
    # Check for bbox parameter if any
    
    if ( my $bbox = $request->clean_param('bbox') )
    {
	push @params, "bbox=$bbox";
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
    
    # my $json_parser = JSON::SL->new(10);
    # $json_parser->set_jsonpointer(["/success", "/message", "/data/^"]);
    # $json_parser->noqstr(1);
    # $json_parser->nopath(1);
    
    # $subquery->{parser} = $json_parser;
    
    my $url = $request->ds->config_value('neotoma_base') . 'occurrences?';
    $url .= join('&', @params);
    
    return $url;
}


sub init_occs_single {

    my ($subquery, $request) = @_;
    
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
    
    # my $json_parser = JSON::SL->new(10);
    # $json_parser->set_jsonpointer(["/success", "/message", "/data/^"]);
    # $json_parser->noqstr(1);
    # $json_parser->nopath(1);
    
    my $url = $request->ds->config_value('neotoma_base') . 'occurrences?';
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
        
    # my $message = "Got NEOTOMA response chunk: $count records";
    # $message .= " $wcount warnings" if $wcount;
    # $message .= " STATUS $request->{status}" if $request->{status} && $request->{status} ne '200';
    
    # $request->ds->debug_line($message);
    
    # if ( my @warnings = $subquery->warnings )
    # {
    # 	$request->add_warning(@warnings);
    # }
    
    $request->{neotoma_count} += $count;
    
    # Process the results and return them.
    
    # my @records = $subquery->records;
    
    my $ageunit = $request->clean_param('ageunit');
    
    # foreach my $r (@records)
    # {
    # 	process_neotoma_age($request, $r, $ageunit);
    # }
    
    $subquery->process_records('process_neotoma_age', $ageunit);
    
    if ( $request->{my_timerule} eq 'major' || $request->{my_timerule} eq 'buffer' )
    {
	# @records = grep { $request->time_filter($_) } @records;
	
	my $reduced_count = $subquery->filter_records( sub { $request->time_filter(@_) } );
	
	if ( $reduced_count < $count )
	{
	    my $diff = $count - $reduced_count;
	    $subquery->{removed} += $diff;
	}
    }
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


# generate_parser ( request )
# 
# This method must return a parser object which will be used to parse the
# subquery response, or else the undefined value if no parser is needed or
# none is available.

sub generate_parser {
    
    my ($subquery, $request) = @_;
    
    my $json_parser = JSON::SL->new(10);
    $json_parser->set_jsonpointer(["/success", "/message", "/data/^"]);
    
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
	if ( $r->{Path} =~ /data/ )
	{
	    $subquery->add_record($r->{Value});
	}
	
	elsif ( $r->{Path} =~ /success/ )
	{
	   $subquery->add_warning("Request failed") unless $r->{Value};
	}
	
	elsif ( $r->{Path} =~ /message/ )
	{
	    $subquery->add_warning($r->{Value});
	}
    }
}

1;
