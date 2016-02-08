#
# ConfigData
# 
# A classs that returns information from the Neotoma database about the
# values necessary to properly handle the data returned by other queries.
# 
# Author: Michael McClennen

package ConfigData;

use strict;

use Carp qw(carp croak);

our (@REQUIRES_ROLE) = qw(CommonData);

use Moo::Role;


our (%CONFIG_CACHE);


# Initialization
# --------------

# initialize ( )
# 
# This routine is called once by the Web::DataService module, to initialize this
# output class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    # We start by defining an output map that lists the output blocks to be
    # used in generating responses for the operation defined by this class.
    # Each block is assigned a short key.
    
    $ds->define_set('1.0:config:config_map' =>
	{ value => 'taxagroups', maps_to => '1.0:config:taxagroups' },
	    "Return information about the taxonomic groups defined in this database.",
	{ value => 'all', maps_to => '1.0:config:all' },
	    "Return all of the above blocks of information.",
	    "This is generally useful only with C<json> format.");
    
    # Next, define these output blocks.
    
    $ds->define_block('1.0:config:taxagroups' =>
	{ output => 'config_section', value => 'tgp', if_field => 'TaxaGroupID' },
	    "The configuration section: 'tgp' for taxonomic groups",
	{ output => 'TaxaGroupID' },
	    "Taxonomic group identifier",
	{ output => 'TaxaGroup' },
	    "The kind of organism represented by this taxonomic group identifier.");
        
    $ds->define_block('1.0:config:all' =>
	{ include => 'taxagroups' });
    
    # Then define a ruleset to interpret the parmeters accepted by operations
    # from this class.
    
    $ds->define_ruleset('1.0:config' =>
	"The following URL parameters are accepted for this path:",
	{ param => 'show', valid => '1.0:config:config_map', list => ',' },
	    "The value of this parameter selects which information to return:",
	{ allow => '1.0:special_params' },
	"^You can also use any of the L<special parameters|node:special> with this request.");
    
    
}


# Data service operations
# -----------------------

# get ( )
# 
# Return configuration information.  Fetch it unless this process has already
# cached the requested information.

sub get {

    my ($request) = @_;
    
    my $show_all; $show_all = 1 if $request->has_block('all');
    my @result;
    
    if ( $request->has_block('taxagroups') or $show_all )
    {
	$CONFIG_CACHE{taxagroups} ||= $request->fetch_taxagroups;
	push @result, @{$CONFIG_CACHE{taxagroups}};
    }
    
    if ( my $offset = $request->result_offset(1) )
    {
    	splice(@result, 0, $offset);
    }
    
    print STDERR "CONFIG REQUEST" . "\n\n" if $request->debug;
    
    $request->list_result(@result);
}


sub fetch_taxagroups {
    
    my ($request) = @_;
    
    my $dbh = $request->get_connection;
    
    my $sql = "
	SELECT TaxaGroupID, TaxaGroup FROM TaxaGroupTypes";
    
    my $result = $dbh->selectall_arrayref($sql, { Slice => {} });
    
    return $result;
}

1;
