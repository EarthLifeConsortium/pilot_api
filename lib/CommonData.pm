# 
# CommonData
# 
# A class that contains common routines for formatting and processing Neotoma data.
# 
# Author: Michael McClennen

package CommonData;

use strict;

use HTTP::Validate qw(:validators);
use Carp qw(croak);

use Moo::Role;


our ($COMMON_OPT_RE) = qr{ ^ (?: ( taxa ) _ )?
			     ( created_before | created_after | 
			       modified_before | modified_after ) $ }xs;


# Initialization
# --------------

# initialize ( )
# 
# This routine is called once by the Web::DataService module, to initialize this
# output class.

sub initialize {
    
    my ($class, $ds) = @_;
    
    $ds->define_ruleset('1.0:special_params' => 
	"The following parameters can be used with most requests:",
	{ optional => 'SPECIAL(limit)' },
	{ optional => 'SPECIAL(offset)' },
	{ optional => 'SPECIAL(count)' },
	{ optional => 'SPECIAL(datainfo)' },
	{ optional => 'strict', valid => FLAG_VALUE },
	    "If specified, then any warnings will result in an error response.",
	    "You can use this parameter to make sure that all of your parameters",
	    "have proper values.  Otherwise, by default, the result will be",
	    "generated using good values and ignoring bad ones.",
	{ optional => 'textresult', valid => FLAG_VALUE },
	    "If specified, then the result will be given a content type of 'text/plain'.",
	    "With most browsers, that will cause the result to be displayed directly",
	    "instead of saved to disk.  This parameter does not need any value.",
	{ optional => 'markrefs', valid => FLAG_VALUE },
	    "If specified, then formatted references will be marked up with E<lt>bE<gt> and E<lt>iE<gt> tags.",
	    "This parameter does not need a value.",
	{ optional => 'SPECIAL(vocab)' },
	{ optional => 'SPECIAL(save)' },
	">>The following parameters are only relevant to the text formats (.csv, .tsv, .txt):",
	{ optional => 'noheader', valid => FLAG_VALUE },
	    "If specified, then the header line which gives the field names is omitted.",
	    "This parameter does not need any value.  It is equivalent to \"header=no\".",
	{ optional => 'SPECIAL(linebreak)', alias => 'lb' },
	{ optional => 'SPECIAL(header)', undocumented =>  1 },
	{ ignore => 'splat' });
    
    $ds->define_ruleset('1.0:common:select_crmod' =>
	{ param => 'created_before', valid => \&datetime_value },
	    "Select only records that were created before the specified L<date or date/time|/data1.0/datetime>.",
	{ param => 'created_after', valid => \&datetime_value, alias => 'created_since' },
	    "Select only records that were created on or after the specified L<date or date/time|/data1.0/datetime>.",
	{ param => 'modified_before', valid => \&datetime_value },
	    "Select only records that were last modified before the specified L<date or date/time|/data1.0/datetime>.",
	{ param => 'modified_after', valid => \&datetime_value, alias => 'modified_since' },
	    "Select only records that were modified on or after the specified L<date or date/time|/data1.0/datetime>.");
    
    $ds->define_block('1.0:common:crmod' =>
	{ select => ['$cd.RecDateCreated', '$cd.RecDateModified'], tables => '$cd' },
	{ output => 'RecDateCreated' },
	  "The date and time at which this record was created.",
	{ output => 'RecDateModified' },
	  "The date and time at which this record was last modified.");
    
}


# datetime_value ( )
# 
# Validate a date or date/time value.

my (%UNIT_MAP) = ( d => 'DAY', m => 'MINUTE', h => 'HOUR', w =>'WEEK', M => 'MONTH', Y => 'YEAR' );

sub datetime_value {
    
    my ($value, $context) = @_;
    
    my $dbh = $NeotomaData::ds0->get_connection;
    my $quoted = $dbh->quote($value);
    my $clean;
    
    # If we were given a number of days/hours, then treat that as "ago".
    
    if ( $value =~ /^(\d+)([mhdwMY])$/xs )
    {
	if ( $2 eq 'm' || $2 eq 'h' )
	{
	    ($clean) = $dbh->selectrow_array("SELECT DATE_SUB(NOW(), INTERVAL $1 $UNIT_MAP{$2})");
	}
	
	else
	{
	    ($clean) = $dbh->selectrow_array("SELECT DATE_SUB(CURDATE(), INTERVAL $1 $UNIT_MAP{$2})");
	}
    }
    
    else {
	($clean) = $dbh->selectrow_array("SELECT CONVERT($quoted, datetime)");
    }
    
    if ( $clean )
    {
	return { value => "$clean" };
    }
    
    else
    {
	return { error => "the value of {param} must be a valid date or date/time as defined by the MySQL database (was {value})" };
    }
}


# generate_common_filters ( table_short, select_table, tables_hash )
# 
# 

sub generate_common_filters {
    
    my ($request, $select_table, $tables_hash) = @_;
    
    my $dbh = $request->get_connection;
    
    my @params = $request->param_keys();
    my @filters;
    
    foreach my $key ( @params )
    {
	next unless $key =~ $COMMON_OPT_RE;
	
	my $prefix = $1 || 'bare';
	my $option = $2;
	
	next if defined $prefix && defined $select_table->{$prefix} && $select_table->{$prefix} eq 'ignore';
	
	my $value = $request->clean_param($key);
	next unless defined $value && $value ne '';    
	
	my $quoted; $quoted = $dbh->quote($value) unless ref $value;
	
	my $t = $select_table->{$prefix} || die "Error: bad common option prefix '$prefix'";
	
	$tables_hash->{$t} = 1 if ref $tables_hash;
	
	if ( $option eq 'created_after' )
	{
	    push @filters, "$t.created >= $quoted";
	}
	
	elsif ( $option eq 'created_before' )
	{
	    push @filters, "$t.created < $quoted";
	}
	
	elsif ( $option eq 'modified_after' )
	{
	    push @filters, "$t.modified >= $quoted";
	}
	
	elsif ( $option eq 'modified_before' )
	{
	    push @filters, "$t.modified < $quoted";
	}
	
	else
	{
	    die "Error: bad common option '$option'";
	}
    }
    
    return @filters;
}


# generate_crmod_filters ( table_name )
# 
# Generate the proper filters to select records by date created/modified.

sub generate_crmod_filters {

    my ($request, $table_name, $tables_hash) = @_;
    
    my @filters;
    
    if ( my $dt = $request->clean_param('created_after') )
    {
	push @filters, "$table_name.created >= $dt";
    }
    
    if ( my $dt = $request->clean_param('created_before') )
    {
	push @filters, "$table_name.created < $dt";
    }
    
    if ( my $dt = $request->clean_param('modified_after') )
    {
	push @filters, "$table_name.modified >= $dt";
    }
    
    if ( my $dt = $request->clean_param('modified_before') )
    {
	push @filters, "$table_name.modified < $dt";
    }
    
    $tables_hash->{$table_name} = 1 if @filters && ref $tables_hash eq 'HASH';
    
    return @filters;
}


# generateAttribution ( )
# 
# Generate an attribution string for the given record.  This relies on the
# fields "a_al1", "a_al2", "a_ao", and "a_pubyr".

sub generateAttribution {

    my ($request, $row) = @_;
    
    my $auth1 = $row->{a_al1} || '';
    my $auth2 = $row->{a_al2} || '';
    my $auth3 = $row->{a_ao} || '';
    my $pubyr = $row->{a_pubyr} || '';
    
    $auth1 =~ s/( Jr)|( III)|( II)//;
    $auth1 =~ s/\.$//;
    $auth1 =~ s/,$//;
    $auth2 =~ s/( Jr)|( III)|( II)//;
    $auth2 =~ s/\.$//;
    $auth2 =~ s/,$//;
    
    my $attr_string = $auth1;
    
    if ( $auth3 ne '' or $auth2 =~ /et al/ )
    {
	$attr_string .= " et al.";
    }
    elsif ( $auth2 ne '' )
    {
	$attr_string .= " and $auth2";
    }
    
    $attr_string .= " $pubyr" if $pubyr ne '';
    
    if ( $attr_string ne '' )
    {
	$attr_string = "($attr_string)" if defined $row->{orig_no} &&
	    $row->{orig_no} > 0 && defined $row->{taxon_no} && 
		$row->{orig_no} != $row->{taxon_no};
	
	return $attr_string;
    }
    
    return;
}


# safe_param_list ( param_name, bad_value )
# 
# If the specified parameter was not given, return undefined.  If it was given
# one or more good values, return a list of them.  If it was given only bad
# values, return $bad_value (or default to -1).

sub safe_param_list {
    
    my ($request, $param, $bad_value) = @_;
    
    unless ( $request->param_given($param) )
    {
	return;
    }
    
    my @values = grep { $_ && $_ ne '' } $request->clean_param_list($param);
    
    unless ( @values )
    {
	push @values, ($bad_value // -1);
    }
    
    return @values;
}


# strict_check ( )
# 
# If the special parameter 'strict' was specified, and if any warnings have
# been generated for this request, then return an error.

sub strict_check {
    
    my ($request) = @_;
    
    if ( $request->clean_param('strict') && $request->warnings )
    {
	die "400 Bad parameter values\n";
    }
}


1;
