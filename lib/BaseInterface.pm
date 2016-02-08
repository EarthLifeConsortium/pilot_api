# 
# Base class for constituent subservices
# 


use strict;

package BaseInterface;

use Carp qw(carp croak);
use Try::Tiny;

# sub new {
    
#     my ($class, $name) = @_;
    
#     my $subservice = { label => $label };
    
#     return bless $subservice, $class;
# }


sub new_subquery {
    
    my ($class, %options) = @_;
    
    no strict 'refs';
    
    croak "you must specify a request" unless ref $options{request} eq 'REQ::CompositeData';
    croak "you must specify a URL" unless defined $options{url} && $options{url} ne '';
    
    my $label = ${"${class}::SERVICE_LABEL"};
    
    croak "you must specify a label" unless defined $label && $label ne '';
    
    my $format = $options{type} || 'json';
    
    $options{url} =~ s/ /%20/g;
    
    my $subquery = { label => $label, 
		     format => $format, 
		     request => $options{request},
		     url => $options{url} };
    
    $subquery->{parser} = $options{parser} if $options{parser};
    
    return bless $subquery, $class;
}


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

1;
