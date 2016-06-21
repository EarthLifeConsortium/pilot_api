


package URLParam;

use URI::Escape qw(uri_escape_utf8);
use Exporter qw(import);

our (@EXPORT) = qw(url_param);


# url_param ( param, value )
# 
# Return a string consisting of "param=value" where value has been URL encoded.

sub url_param {
    
    my ($param, $value) = @_;
    
    my $encoded = uri_escape_utf8($value, "^A-Za-z0-9\-\._~,*()!");
    
    return "$param=$encoded";
}

