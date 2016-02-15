# 
# PaleoBioDB/Neotoma Composite Data Service
# 
# This application configures a data service that queries the Neotoma
# Database (MySQL version).  It is implemented using the Perl Dancer
# framework.
# 
# Author: Michael McClennen <mmcclenn@geology.wisc.edu>



package CompositeService;

use Web::DataService;

use CommonData;
use ConfigData;
use CompositeData;

use PBDBInterface;
use NeotomaInterface;

{
    # We start by defining a data service instance
    
    our ($ds0) = Web::DataService->new(
	{ name => '1.0',
	  title => 'Paleo Composite Data Service',
	  version => 'd1',
	  features => 'standard',
	  special_params => 'standard,count=rowcount',
	  path_prefix => 'comp1.0/',
	  ruleset_prefix => '1.0:',
	  doc_template_dir => 'doc/1.0' });
    
    # List the modules that will be used to communicate with the various
    # constituent services.
    
    our (@SERVICES) = qw(PBDBInterface NeotomaInterface);
    our ($TIMEOUT) = $ds0->config_value("composite_timeout") || 60;
    
    print STDERR "Overall timeout: $TIMEOUT\n";
    
    # We then define the vocabularies that will be used to label the data
    # fields returned by this service.
    
    $ds0->define_vocab(
        { name => 'null' },
	{ name => 'neotoma', title => 'Neotoma vocabulary' },
	    "The Neotoma vocabulary is based on the field names from the Neotoma database,",
	    "with a few extra fields.",
	{ name => 'pbdb', title => 'PaleoBioDB vocabulary' },
	    "The PaleoBioDB vocabulary is based on the field names from the Paleobiology",
	    "Database, with a few extra fields.",
	{ name => 'com', title => 'Compact vocabulary' },
	    "The compact vocabulary uses 3-character field names, to minimize",
	    "the size of the result records.",
	{ name => 'dwc', title => 'Darwin Core' },
	    "The Darwin Core vocabulary follows the L<Darwin Core standard|http://www.tdwg.org/standards/450/>",
	    "set by the L<TDWG|http://www.tdwg.org/>.  This includes both the field names and field values.",
	    "Because the Darwin Core standard is XML-based, it is very strict.  Many",
	    "but not all of the fields can be expressed in this vocabulary; those that",
	    "cannot are unavoidably left out of the response.");
    
    
    # Then the formats in which data can be returned.
    
    $ds0->define_format(
	{ name => 'json', content_type => 'application/json',
	  doc_node => 'formats/json', title => 'JSON' },
	    "The JSON format is intended primarily to support client applications.",
	{ name => 'xml', disabled => 1, content_type => 'text/xml', title => 'XML',
	  doc_node => 'formats/xml', disposition => 'attachment',
	  default_vocab => 'dwc' },
	    "The XML format is intended primarily to support data interchange with",
	    "other databases, using the Darwin Core element set.",
	{ name => 'txt', content_type => 'text/plain',
	  doc_node => 'formats/text', title => 'Comma-separated text' },
	    "The text formats (txt, tsv, csv) are intended primarily for researchers",
	    "downloading data from the database.  These downloads can easily be",
	    "loaded into spreadsheets or other analysis tools.",
	{ name => 'csv', content_type => 'text/csv',
	  disposition => 'attachment',
	  doc_node => 'formats/text', title => 'Comma-separated text' },
	    "The text formats (txt, tsv, csv) are intended primarily for researchers",
	    "downloading data from the database.  These downloads can easily be",
	    "loaded into spreadsheets or other analysis tools.",
	{ name => 'tsv', content_type => 'text/tab-separated-values', 
	  disposition => 'attachment',
	  doc_node => 'formats/text', title => 'Tab-separated text' },
	    "The text formats (txt, tsv, csv) are intended primarily for researchers",
	    "downloading data from the database.  These downloads can easily be",
	    "loaded into spreadsheets or other analysis tools.",
	# { name => 'html', content_type => 'text/html', doc_node => 'formats/html', title => 'HTML',
	#   module => 'Template', disabled => 1 },
	#     "The HTML format returns formatted web pages describing the selected",
	#     "object or objects from the database.",
	{ name => 'ris', disabled => 1, content_type => 'application/x-research-info-systems',
	  doc_node => 'formats/ris', title => 'RIS', disposition => 'attachment',
	  encode_as_text => 1, default_vocab => '', module => 'RISFormat'},
	    "The L<RIS format|http://en.wikipedia.org/wiki/RIS_(file_format)> is a",
	    "common format for bibliographic references.",
	{ name => 'png', disabled => 1, content_type => 'image/png', module => '',
	  default_vocab => '', doc_node => 'formats/png', title => 'PNG' },
	    "The PNG suffix is used with a few URL paths to fetch images stored",
	    "in the database.");
    
    
    # Then define the URL paths that this subservice will accept.  We start with
    # the root of the hierarchy, which sets defaults for all the rest of the nodes.
    
    $ds0->define_node({ path => '/', 
			public_access => 1,
			doc_default_op_template => 'operation.tt',
			allow_format => 'json,csv,tsv,txt',
			allow_vocab => 'neotoma,pbdb,dwc,com',
			default_save_filename => 'neotoma_data',
			title => 'Documentation' });
    
    # If a default_limit value was defined in the configuration file, get that
    # now so that we can use it to derive limits for certain nodes.
    
    # my $base_limit = $ds0->node_attr('/', 'default_limit');
    # my $taxa_limit = $base_limit ? $base_limit * 5 : undef;
    # $taxa_limit = 20000 if defined $taxa_limit && $taxa_limit < 20000;
    # my $ref_limit = $base_limit ? $base_limit * 5 : undef;
    # $ref_limit = 10000 if defined $ref_limit && $ref_limit < 10000;
    
    # Configuration. This path is used by clients who need to configure themselves
    # based on parameters supplied by the data service.
    
    # $ds0->define_node({ path => 'config',
    # 			place => 10,
    # 			title => 'Client configuration',
    # 			usage => [ "config.json?show=all",
    # 			 	   "config.txt?show=taxagroups" ],
    # 			role => 'ConfigData',
    # 			method => 'get',
    # 			optional_output => '1.0:config:config_map' },
    # 	"This class provides information about the structure, encoding and organization",
    # 	"of the information in the database. It is designed to enable the easy",
    # 	"configuration of client applications.");
    
    # Occurrences.  These paths are used to fetch information about fossil
    # occurrences known to the database.
    
    $ds0->define_node({ path => 'occs',
			place => 1,
			title => 'Fossil occurrences',
			usage => [ 'occs/list.txt?base_name=Canis&show=loc&vocab=pbdb',
				   'occs/single.json?occ_id=pbdb:occ:10245&vocab=neotoma' ],
			role => 'CompositeData',
			allow_format => '+xml' },
	"A fossil occurence represents the occurrence of a particular organism at a particular",
	"location in time and space. Each occurrence is a member of a single fossil collection,",
	"and has a taxonomic identification which may be more or less specific.");
    
    $ds0->define_node({ path => 'occs/list',
			place => 1,
			title => 'Lists of fossil occurrences',
			method => 'occs_list',
			output => '1.0:occs:basic',
			optional_output => '1.0:occs:basic_map',
			usage => [ 'occs/list.txt?base_name=Canis&show=loc&vocab=pbdb',
				   'occs/list.json?base_name=Poaceae&vocab=neotoma' ] },
	"This operation returns information about fossil occurrences, selected",
	"according to the criteria you specify.");
    
    $ds0->define_node({ path => 'occs/single',
		       place => 2,
		       title => 'Single fossil occurrence',
		       method => 'occs_single',
		       output => '1.0:occs:basic',
		       optional_output => '1.0:occs:basic_map',
		       usage => [ 'occs/single.json?occ_id=pbdb:occ:10245&vocab=neotoma' ] },
	"This operation returns information about a single fossil occurrence",
	"from one of the underlying databases.");
    
    # Documentation nodes
    
    $ds0->define_node({ path => 'special',
			ruleset => '1.0:special_params',
			title => 'Special parameters' });
    
    # And finally, stylesheets and such
    
    $ds0->define_node({ path => 'css',
			file_dir => 'css' });
    
    $ds0->define_node({ path => 'images',
			file_dir => 'images' });
};

1;
