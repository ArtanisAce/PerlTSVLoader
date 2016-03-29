use strict;
use warnings;
use DBI;
use Text::CSV;
use Path::Tiny qw(path);
use Data::Dumper qw(Dumper);
use 5.014;

# Quit unless we have the correct number of command-line args
# there should be two: a folder path and a file name
my $num_args = $#ARGV + 1;
if ($num_args != 2) {
    say "Not enough parameters entered. Usage: loader.pl ./dir/subdir filename.tsv";
    exit;
}
# Extract values entered by user and compose the filename path
my $path_entered=$ARGV[0];
my $filename_entered=$ARGV[1];

my $filename = 	$path_entered . $filename_entered;
say "Loading from path: " . $filename;
	

# Connection to MySQL DB with credentials
my $driver = "mysql"; 
my $database = "loader";
my $dsn = "DBI:$driver:database=$database";
my $userid = "root";
my $password = "test";
my $dbh = DBI->connect($dsn, $userid, $password ) or die $DBI::errstr;

	if ($dbh){
		say "Connection with DB stablished";
	}

# Read tab separated file from path and convert the data to an array of hashes references.
my @rows;
my $csv = Text::CSV->new ( { binary => 1, sep_char => "\t" } )
                   or die "Error creating TSV object: ".Text::CSV->error_diag ();
 
open my $fh, "<:encoding(utf8)", "$filename"
                    or die "Error reading CSV file: $!";

$csv->column_names ($csv->getline ($fh)); # Get header names
while ( my $row_record = $csv->getline_hr( $fh ) ) {
     # Process record
     push @rows, $row_record;
}
$csv->eof or $csv->error_diag();
close $fh;

if (!@rows){
	say "No records on the file. No action was taken on DB";
	exit;
}

# Array of hash references with all the values from the DB table
my $hashref_db = $dbh->selectall_arrayref( 
      'SELECT * FROM users',
      { Slice => {} } );
      
   
# Compare values on both hash ref arrays, and take actions in consequence:
foreach my $fileRow (@rows){
	
	# This variable will be a boolean substitute to check for new record on file
	# 'true' as default
	my $new_rec = "Yes";

	# Obtain values from file record
	my %hash_file = %$fileRow;
	my $record_ID = $hash_file{ID};
	my $record_Name = $hash_file{Name};
	my $record_Date = $hash_file{Date};
	
	foreach my $tableRow (@{$hashref_db}){
		
		# Obtain values from db row
		my %hash_db = %$tableRow;
		my $row_ID = $hash_db{ID};
		my $row_Name = $hash_db{Name};
		my $row_Date = $hash_db{Date};
		
		# Compare data and call for subroutines to take actions
		if ($record_ID eq $row_ID){
			$new_rec="";# New record = false
			IDexists($record_Name, $record_Date, $record_ID);
			}
		
		elsif (($record_Name eq $row_Name)){
			$new_rec="";
			update_detected($record_ID, $record_Name, $record_Date, $row_Name, $record_Name);		
			}
		elsif ($record_Date eq $row_Date){
			$new_rec="";
			update_detected($record_ID, $record_Name, $record_Date, $row_Date, $record_Date);							
			}
				
		}
		
	if ($new_rec){
		new_record($record_ID,$record_Name,$record_Date);
		say "New record added on DB!";
	}	
}
    
# Update DB with file content
# If there already exists record in DB with given ID, that record is updated with information
# from file and Update_timestamp is set to system current timestamp.
sub IDexists{
	my $query = "UPDATE users SET Name=?, Date=?, Update_timestamp=now() WHERE ID=?";
	my $result = $dbh->do($query, {}, $_[0],$_[1],$_[2]);
	say "DB record updated!";
}

# If new record is detected in file, new record is created in DB with attributes from file
# and Update_timestamp is set to system current timestamp.
sub new_record{
	my $query = "INSERT INTO users (ID,Name,Date,Update_timestamp) VALUES (?,?,?,now())";
	my $result = $dbh->do($query, {}, $_[0],$_[1],$_[2]);
	say "New record added on DB!";
}

# If any update is detected, record in DB is updated and Update_timestamp is set to system current timestamp.
sub update_detected{
	my $query = "UPDATE users SET ID=?, Name=?, Date=?, Update_timestamp=now() WHERE ?=?";
	my $result = $dbh->do($query, {}, $_[0],$_[1],$_[2],$_[3],$_[4]);
	say "DB record updated!";
}
	
# Move file to 'executed' folder
my $executed_path = "./loader_files/executed/";
my $executed_file = $executed_path . $ARGV[1];
path($filename)->move($executed_file);