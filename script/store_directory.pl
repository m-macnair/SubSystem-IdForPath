use strict;
use warnings;
use SubSystem::IdForPath::CachedDB;
use Try::Tiny;
use Carp;
use File::Slurp;
main( @ARGV );

sub main {
	Carp::croak( "Supplied parameter [$_[0]] is not a valid directory" ) unless ( -d $_[0] );

	my $db_file = time . "_path_ids.sqlite";
	my $obj     = SubSystem::IdForPath::CachedDB->new(
		{
			dsn => [
				"dbi:SQLite:$db_file",
				undef, undef,
				{
					AutoCommit                 => 0,
					RaiseError                 => 1,
					sqlite_see_if_its_a_number => 1,
				}
			],
		}
	);

	my $sql_strings = read_file( "./etc/sqlite_schema.sql" );
	for ( split( $/, $sql_strings ) ) {
		$obj->dbh->do( $_ );
	}

	$obj->default_source_id( 2 );

	findfilesub(
		"$_[0]",
		sub {
			my ( $file_path ) = @_;
			return unless -f $file_path;
			my $progress = "$file_path ... ";
			my $size     = -s $file_path;
			if ( $size > 75000000 ) {
				warn( "$progress Skipping due to size of $size" );
				return;
			}
			for ( qw/ .txt .yml .html / ) {
				if ( index( $file_path, $_ ) != -1 ) {
					warn( "$progress Skipping due to likely file type of $_" );
					return;
				}
			}
			my $die;
			try {
				$obj->store_local_file( $file_path );
			}
			catch {
				warn( "$progress failed $_" );
				$die = 1;
			};
			die if $die;

		}
	);

	$obj->clean_finish();
	print "$/It is done. Move on.$/";
}

sub findfilesub {
	my ( $dir, $sub ) = @_;
	die "[$dir] is not a directory" unless ( -d $dir );
	require File::Find;

	File::Find::find(
		{
			wanted => sub {
				return if -d ( $File::Find::name );
				return if -l ( $File::Find::name );
				&$sub( $File::Find::name );
			},
			no_chdir => 1,
			follow   => 0,
		},
		$dir
	);
}
