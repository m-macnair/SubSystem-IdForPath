package SubSystem::IdForPath::CachedDB;
use strict;
use 5.006;
use warnings;
use base qw/
  SubSystem::CachedDB::DBI
  SubSystem::IdForPath
  /;

=head1 NAME
	SubSystem::IdForPath - Get integer path identifiers, using the CachedDB contraption
=head1 VERSION
	Version 0.01
=cut

our $VERSION = '0.01';

=head1 SYNOPSIS
	Use DBI and a cache to implement the IdForPath critical path
=head1 EXPORT
	None
=head1 SUBROUTINES/METHODS
=head2 Facilitators
	Specific to this module
=head3 _init
	Separate class instantiation and configuration for when that's a good idea
	
=cut

# TODO
# parametised table names

sub _init {
	my ( $self, $conf ) = @_;
	my $cdb_init = SubSystem::CachedDB::DBI::_init( $self, $conf );
	return $cdb_init unless $cdb_init->{pass};
	my $ifp_init = SubSystem::IdForPath::_init( $self, $conf );
	return $ifp_init unless $ifp_init->{pass};

	my @table_caches = qw/
	  paths_path_to_id
	  sources_hostname_to_id
	  files_sha1_to_id
	  types_suffix_to_id
	  names_name_to_id
	  instances_name_to_id
	  /;
	$self->mk_accessors( @table_caches );
	$self->init_cache_for_accessors( \@table_caches );

	return {pass => 1};
}

=head2 Place holders Overwrites
	Replacing CachedDB private methods with 'actually do something'
=cut

=head3 _get_set_id_for_file
	
=cut

sub _get_set_id_for_file {
	my ( $self, $p ) = @_;

	$self->_preserve_sth( "files.get_id_from_sha1()", "select id from files where sha1 = ?" ) unless $self->_preserve_sth( "files.get_id_from_sha1()" );
	$self->_preserve_sth( "files.new()",              "insert into files (sha1) values (?)" ) unless $self->_preserve_sth( "files.new()" );

	return $self->_cache_or_db_or_new(
		{
			cache          => "files_sha1_to_id",
			cache_key      => "files_id_from_sha1.$p->{sha1_digest}",
			get_sth_label  => "files.get_id_from_sha1()",
			get_sth_params => [ $p->{sha1_digest} ],
			set_sth_label  => "files.new()",
			set_sth_params => [ $p->{sha1_digest} ],
		}
	);

}

=head3 _get_set_id_for_instance
	
=cut

# TODO find that funky way of getting ordered key values from a href using an array directly

sub _get_set_id_for_instance {
	my ( $self, $p ) = @_;
	$self->_preserve_sth(
		"instances.get_id()",
		"select id 
		from instances 
		where file_id = ?
		and path_id = ?
		and source_id = ?
		and name_id = ?
		"
	) unless $self->_preserve_sth( "instances.get_id()" );
	$self->_preserve_sth( "instances.new()", "insert into instances (file_id,path_id,source_id,name_id) values (?,?,?,?)" ) unless $self->_preserve_sth( "instances.new()" );
	my $array_params = [ $p->{file_id}, $p->{path_id}, $p->{source_id}, $p->{name_id} ];
	my $key = "$p->{file_id},$p->{path_id},$p->{source_id},$p->{name_id}";
	return $self->_cache_or_db_or_new(
		{
			cache          => "instances_name_to_id",
			cache_key      => "instances_id_from_name.$key",
			get_sth_label  => "instances.get_id()",
			get_sth_params => $array_params,
			set_sth_label  => "instances.new()",
			set_sth_params => $array_params,
		}
	);
}

=head3 _get_set_id_for_name
	
=cut

sub _get_set_id_for_name {
	my ( $self, $name ) = @_;
	$self->_preserve_sth( "names.get_id_from_name()", "select id from names where name = ?" ) unless $self->_preserve_sth( "names.get_id_from_name()" );
	$self->_preserve_sth( "names.new()",              "insert into names (name) values (?)" ) unless $self->_preserve_sth( "names.new()" );

	return $self->_cache_or_db_or_new(
		{
			cache          => "names_name_to_id",
			cache_key      => "names_id_from_name.$name",
			get_sth_label  => "names.get_id_from_name()",
			get_sth_params => [$name],
			set_sth_label  => "names.new()",
			set_sth_params => [$name],
		}
	);
}

=head3 _get_set_id_for_path
	
=cut

sub _get_set_id_for_path {
	my ( $self, $path ) = @_;
	$self->_preserve_sth( "paths.get_id_from_path()", "select id from paths where path = ?" ) unless $self->_preserve_sth( "paths.get_id_from_path()" );
	$self->_preserve_sth( "paths.new()",              "insert into paths (path) values (?)" ) unless $self->_preserve_sth( "paths.new()" );

	return $self->_cache_or_db_or_new(
		{
			cache          => "paths_path_to_id",
			cache_key      => "paths_id_from_path.$path",
			get_sth_label  => "paths.get_id_from_path()",
			get_sth_params => [$path],
			set_sth_label  => "paths.new()",
			set_sth_params => [$path],
		}
	);
}

=head3 _get_set_id_for_type
	
=cut

# TODO implemente mime
sub _get_set_id_for_type {

	my ( $self, $p ) = @_;
	$self->demand_params( $p, [qw/suffix /] );
	$self->_preserve_sth( "types.get_id_from_suffix()", "select id from types where suffix = ?" ) unless $self->_preserve_sth( "types.get_id_from_suffix()" );
	$self->_preserve_sth( "types.new()", "insert into types (suffix,mime,name) values (?,?,?)" ) unless $self->_preserve_sth( "types.new()" );

	return $self->_cache_or_db_or_new(
		{
			cache          => "types_suffix_to_id",
			cache_key      => "types_id_from_suffix.$p",
			get_sth_label  => "types.get_id_from_suffix()",
			get_sth_params => [ $p->{suffix}, ],
			set_sth_label  => "types.new()",
			set_sth_params => [ $p->{suffix}, $p->{type}, $p->{protocol}, ],
		}
	);
}

=head3 _get_set_id_for_source
	
=cut

sub _get_set_id_for_source {
	my ( $self, $p ) = @_;
	$self->_preserve_sth( "sources.get_id_from_hostname()", "select id from sources where name = ?" ) unless $self->_preserve_sth( "sources.get_id_from_hostname()" );
	$self->_preserve_sth( "sources.new()", "insert into sources (name,type,protocol) values (?,?,?)" ) unless $self->_preserve_sth( "sources.new()" );

	return $self->_cache_or_db_or_new(
		{
			cache          => "sources_hostname_to_id",
			cache_key      => "sources_id_from_hostname.$p",
			get_sth_label  => "sources.get_id_from_hostname()",
			get_sth_params => [ $p->{hostname}, ],
			set_sth_label  => "sources.new()",
			set_sth_params => [ $p->{hostname}, $p->{type}, $p->{protocol}, ],
		}
	);
}

=head1 AUTHOR

mmacnair, C<< <mmacnair at cpan.org> >>

=head1 BUGS

	TODO Bugs

=head1 SUPPORT

	TODO Support

=head1 ACKNOWLEDGEMENTS
	TODO 

=head1 LICENSE AND COPYRIGHT

Copyright 2018 mmacnair.

This program is distributed under the (Revised) BSD License:
L<http://www.opensource.org/licenses/BSD-3-Clause>

Redistribution and use in source and binary forms, with or without
modification, are permitted provided that the following conditions
are met:

* Redistributions of source code must retain the above copyright
notice, this list of conditions and the following disclaimer.

* Redistributions in binary form must reproduce the above copyright
notice, this list of conditions and the following disclaimer in the
documentation and/or other materials provided with the distribution.

* Neither the name of mmacnair's Organization
nor the names of its contributors may be used to endorse or promote
products derived from this software without specific prior written
permission.

THIS SOFTWARE IS PROVIDED BY THE COPYRIGHT HOLDERS AND CONTRIBUTORS
"AS IS" AND ANY EXPRESS OR IMPLIED WARRANTIES, INCLUDING, BUT NOT
LIMITED TO, THE IMPLIED WARRANTIES OF MERCHANTABILITY AND FITNESS FOR
A PARTICULAR PURPOSE ARE DISCLAIMED. IN NO EVENT SHALL THE COPYRIGHT
OWNER OR CONTRIBUTORS BE LIABLE FOR ANY DIRECT, INDIRECT, INCIDENTAL,
SPECIAL, EXEMPLARY, OR CONSEQUENTIAL DAMAGES (INCLUDING, BUT NOT
LIMITED TO, PROCUREMENT OF SUBSTITUTE GOODS OR SERVICES; LOSS OF USE,
DATA, OR PROFITS; OR BUSINESS INTERRUPTION) HOWEVER CAUSED AND ON ANY
THEORY OF LIABILITY, WHETHER IN CONTRACT, STRICT LIABILITY, OR TORT
(INCLUDING NEGLIGENCE OR OTHERWISE) ARISING IN ANY WAY OUT OF THE USE
OF THIS SOFTWARE, EVEN IF ADVISED OF THE POSSIBILITY OF SUCH DAMAGE.


=cut

1;
