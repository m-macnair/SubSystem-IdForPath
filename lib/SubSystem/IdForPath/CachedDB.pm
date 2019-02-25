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
	  file_md5s_md5_to_id
	  instance_names
	  instance_paths
	  sources_from_id
	  type_suffix
	  any_instance
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
		and type_id = ?
		"
	) unless $self->_preserve_sth( "instances.get_id()" );

	$p->{type_id} ||= '';
	$self->_preserve_sth( "instances.new()", "insert into instances (file_id,path_id,source_id,name_id,type_id) values (?,?,?,?,?)" ) unless $self->_preserve_sth( "instances.new()" );
	my $array_params = [ $p->{file_id}, $p->{path_id}, $p->{source_id}, $p->{name_id}, $p->{type_id} ];
	my $key = "$p->{file_id},$p->{path_id},$p->{source_id},$p->{name_id},$p->{type_id}";
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

sub _get_set_md5_for_file {
	my ( $self, $p ) = @_;

	$self->_preserve_sth( "file_md5s.get_id_from_md5()", "select id from file_md5s where md5 = ?" )           unless $self->_preserve_sth( "file_md5s.get_id_from_md5()" );
	$self->_preserve_sth( "file_md5s.new()",             "insert into file_md5s (md5,file_id) values (?,?)" ) unless $self->_preserve_sth( "file_md5s.new()" );

	return $self->_cache_or_db_or_new(
		{
			cache          => "file_md5s_md5_to_id",
			cache_key      => "file_md5s_id_from_md5.$p->{md5_digest}",
			get_sth_label  => "file_md5s.get_id_from_md5()",
			get_sth_params => [ $p->{md5_digest} ],
			set_sth_label  => "file_md5s.new()",
			set_sth_params => [ $p->{md5_digest}, $p->{file_id} ],
		}
	);

}

=head3 _file_name_from_instance_id
	
=cut

sub _file_name_from_instance_id {
	my ( $self, $id, $params ) = @_;
	Carp::croak( '$id required in _file_name_from_instance_id' ) unless $id;

	# TODO implement source criteria
	$self->_preserve_sth( "instance_name.from_names()", "select name from names join instances on instances.name_id = names.id where instances.id = ? " ) unless $self->_preserve_sth( "instance_name.from_names()" );
	$self->_preserve_sth( "instance_name.from_hash()",  "select sha1 from files join instances on instances.file_id = files.id where instances.id = ? " ) unless $self->_preserve_sth( "instance_name.from_hash()" );

	my $v = $self->_cache_or_db(
		{
			cache          => "instance_names",
			cache_key      => "instance_name.name.$id",
			cache_value    => 'name',
			get_sth_label  => "instance_name.from_names()",
			get_sth_params => [$id],

		}
	);
	return $v if $v;

	# Still TODO, put a value conversion facility in here
	$v = $self->_cache_or_db(
		{
			cache          => "instance_names",
			cache_key      => "instance_name.hash.$id",
			cache_value    => 'name',
			get_sth_label  => "instance_name.from_hash()",
			get_sth_params => [$id],
		}
	);

	return sprintf( '%x@', oct( "0b$v" ) ) if $v;

}

=head3 _path_from_instance_id
	
=cut

sub _path_from_instance_id {
	my ( $self, $id, $params ) = @_;

	# TODO implement source criteria
	Carp::croak( '$id required in _path_from_instance_id' ) unless $id;
	$self->_preserve_sth( "instance_path.from_id()", "select path from paths join instances on instances.path_id = paths.id where instances.id = ? " ) unless $self->_preserve_sth( "instance_path.from_id()" );

	my $v = $self->_cache_or_db(
		{
			cache          => "instance_paths",
			cache_key      => "instance_path.$id",
			cache_value    => 'path',
			get_sth_label  => "instance_path.from_id()",
			get_sth_params => [$id],
		}
	);
	return $v if $v;
}

=head3 _source_from_instance_id
	
=cut

sub _source_from_instance_id {
	my ( $self, $id, $params ) = @_;

	# TODO implement source criteria
	Carp::croak( '$id required in _source_from_instance_id' ) unless $id;
	$self->_preserve_sth( "source.from_id()", "select name from sources  join instances on instances.source_id = sources.id where instances.id = ? " ) unless $self->_preserve_sth( "source.from_id()" );

	my $v = $self->_cache_or_db(
		{
			cache          => "sources_from_id",
			cache_key      => "source.$id",
			cache_value    => 'name',
			get_sth_label  => "source.from_id()",
			get_sth_params => [$id],
		}
	);
	return $v if $v;

}

=head3 _suffix_from_instance_id
	
=cut

sub _suffix_from_instance_id {
	my ( $self, $id, $params ) = @_;
	Carp::croak( '$id required in _suffix_from_instance_id' ) unless $id;
	$self->_preserve_sth( "types.suffix.from_id()", "select suffix from types join instances on instances.type_id = types.id where instances.id = ? " ) unless $self->_preserve_sth( "types.suffix.from_id()" );

	my $v = $self->_cache_or_db(
		{
			cache          => "type_suffix",
			cache_key      => "suffix.$id",
			cache_value    => 'suffix',
			get_sth_label  => "types.suffix.from_id()",
			get_sth_params => [$id],
		}
	);

	return $v;
}

=head3 _get_any_instance_id
	Get some instance by some rule or other
	Atm it's literally 'the first for the file'
	Question mark over if this should be cached
=cut

sub _get_any_instance_id {
	my ( $self, $file_id, $p ) = @_;
	$self->_preserve_sth( "_get_any_instance_id()", sprintf( 'select id from %s where file_id = ?', 'instances' ) ) unless $self->_preserve_sth( "_get_any_instance_id()" );
	$self->{debug_level} = 2;
	return $self->_cache_or_db(
		{
			cache          => "any_instance",
			cache_key      => "any_instance.$file_id",
			cache_value    => 'id',
			get_sth_label  => "_get_any_instance_id()",
			get_sth_params => [$file_id],
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
