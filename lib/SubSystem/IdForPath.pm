package SubSystem::IdForPath;
use strict;
use 5.006;
use warnings;
use base qw(Class::Accessor);
__PACKAGE__->mk_accessors( qw/default_source_id / );
use Carp;
use Module::Runtime;
use Digest::SHA1;
use Sys::Hostname;
use File::Spec;
use File::Basename;

=head1 NAME
	SubSystem::IdForPath - Get integer path identifiers, somehow
=head1 VERSION
	Version 0.01
=cut

our $VERSION = '0.01';

=head1 SYNOPSIS
	For when 'protocol://host/path/to/file.ext' is better represented as '12'
	or
	when 'protocol://host' and '/path/to/file.ext' might be useful as '3' and '13', such as when we don't particularly care *which* 'protocol://host' is used 
	
=head1 EXPORT
	None
=head1 SUBROUTINES/METHODS
=head2 Facilitators
	Specific to this module
=head3 _init
	Separate class instantiation and configuration for when that's a good idea
=cut

sub _init {
	my ( $self, $conf ) = @_;
	return {pass => 1};
}

sub get_this_instance {
	die 'Not Yet Implemented';
}

=head3 get_any_instance
	return the full path including source when given file id and optionally some rules for the source selection
=cut

sub get_any_instance {
	die 'Not Yet Implemented';
}

sub demand_params {
	my ( $self, $href, $arref, $p ) = @_;
	$p ||= {};
	for ( @{$arref} ) {
		my $msg;
		unless ( $href->{$_} ) {
			my $callerbits = [ caller( 1 ) ];

			$msg = "Missing required parameter [$_] in $callerbits->[3]";
		}

		croak $msg if $msg;
	}
}

sub one_of {
	my ( $self, $href, $arref ) = @_;
	my $ok;
	for ( @{$arref} ) {
		if ( $href->{$_} ) {
			$ok = 1;
			last;
		}
	}
	return $ok;
}

sub digest_local_path {
	my ( $self, $file, $digests ) = @_;
	my @digest_objects;
	for my $digest ( @{$digests} ) {
		$digest = uc( $digest );

		Module::Runtime::require_module( "Digest::$digest" );
		push( @digest_objects, "Digest::$digest"->new() );
	}

	#mistake previously that closed the file handle after the first, meaning empty digest

	for my $digest_object ( @digest_objects ) {
		open( my $fh, '<', $file ) or return {fail => "Can't open [$file]: $!"};
		binmode( $fh );
		$digest_object->addfile( $fh );
		close( $fh );
	}
	return @digest_objects;
}

=head3 store_local_file
	Do The Necessary when given a file path
	Specifically,generates the keys and gets the ids for 
		always
			digest
			path
			type
			source
		conditionally
			name
	and returns the new instance id 
=cut

sub store_local_file {
	my ( $self, $file, $p ) = @_;
	$p ||= {};

	#set source id from various places
	my $source_id = $p->{source_id};
	$source_id ||= $self->default_source_id;
	$source_id ||= $self->get_set_id_for_source( {hostname => hostname()} );
	Carp::croak( "No source ID Available" ) unless $source_id;

	$file = File::Spec->rel2abs( $file );

	#split, get ids for parts
	my ( $name, $path, $suffix ) = File::Basename::fileparse( $file, qr/\.[^.]*/ );

	$suffix ||= '.none';

	#hash
	my @hashes = qw/sha1/;
	push( @hashes, 'md5' ) unless $p->{nomd5};
	my ( $sha1_obj, $md5_obj ) = $self->digest_local_path( $file, \@hashes );

	my $file_id = $self->get_set_id_for_file( {sha1_digest => $sha1_obj->digest} );

	#required parameters
	my $ip = {
		file_id   => $file_id,
		type_id   => $self->get_set_id_for_type( {suffix => $suffix} ),
		path_id   => $self->get_set_id_for_path( $path ),
		source_id => $source_id,

	};

	#optional parameters
	#In many cases, the lower case SHA1.suffix will be used as the file name
	unless ( $name eq $sha1_obj->hexdigest ) {
		unless ( $name ) {
			Carp::carp( "Name not detected for $file, exiting store_local_file" );
			return;

		}
		$ip->{name_id} = $self->get_set_id_for_name( $name );
	}

	if ( $md5_obj ) {
		$self->get_set_md5_for_file(
			{
				file_id    => $file_id,
				md5_digest => $md5_obj->digest
			}
		);
	}

	#create
	return $self->get_set_id_for_instance( $ip );

}

=head2 Wrappers
=head3 get_set_id_for_file
	From the file's hash, create and/or return its id, and create any associated records
=cut

sub get_set_id_for_file {
	my ( $self, $p ) = @_;
	$self->demand_params( $p, [qw/ sha1_digest /] );
	return $self->_get_set_id_for_file( $p );
}

=head3 get_set_id_for_instance

=cut

sub get_set_id_for_instance {
	my ( $self, $p ) = @_;
	$self->demand_params( $p, [qw/ file_id path_id type_id source_id /] );
	return $self->_get_set_id_for_instance( $p );
}

=head3 get_set_id_for_name
	From a filename w/o extension, create and/or return the id
=cut

sub get_set_id_for_name {
	my ( $self, $name ) = @_;
	Carp::croak( "get_set_id_for_name called without a name" ) unless $name;
	return $self->_get_set_id_for_name( $name );
}

=head3 get_set_id_for_path
	From a directory path, create and/or return the id
=cut

sub get_set_id_for_path {
	my ( $self, $path ) = @_;
	Carp::croak( "get_set_id_for_path called without a path" ) unless $path;

	#more a check to ensure we're storing a path instead of a file, the trailing slash is removed in storage since it should always be present
	Carp::croak( "Missing trailing slash from [$path]" ) unless ( substr( $path, -1 ) eq '/' );
	$path = File::Spec->rel2abs( $path );
	return $self->_get_set_id_for_path( $path );
}

=head3 get_set_id_for_type
	From an extension or mime type, create and/or return the id
=cut

sub get_set_id_for_type {
	my ( $self, $p ) = @_;

	Carp::croak( "get_set_id_for_type called without a suffix or mime type" ) unless $self->one_of( $p, [qw/ suffix mime /] );
	for ( qw/suffix mime / ) {
		$p->{$_} = lc( $p->{$_} ) if $p->{$_};
	}

	return $self->_get_set_id_for_type( $p );
}

=head3 get_set_id_for_source
	From a hostname or service, get the source id
=cut

sub get_set_id_for_source {
	my ( $self, $p ) = @_;
	Carp::croak( "get_set_id_for_source called without hostname" ) unless $self->one_of( $p, [qw/ hostname /] );
	return $self->_get_set_id_for_source( $p );
}

sub get_set_md5_for_file {
	my ( $self, $p ) = @_;
	$self->demand_params( $p, [qw/ file_id md5_digest /] );

	return $self->_get_set_md5_for_file( $p );
}

sub path_elements_from_instance_id {
	my ( $self, $id ) = @_;
	return {

		file_name => $self->_file_name_from_instance_id( $id ),
		path      => $self->_path_from_instance_id( $id ),
		source    => $self->_source_from_instance_id( $id )
	};

}

=head2 Place holders
	Should all be replaced in child classes 
=cut

=head3 _get_set_id_for_file
	
=cut

sub _get_set_id_for_file {
	die( 'not implemented' );
}

=head3 _get_set_id_for_instance
	
=cut

sub _get_set_id_for_instance {
	die( 'not implemented' );
}

=head3 _get_set_id_for_name
	
=cut

sub _get_set_id_for_name {
	die( 'not implemented' );
}

=head3 _get_set_id_for_path
	
=cut

sub _get_set_id_for_path {
	die( 'not implemented' );
}

=head3 _get_set_id_for_type
	
=cut

sub _get_set_id_for_type {
	die( 'not implemented' );
}

=head3 _get_set_id_for_source
	
=cut

sub _get_set_id_for_source {
	die( 'not implemented' );
}

=head3 _get_set_md5_for_file
	
=cut

sub _get_set_md5_for_file {
	die( 'not implemented' );
}

=head3 _file_name_from_instance_id
	
=cut

sub _file_name_from_instance_id {
	die( 'not implemented' );
}

=head3 _path_from_instance_id
	
=cut

sub _path_from_instance_id {
	die( 'not implemented' );
}

=head3 _source_from_instance_id
	
=cut

sub _source_from_instance_id {
	die( 'not implemented' );
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
