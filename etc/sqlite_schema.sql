CREATE TABLE files ( id INTEGER PRIMARY KEY , sha1 BLOB );
CREATE TABLE names ( id INTEGER PRIMARY KEY , name TEXT );
CREATE TABLE types ( id INTEGER PRIMARY KEY , name TEXT, mime TEXT, suffix TEXT );
CREATE TABLE paths ( id INTEGER PRIMARY KEY , path TEXT );
CREATE TABLE sources ( id INTEGER PRIMARY KEY , name TEXT, type TEXT, protocol TEXT );
CREATE TABLE instances ( id INTEGER PRIMARY KEY , name_id INTEGER, type_id INTEGER, file_id INTEGER, path_id INTEGER, source_id INTEGER );
CREATE TABLE file_md5s ( id INTEGER PRIMARY KEY , file_id INTEGER, md5 BLOB );
