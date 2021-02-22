create table if not exists "{{.Schema}}"."{{.Table}}"
(
	id serial
		constraint nginx_pk
			primary key,
	time_local varchar(256),
	path text,
	ip varchar(256),
	remote_user varchar(256),
	remote_port int,
    time timestamp,
	user_agent text,
	user_id_got text,
	user_id_set text,
	request text,
	status int,
	body_bytes_sent int,
	request_time float,
	request_method varchar(128),
	server_name varchar(256),
	geoip_country_code varchar(256),
	http_referrer text
);
