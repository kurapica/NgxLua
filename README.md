# The implementation of the PLoop.System.Web based on the Openresty

This lib provide the implementation of the [PLoop][].System.Web framework for [Openresty][].

It provide the implementation of:

* NgxLua.HttpRequest  - Fetch data from the ngx.req as the http request object
* NgxLua.HttpResponse - Send data to ngx as the http response object
* NgxLua.HttpContext  - Use the NgxLua.HttpRequest as the request and the NgxLua.HttpResponse as the response

It also provide plenty features:

* Data
	* ShareDict       - The cache based on the Openresty's shared table
	* Redis           - The cache based on the redis
	* MySQLProvider   - MySQL providers that could be used by the data entity system
* Thread
	* LockManager     - The lock manager for threads based on the *resty.lock* lib
* Http
	* HttpFiles       - Handle the upload files, wrap them to HttpFile objects for simple using
	* ShareSessionStorageProvider -- The session storage provider based on the share dict
	* RedisSessionStorageProvider -- The session storage provider based on the redis


## Embed the PLoop into Openresty

Take the [PLoop.Browser][] as example, we'll see how to use the [PLoop][] and **NgxLua** in an Openresty server.

[PLoop]: 		 https://github.com/kurapica/PLoop  "PLoop"
[PLoop.Browser]: https://github.com/kurapica/PLoop.Browser  "PLoop Lib Browser"
[nginx]: 		 https://www.nginx.com/ "Nginx"
[Openresty]: 	 https://github.com/openresty/lua-nginx-module/ "Openresty"