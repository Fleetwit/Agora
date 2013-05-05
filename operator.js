var _cluster		= require('cluster');
var _os				= require('os');
var _ 				= require('underscore');
var _qs 			= require('querystring');
var _http 			= require('./lib.httpserver').httpserver;
var _server 		= require('./lib.simpleserver').simpleserver;
var _logger 		= require('./lib.logger').logger;
var _datastore 		= require('./lib.datastore').datastore;
var _redis 			= require("redis");
var _mysql			= require('mysql');
var uuid 				= require('./lib.uuid');

var debug_mode		= true;

function operator() {
	this.port	= 8020;
	this.wsport	= 8022;
	this.logger = new _logger({label:'Operator:'+process.pid});
	var scope = this;
}

operator.prototype.init = function() {
	var scope = this;
	scope.redis = _redis.createClient();
	scope.redis.select(0, function() {
		scope.mongo = new _datastore({
			database:		"fleetwit"
		});
		scope.mongo.init(function() {
			scope.mysql = _mysql.createConnection({
				host     : 'localhost',
				user     : 'root',
				password : '',
				database : 'fleetwit'
			});
			scope.mysql.connect(function(err) {
				scope.serverInit();
			});
		});
	});
}
operator.prototype.serverInit = function() {
	var scope = this;
	this.logger.error("HTTP Server Starting");
	this.server	= new _http({
		port:		this.port,
		mysql:		this.mysql,
		onRequest:	function(params, instance, server) {
			//instance.output(server, params, true, true);
			if (instance.requireParameters(server, params, ["authtoken","rid"])) {
				instance.validateAuthToken(server, params, function(success, data) {
					scope.logger.info(success, data);
					if (success) {
						/*
						@TODO:
						CylonInstance = min(usercount) = {
							host,
							port
						}
						*/
						// Get the raceToken
						scope.getRaceToken(data.id, params.rid, function(raceToken) {
							scope.logger.log("raceToken: ",raceToken);
							instance.output(server, {
								raceToken:	raceToken,
								cylon:		{
									host:	'127.0.0.1',
									port:	8080
								}
							}, true);
						});
						
					}
					// fail is already handled by the httpserver
				});
			}
		}
	});
	this.logger.error("Socket Server Starting");
	this.wsserver = new _server(this.wsport, {
		logger:		this.logger,
		onConnect:	function(client) {
			//scope.logger.log("client",client);
		},
		onReceive:	function(client, data) {
			if (data.authToken) {
				scope.mysql.query("select u.id,u.email,u.firstname,u.lastname,t.token,t.validity from authtokens as t, users as u where u.id=t.uid and t.token='"+data.authToken+"' and t.validity > "+(new Date().getTime()/1000), function(err, rows, fields) {
					if (rows.length > 0 && rows[0].id > 0) {
						scope.logger.info(
							"Auth Token validated: ",
							"UID: \t\t"+rows[0].id,
							"Token: \t"+rows[0].token,
							"Validity: \t"+new Date(rows[0].validity*1000).toISOString(),
							"User: \t\t"+rows[0].firstname+" "+rows[0].lastname
						);
						scope.getRaceToken(rows[0].id, data.rid, function(raceToken) {
							scope.logger.log("raceToken: ",raceToken);
							
							var response = {
								raceToken:	raceToken,
								cylon:		{
									host:	'127.0.0.1',
									port:	8080
								}
							};
							if (data.ask_id) {
								response.response_id = data.ask_id;
							}
							scope.wsserver.send(client.uid, response);
						});
					} else {
						
						var response = {invalidtoken: true};
						if (data.ask_id) {
							response.response_id = data.ask_id;
						}
						scope.wsserver.send(client.uid, response);
					}
				});
			}
			
		},
		onQuit:	function(client) {
			
		}
	});
}
operator.prototype.getRaceToken = function(uid, rid, callback) {
	var scope = this;
	// Check if the user already has a raceToken
	this.mysql.query("select * from races_scores where uid="+uid+" and rid="+rid, function(err, rows, fields) {
		if (rows.length > 0 && rows[0].id > 0) {
			// We have a raceToken, we return it
			callback(rows[0].racetoken);
		} else {
			// no raceToken, we need to create it
			var raceToken = uuid.v4();
			// save the raceToken
			scope.mysql.query("insert into races_scores (rid,uid,racetoken) values("+rid+","+uid+",'"+raceToken+"')", function(err, rows, fields) {
				callback(raceToken);
			});
		}
	});
}

var instance = new operator();
instance.init();

/************************************/
/************************************/
/************************************/
// Process Monitoring
setInterval(function() {
	process.send({
		memory:		process.memoryUsage(),
		process:	process.pid
	});
}, 1000);

// Crash Management
if (!debug_mode) {
	process.on('uncaughtException', function(err) {
		console.log("uncaughtException",err);
	});
}


