var mysql				= require('mysql');
var http 				= require('http');
var fs 					= require('fs');
var mongodb 			= require('mongodb');
var qs 					= require('querystring');
var _ 					= require('underscore');
var datastore 			= require('./datastore').datastore;
var url 				= require('url');
var ObjectID 			= require('mongodb').ObjectID;

var debug_mode			= true;



function main() {
	var scope 		= this;
	this.serverPort = 8018;
	this.raceData	= {};		// Data on all the races
	this.online		= {};
	this.interval	= {
		refresh:		5000,		// Refresh of race data
		update:			5000		// Refresh of online count
	};
	
	scope.log("Connecting to MongoDB...");
	this.mongo = new datastore({
		database:		"fleetwit"
	});
	this.mongo.init(function() {
		scope.log("MongoDB: Connected.");
		scope.log("Connecting to MySQL...");
		scope.mysql = mysql.createConnection({
			host     : 'localhost',
			user     : 'root',
			password : '',
			database : 'fleetwit'
		});
		scope.mysql.connect(function(err) {
			scope.log("MySQL: Connected.");
			
			scope.refreshData(function() {
				
				scope.updateCount(function() {
					scope.initServer();
					// Start the intervals
					setInterval(function() {
						scope.refreshData(function() {});
					}, scope.interval.refresh);
					
					setInterval(function() {
						scope.updateCount(function() {});
					}, scope.interval.update);
				});
			});
		});
	});
}
main.prototype.refreshData = function(callback) {
	var i;
	var j;
	var l;
	var scope = this;
	// get the races data
	scope.mysql.query("select * from races", function(err, rows, fields) {
		if (rows.length > 0 && rows[0].id > 0) {
			var l = rows.length;
			// save the races data
			for (i=0;i<l;i++) {
				scope.raceData[rows[i].id] = rows[i];
			}
			// Load the number of level per race
			scope.mysql.query("select count(g.id) as levels, r.id from races as r, races_games_assoc as g where r.id=g.rid group by r.id", function(err, rows, fields) {
				if (rows.length > 0 && rows[0].id > 0) {
					l = rows.length;
					// save the races data
					for (i=0;i<l;i++) {
						scope.raceData[rows[i].id].levels	= rows[i].levels;
					}
					callback();
				}
			});
		}
	});
};
main.prototype.updateCount = function(callback) {
	var i;
	var j;
	var l;
	var scope = this;
	
	var mapFn = function(){
		emit(
			"output",
			this.racedata
		);
	}
	var reduceFn = function(key, values) {
		
		var output = {};
		var i;
		var j;
		var l = values.length;
		var l2;
		for (i=0;i<l;i++) {
			if (values[i]) {
				l2 = values[i].length;
				for (j=0;j<l2;j++) {
					if (!output[values[i][j].race]) {
						output[values[i][j].race] = {};
					}
					if (!output[values[i][j].race][values[i][j].level]) {
						output[values[i][j].race][values[i][j].level] = 0;
					}
					output[values[i][j].race][values[i][j].level]++;
				}
			}
		}
		return output;
	};
	
	this.mongo.db.executeDbCommand({
		mapreduce: 	"datastore", 
		out:  		{ inline : 1 },
		map: 		mapFn.toString(),
		reduce: 	reduceFn.toString()
  }, function(err, dbres) {
  		if (dbres.documents[0].results.length > 0) {
  			var results = dbres.documents[0].results[0].value;
			scope.online = results;
  		}
  		console.log("ONLINE:: ",JSON.stringify(scope.online));
		callback();
  })
	
	
};
main.prototype.initServer = function() {
	var scope = this;
	
	http.createServer(function (req, server) {
		console.log("****************************************************************");
		
		if(req.method == 'POST') {
			var body='';
			req.on('data', function (data) {
				body += data;
			});
			req.on('end',function(){
				var POST =  qs.parse(body);
				scope.log("POST RAW:",POST);
				scope.parseRequest(POST, server);
			});
		} else {
			scope.log("URL:",req.url);
			url_parts = url.parse(req.url, true);
			scope.log("url_parts:",url_parts);
			scope.parseRequest(url_parts.query, server);
			//scope.output({message:"HTTP POST requests only."},server,true);
		}
		
	}).listen(this.serverPort);
	
	
}
main.prototype.parseRequest = function(data, server) {
	var scope = this;
	
	if (data.json && typeof(data.json) == "string") {
		data = JSON.parse(data.json);
	}
	
	if (data.params && typeof(data.params) == "string") {
		data.params = JSON.parse(data.params);
	}
	
	
	scope.log("POST PARSED:",data);
	
	// Make sure we have all the data
	if (!this.validateParameters(data, server)) {
		return false;
	}
	
	scope.execute(data, server);
}
main.prototype.execute = function(data, server) {
	var scope = this;
	
	scope.log("METHOD: ",data.method);
	
	switch (data.method) {
		default:
			scope.output({message:"Method '"+data.method+"' doesn't exist."},server,true,data.callback?data.callback:false);
		break;
		case "register":
			if (scope.requireParameters(data, server, ["race"])) {
				// Tell the user when the race is starting
				var timer = new Date(scope.raceData[data.params.race].start_time*1000).getTime()-new Date().getTime();
				scope.output({timer:timer},server,false,data.callback?data.callback:false);
			}
			// register the user to level 0
			// get the userdata
			this.mongo.open("datastore", function(collection) {
				collection.find({
					_id:	new ObjectID(data.id)
				}, {
					limit:1
				}).toArray(function(err, docs) {
					
					var scope 			= this;
					var user 			= docs[0];
					
					// Log the score and the data for this game
					collection.update(
						{
							uid:				user.uid
						},{
							$addToSet: {
								racedata: {
									data:	{score:0},
									race:	data.params.race*1,
									level:	0,
									type:	"data"
								}
							}
						},{
							upsert:true
						}, function(err, docs) {
							
						}
					);
					
				});
			});
		break;
		case "level":
			if (scope.requireParameters(data, server, ["race","level"])) {
				//@TODO
				if (!scope.online[data.params.race]) {
					scope.online[data.params.race] = {};
				}
				if (!scope.online[data.params.race][data.params.level-1]) {
					scope.online[data.params.race][data.params.level-1] = 0;
				}
				var online = scope.online[data.params.race][data.params.level-1];
				scope.output({online:online},server,false,data.callback?data.callback:false);
			}
		break;
		case "score":
			if (scope.requireParameters(data, server, ["race","level","data"])) {
				
				// get the userdata
				this.mongo.open("datastore", function(collection) {
					collection.find({
						_id:	new ObjectID(data.id)
					}, {
						limit:1
					}).toArray(function(err, docs) {
						
						var scope 			= this;
						var user 			= docs[0];
						var levelData		= data.params.data;
						var levelIndex		= data.params.level*1;
						
						if (!levelData.score) {
							levelData.score = 0;
						}
						
						
						// Log the score and the data for this game
						collection.update(
							{
								uid:				user.uid
							},{
								$addToSet: {
									racedata: {
										data:	levelData,
										race:	data.params.race*1,
										level:	levelIndex,
										type:	"data"
									}
								}
							},{
								upsert:true
							}, function(err, docs) {
								
							}
						);
						
						// update general score
						collection.update(
							{
								uid:			user.uid
							},{
								$inc: {
									score:	levelData.score
								}
							},{
								upsert:true
							}, function(err, docs) {
								
							}
						);
							
					});
				});
				
				
				scope.output({sent:true},server,false,data.callback?data.callback:false);
			}
		break;
	}
}
main.prototype.sendScore = function(server) {
	var scope = this;
	
}
main.prototype.validateParameters = function(data, server, required) {
	var scope = this;
	var i;
	var j;
	
	// Default required parameters
	if (!required) {
		var required = ["method","id","params"];
	}
	
	for (i=0;i<required.length;i++) {
		if (data[required[i]] == undefined || data[required[i]] == "") {
			scope.output({message:"Parameter '"+required[i]+"' is required. Parameters expected: "+required.join(", ")+".",data:data},server,true,data.callback?data.callback:false);
			return false;
		}
	}
	
	scope.info("typeof",typeof(data.params));
	
	if (typeof(data.params) == "string") {
		data.params = JSON.parse(data.params);
	}
	
	return true;
}
main.prototype.requireParameters = function(data, server, params) {
	var scope = this;
	var i;
	var j;
	
	
	for (i=0;i<params.length;i++) {
		if (data.params[params[i]] == undefined || data.params[params[i]] == "") {
			scope.output({message:"Parameter '"+params[i]+"' is required. Parameters expected: "+params.join(", ")+".",data:data},server,true,data.callback?data.callback:false);
			return false;
		}
	}
	
	return true;
}
main.prototype.output = function(data, server, error, jsonpCallback) {
	var scope 		= this;
	
	var output		= {};
	
	if (error) {
		output.error = true;
	}
	
	output = _.extend(data,output);
	
	if (jsonpCallback) {
		server.writeHead(200, {"Content-Type": "application/javascript"});
		server.write(jsonpCallback+"("+JSON.stringify(output)+");");
	} else {
		server.writeHead(200, {"Content-Type": "application/json"});
		server.write(JSON.stringify(output));
	}
	
	scope.log("OUTPUT",output);
	server.end();
	
	return true;
}
main.prototype.log = function(){
	var red, blue, reset;
	red   	= '\u001b[31m';
	blue  	= '\u001b[34m';
	green  	= '\u001b[32m';
	reset 	= '\u001b[0m';
	console.log(green+"<DATASTORE>");
	for (i in arguments) {
		console.log(reset, arguments[i],reset);
	}
};
main.prototype.info = function(){
	var red, blue, reset;
	red   	= '\u001b[31m';
	blue  	= '\u001b[34m';
	green  	= '\u001b[32m';
	reset 	= '\u001b[0m';
	console.log(green+"<DATASTORE>");
	for (i in arguments) {
		console.log(blue, arguments[i],reset);
	}
};

new main();



function QS(req) {
	var j;
	var raw 	= req.url;
	var parts	= raw.split("?");
	var url		= parts[parts.length-1];
	var obj		= qs.parse(url);
	for (j in obj) {
		if (!isNaN(obj[j]*1)) {
			obj[j]	= obj[j]*1;
		}
		if (obj[j] == 'true') {
			obj[j]	= true;
		}
		if (obj[j] == 'false') {
			obj[j]	= false;
		}
	}
	return obj;
}


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


