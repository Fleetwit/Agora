var mysql				= require('mysql');
var http 				= require('http');
var fs 					= require('fs');
var mongodb 			= require('mongodb');
var qs 					= require('querystring');
var _ 					= require('underscore');
var datastore 			= require('./datastore').datastore;
var url 				= require('url');
var ObjectID 			= require('mongodb').ObjectID;
var monitor 			= require('./lib.monitoring').monitor;
var mongo 				= require('./mongo').main;

var debug_mode			= false;

global.monitor 		= new monitor({name:"Agora"});
global.mongo		= new mongo({database:"stats"});

function main() {
	var scope 		= this;
	this.serverPort = 8018;
	this.raceData	= {};		// Data on all the races
	this.online		= {};
	this.interval	= {
		refresh:		5000,		// Refresh of race data
		update:			5000		// Refresh of online count
	};
	
	this.options 	= this.processArgs();
	console.log("Options:\n",this.options);
	
	global.monitor.set("Agora.settings", this.options);
	
	console.log("Connecting to MongoDB...");
	this.mongo = new datastore({
		database:		"fleetwit"
	});
	this.mongo.init(function() {
		console.log("MongoDB: Connected.");
		console.log("Connecting to MySQL...");
		if (scope.options.mysql && scope.options.mysql == "online") {
			console.log("Switching to online settings...");
			scope.mysql = mysql.createConnection({
				host     : 'localhost',
				user     : 'fleetwit_beta',
				password : '!80803666',
				database : 'fleetwit_beta'
			});
		} else {
			console.log("Switching to offline settings...");
			scope.mysql = mysql.createConnection({
				host     : 'localhost',
				user     : 'root',
				password : '',
				database : 'fleetwit'
			});
		}
		scope.mysql.connect(function(err) {
			console.log("MySQL: Connected.");
			
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
main.prototype.processArgs = function() {
	var i;
	var args 	= process.argv.slice(2);
	var output 	= {};
	for (i=0;i<args.length;i++) {
		var l1	= args[i].substr(0,1);
		if (l1 == "-") {
			if (args[i+1] == "true") {
				args[i+1] = true;
			}
			if (args[i+1] == "false") {
				args[i+1] = false;
			}
			if (!isNaN(args[i+1]*1)) {
				args[i+1] = args[i+1]*1;
			}
			output[args[i].substr(1)] = args[i+1];
			i++;
		}
	}
	return output;
};

// Get the list of clients and races
main.prototype.refreshData = function(callback) {
	var i;
	var j;
	var l;
	var scope = this;
	// get the races data
	this.mongo.open("clients", function(collection) {
		collection.find({
			
		}, {
			limit:1
		}).toArray(function(err, docs) {
			var clients 		= docs[0].data.clients;
			var races			= {};
			
			var i;
			var j;
			for (i=0;i<clients.length;i++) {
				//console.log("clients[i]",clients[i].races.length);
				for (j=0;j<clients[i].races.length;j++) {
					races[clients[i].races[j].uuid] = clients[i].races[j];
					races[clients[i].races[j].uuid].client = clients[i];
				}
			}
			for (i in races) {
				delete races[i].client.races;
			}
			scope.raceData = races;
			callback();
		});
	});
};

// get the number of users online
main.prototype.updateCount = function(callback) {
	var scope = this;
	
	this.mongo.open("online", function(collection) {
		collection.find({
			type: 'level'
		}).toArray(function(err, docs) {
			
			if (docs.length == 0) {
				scope.online = {};
			} else {
				scope.online = docs[0].data;
			}
			callback();
		});
	});
	
};
/*main.prototype.updateCount = function(callback) {
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
  		if (dbres.documents[0].results && dbres.documents[0].results.length > 0) {
  			var results = dbres.documents[0].results[0].value;
			scope.online = results;
  		}
  		//console.log("ONLINE:: ",JSON.stringify(scope.online));
		callback();
  })
	
	
};*/
main.prototype.initServer = function() {
	var scope = this;
	console.log("Starting HTTP server on port "+this.serverPort+"...");
	http.createServer(function (req, server) {
		console.log("****************************************************************");
		
		if(req.method == 'POST') {
			var body='';
			req.on('data', function (data) {
				body += data;
			});
			req.on('end',function(){
				var POST =  qs.parse(body);
				scope.parseRequest(POST, server);
			});
		} else {
			url_parts = url.parse(req.url, true);
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
	
	// Make sure we have all the data
	if (!this.validateParameters(data, server)) {
		return false;
	}
	
	scope.execute(data, server);
}
main.prototype.execute = function(data, server) {
	var scope = this;
	
	console.log("exec: ",data.method);
	
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
			
			// Log the connection
			global.monitor.push("Agora.register", 1, {race:data.params.race});
			
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
									race:	data.params.race,
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
				
				var online = scope.getOnlineCount(data.params.race, data.params.level);
				
				global.monitor.push("Agora.online", 1, {race:data.params.race,level:data.params.level});
				
				// register the number of users online
				this.mongo.open("online", function(collection) {
					var updateObj = {
						'$inc':	 {}
					};
					
					updateObj['$inc']["data."+data.params.race+'.'+data.params.level+''] = 1;
					
					collection.update(
						{
							type: 'level'
						},updateObj,{
							upsert:true
						}, function(err, docs) {
							
						}
					);
				});
				
				scope.output({online:online},server,false,data.callback?data.callback:false);
			} else {
				console.log("error :(");
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
						
						// Check if the level was already submitted
						if (user.scores && user.scores[data.params.race] && user.scores[data.params.race][levelIndex]) {
							// user already played this level
							// We flag the user
							collection.update(
								{
									uid:				user.uid
								},{
									$addToSet: {
										flags: {
											data:	{
												data:	levelData,
												race:	data.params.race,
												level:	levelIndex,
											},
											time:		new Date().getTime(),
											type:		"race",
											processed:	false
										}
									}
								},{
									upsert:true
								}, function(err, docs) {
									
								}
							);
							
							global.monitor.push("Agora.duplicate", 1, {race:data.params.race,level:levelIndex+1});
							
							console.log("/!\\ LEVEL SUBMITTED TWICE. USER FLAGGED.");
							
							return false;
						}
						
						
						// Log the score and the data for this game (to track the source and data)
						collection.update(
							{
								uid:				user.uid
							},{
								$addToSet: {
									racedata: {
										data:	levelData,
										race:	data.params.race,
										level:	levelIndex,
										type:	"data",
										time:	new Date().getTime()
									}
								}
							},{
								upsert:true
							}, function(err, docs) {
								
							}
						);
						
						
						// update general score (Total score accross all races)
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
						
						// update game score (total score just for that race + score for that level)
						// prepare the increment data
						var incdata = {};
						incdata["scores."+data.params.race+".total"] 		= levelData.score;
						incdata["scores."+data.params.race+"."+levelIndex] 	= levelData.score;
						
						collection.update(
							{
								uid:			user.uid
							},{
								$inc: incdata
							},{
								upsert:true
							}, function(err, docs) {
								
							}
						);
						
						// log the average score
						global.monitor.push("Agora.score", levelData.score, {race:data.params.race,level:levelIndex+1});
					});
				});
				
				
				scope.output({sent:true},server,false,data.callback?data.callback:false);
			}
		break;
	}
}
main.prototype.getOnlineCount = function(race, level) {
	if (!this.online[race]) {
		return 0;
	}
	if (!this.online[race][level]) {
		return 0;
	}
	return this.online[race][level];
	
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
	
	console.log("OUTPUT",output);
	server.end();
	
	return true;
}
main.prototype.log = function(){
	var red, blue, reset;
	red   	= '\u001b[31m';
	blue  	= '\u001b[34m';
	green  	= '\u001b[32m';
	reset 	= '\u001b[0m';
	console.log(green+"<AGORA>");
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
	console.log(green+"<AGORA>");
	for (i in arguments) {
		console.log(red, arguments[i],reset);
	}
};

global.mongo.init(function() {
	new main();
});


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
		global.monitor.log("Agora.error", err.stack);
	});
}
