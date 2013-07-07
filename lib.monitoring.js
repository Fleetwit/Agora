var http 				= require('http');
var uuid 				= require('./lib.uuid');
var _ 					= require('underscore');
var qs 					= require('querystring');
var fs 					= require('fs');


Date.prototype.time = function() {
	return this.getDate()+"/"+(this.getMonth()+1)+"-"+this.getHours()+":"+this.getMinutes()+":"+this.getSeconds();
}

function monitor(options) {
	var scope = this;
	
	this.options 	= _.extend({
		name:		"not set"
	},options);
	
	this.table		= "monitoring";
	
}
monitor.prototype.updateTime = function(data){
	var d			= new Date();
	this.periods = {
		"seconds":	new Date(d.getFullYear(),d.getMonth(),d.getDate(),d.getHours(),d.getMinutes(),d.getSeconds()).getTime(),
		"minutes":	new Date(d.getFullYear(),d.getMonth(),d.getDate(),d.getHours(),d.getMinutes()).getTime(),
		"hours":	new Date(d.getFullYear(),d.getMonth(),d.getDate(),d.getHours()).getTime(),
		"days":		new Date(d.getFullYear(),d.getMonth(),d.getDate()).getTime()
	};
	
};
monitor.prototype.process = function(data){
	this.updateTime();
	var k;
	var updateObj = {
		'$inc':	 {}
	};
	for (k in data) {
		updateObj['$inc']['data.'+k+'.sum'] = data[k];
		updateObj['$inc']['data.'+k+'.n'] 	= 1;
	}
	
	var p;
	for (p in this.periods) {
		global.mongo.update(this.table, {
			period:	p,
			time:	this.periods[p],
			type:	"process",
			name:	this.options.name,
		},updateObj,
		function() {
			// done
		});
	}
};
monitor.prototype.set = function(name, value){
	global.mongo.update(this.table, {
		type:	"set",
		name:	name
	},{
		value:	value,
		time:	new Date().getTime()
	},
	function() {
		// done
	});
};
monitor.prototype.push = function(name, value, filters){
	this.updateTime();
	var k;
	filters = _.extend({},filters);
	var updateObj = {
		'$inc':	 {}
	};
	updateObj['$inc']['sum'] 	= value;
	updateObj['$inc']['count'] 	= 1;
	updateObj = _.extend(updateObj,filters);
	
	var p;
	for (p in this.periods) {
		global.mongo.update(this.table, _.extend({
			period:	p,
			time:	this.periods[p],
			type:	"push",
			name:	name,
		},filters),updateObj,
		function() {
			// done
		});
	}
};
monitor.prototype.log = function(name, value){
	global.mongo.insert(this.table, {
		type:	"log",
		name:	name,
		time:	new Date().getTime(),
		value:	value
	},
	function() {
		// done
	});
};
monitor.prototype.getGitPush = function(dir, callback){
	fs.readFile(dir+"/.git/FETCH_HEAD", 'utf8', function (err, data) {
		if (err) {
			callback(false);
		} else {
			// split
			var parts = data.split("\t");
			callback(parts[0]);
		}
	});
};

exports.monitor = monitor;