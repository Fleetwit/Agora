var _ 					= require('underscore');
var mongodb 			= require('mongodb');

function mongo(options) {
	this.options = _.extend({
		host:		"127.0.0.1",
		port:		27017,
		database:	"fleetwit"
	},options);
	
	
	this.collections = {};
}
mongo.prototype.init = function(callback) {
	var scope 		= this;
	
	this.server 	= new mongodb.Server(this.options.host, this.options.port, {});
	this.db			= new mongodb.Db(this.options.database, this.server, {w:1});
	this.db.open(function (error, client) {
		if (error) {
			throw error;
		}
		scope.instance = client;
		callback();
	});
}
mongo.prototype.open = function(collectionName, callback) {
	var scope 		= this;
	if (!this.collections[collectionName]) {
		this.collections[collectionName] = new mongodb.Collection(this.instance, collectionName);
	}
	callback(this.collections[collectionName]);
}
mongo.prototype.find = function(collectionName, query, callback, fields, options) {
	var scope 		= this;
	
	query			= _.extend({},query);
	fields			= _.extend({},fields);
	options			= _.extend({},options);
	
	this.open(collectionName, function(collection) {
		collection.find(query,fields,options).toArray(function(err, docs) {
			callback(docs, err);
		});
	});
}
mongo.prototype.insert = function(collectionName, data, callback, options) {
	var scope 		= this;
	
	data			= _.extend({},data);
	options			= _.extend({},options);
	
	this.open(collectionName, function(collection) {
		collection.insert(data,options, callback);
	});
}
mongo.prototype.update = function(collectionName, query, data, callback, options) {
	var scope 		= this;
	
	query			= _.extend({},query);
	data			= _.extend({},data);
	options			= _.extend({upsert:true},options);
	
	this.open(collectionName, function(collection) {
		collection.update(query,data,options, callback);
	});
}

exports.main = mongo;