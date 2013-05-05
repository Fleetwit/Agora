var mysql				= require('mysql');
var http 				= require('http');
var fs 					= require('fs');
var mongodb 			= require('mongodb');
var qs 					= require('querystring');
var _ 					= require('underscore');

function datastore(options) {
	this.options = _.extend({
		host:		"127.0.0.1",
		port:		27017,
		database:	"fleetwit"
	},options);
	
	this.collections = {};
}
datastore.prototype.init = function(callback) {
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
datastore.prototype.open = function(collectionName, callback) {
	var scope 		= this;
	if (!this.collections[collectionName]) {
		this.collections[collectionName] = new mongodb.Collection(this.instance, collectionName);
	}
	callback(this.collections[collectionName]);
}
datastore.prototype.getUser = function(collectionName, uid, callback) {
	var scope 		= this;
	this.open(collectionName, function(collection) {
		collection.find({
			uid:	uid
		}, {
			limit:1
		}).toArray(function(err, docs) {
			//console.dir(docs);
			if (docs.length == 0) {
				// No userdata, we create it.
				collection.insert({
					uid:			uid,
					surveydata:		[],
					publicdata:		{},
					facebookdata:	{},
					twitterdata:	{}
				}, function(err, docs) {
					callback(collection, docs[0]);
				});
			} else {
				callback(collection, false);
			}
			
		});
	});
}

exports.datastore = datastore;