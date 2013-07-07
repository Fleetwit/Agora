var _cluster		= require('cluster');
var _os				= require('os');
var _ 				= require('underscore');
var monitor 		= require('./lib.monitoring').monitor;
var mongo 			= require('./mongo').main;

global.monitor 		= new monitor({name:"Agora"});
global.mongo		= new mongo({database:"stats"});

var options = _.extend({
	timeout:	5000,		// if the process doesn't respond after this time, it is killed,
	online:		true,
	threads:	64
},processArgs());

console.log("Options",options);

var main;
if (options.online) {
	main			= '/home/gitbuffer/Agora/init.js';
} else {
	main			= './init.js';
}

var i;
var workers			= {};
options.thread			= Math.min(options.threads, _os.cpus().length);
_cluster.setupMaster({
    exec:	main
});


global.mongo.init(function() {
	for (var i = 0; i < options.thread; i++) {
	    createWorker();
	}
});

function processArgs() {
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

if (_cluster.isMaster) {
	if (options.timeout > 0) {
		setInterval(function() {
			var time = new Date().getTime();
			for (pid in workers) {
				if (workers[pid] && workers[pid].lastCheck + 5000 < time) {
					console.log("TIMEOUT: ", pid);
					workers[pid].worker.process.kill();
					delete workers[pid];
					createWorker();
				}
			}
		}, options.timeout);
	}
}


var processHistory 	= {};
var processTime 	= {};
function createWorker() {
	var worker 	= _cluster.fork();
	console.log("New worker: ",worker.process.pid);
	workers[worker.process.pid] = {
		worker:		worker,
		lastCheck:	new Date().getTime()-1000	// allow boot time
	};
	worker.on('message', function(data) {
		// register the time
		
		var curTime = new Date().getTime();
		if (processHistory[worker.process.pid]) {
			processTime[worker.process.pid] = curTime-processHistory[worker.process.pid];
			global.monitor.process(processTime);
		}
		processHistory[worker.process.pid] = curTime;
		if (workers[worker.process.pid] && workers[worker.process.pid].lastCheck) {
			workers[worker.process.pid].lastCheck = curTime;
		}
		
	});
	
};