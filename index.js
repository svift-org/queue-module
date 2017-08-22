/*
 *
 * 
 *
 */

'use strict';

var uuid = require('uuid/v1'),
  cp = require('child_process'),
  numCPUs = require('os').cpus().length,
  cp_limit = 2

var queue = (function () {
 
  var module = {},
  	db, children = [], childState = []

  /**
  * Initiate the queue module, by providing an sqlite instance, afterwards a job table is initialised, which will hande the jobs
  *
  * @param {Object} `mysqlite` sqlite3 db object
  */

  module.init = function ( mysqlite, callback ) {
  	db = mysqlite

    //Create job table if not already exists
    db.run("CREATE TABLE IF NOT EXISTS svift_queue (id INTEGER UNIQUE PRIMARY KEY AUTOINCREMENT, job_id text, status integer, added DATETIME, start DATETIME, end DATETIME, params text)", function (err, result){
      if(err){
        //This creates an error if the svift_queue table is created for the first time, no worries about that...
        console.log(err)
      }

      for(let i = 0; i<cp_limit && i<numCPUs; i++){
        children.push(cp.fork(__dirname + '/child'))
        childState.push(-1)
        children[i].send({func:'init', params:i})
        children[i].on('message', function(m) {
          module[m.func](m.params)
        })
      }

      callback()
    })
  }

  module.initDone = function ( params ) {
    childState[params.child_id] = 0
    module.next()
  }

  /**
  * Check in the queue table if there is a job todo, then run it, otherwise wait for jobs
  */

  module.next = function () {
    db.all("SELECT job_id, params FROM svift_queue WHERE status = 0 ORDER BY start ASC", function(err, rows) {
      if(err){
        console.log(err)
      }else if ( rows && rows.length >= 1 ) {
        var ri = 0
        children.some( function (child, ci) {
          if(childState[ci] === 0){
            childState[ci] = 1
            db.run("UPDATE svift_queue SET status = 1, start = strftime('%Y-%m-%d %H:%M:%S', 'now') WHERE job_id = ?", [rows[ri].job_id], function (err) {
              if (err) {
                console.log(err.message)
              }
            })
            console.log(rows[ri].params)
            child.send({func:'start',params:{id:rows[ri].job_id, params:rows[ri].params}})
            ri++
            if(ri >= rows.length){
              return true
            }
          }
        })
      }
    })
  }

  /**
  * Check in the queue table if there is a job todo, then run it, otherwise wait for jobs
  */

  module.addJob = function (job_params, callback) {

   db.run("INSERT INTO svift_queue (job_id, status, added, params) VALUES (?,?, strftime('%Y-%m-%d %H:%M:%S', 'now') ,?)", [uuid(), 0, JSON.stringify(job_params)], function (err) {
    if (err) {
      console.log(err.message)
    }

    let lastID = this.lastID

    db.all("SELECT job_id FROM svift_queue WHERE id = ?", [lastID], function(err, rows){
      if(err){
        console.log(err.message)
      }

      callback(rows[0].job_id)

      module.next()

    })
   })
  }

  module.jobDone = function (params) {
    db.run("UPDATE svift_queue SET status = 2, end = strftime('%Y-%m-%d %H:%M:%S', 'now') WHERE job_id = ?", [params.job_id], function (err) {
      if (err) {
        console.log(err.message)
      }

      childState[params.child_id] = 0

      module.next()
    })
  }

  module.jobStat = function (job_id, callback){
    db.all("SELECT status FROM svift_queue WHERE job_id = ?", [job_id], function(err, rows){
      if(rows.length<1){
        callback('job_id not found', null)
      }else{
        callback(null, rows[0].status)
      }
    })
  }

  module.stats = function ( callback ) {
    db.all("SELECT status, COUNT(*) FROM svift_queue GROUP BY status", function(err, rows) {
      if (err) {
        console.log(err.message)
      }

      callback(rows)
    })
  }

  module.exit = function () {
    children.forEach(function(child){
      child.kill('SIGINT')
    })
  }

  return module;
 
})();

module.exports = queue;