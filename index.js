/*
 *
 * 
 *
 */

'use strict';

var uuid = require('uuid/v1'),
  cp = require('child_process'),
  numCPUs = require('os').cpus().length,
  cpu_limit = 8

var queue = (function () {
 
  var module = {},
  	db, children = [], childState = [], rootDir = null, childStates = [], childJobs = []

  /**
  * Initiate the queue module, by providing an sqlite instance, afterwards a job table is initialised, which will hande the jobs
  *
  * @param {Object} `postgres` postgres db object
  */

  module.init = function ( postgres, dir, callback ) {
    rootDir = dir
  	db = postgres

    //Create job table if not already exists

    db.query("CREATE TABLE IF NOT EXISTS svift_queue (id SERIAL PRIMARY KEY, job_id text, status integer, added TIMESTAMP, start_time TIMESTAMP, end_time TIMESTAMP, params text)", function (err, result){
      if(err){
        //This creates an error if the svift_queue table is created for the first time, no worries about that...
        console.log(err)
      }

      //TODO: DELETE * FROM svift_queue
      db.query("UPDATE svift_queue SET status = 0 WHERE status = 1", function (err, result){  
        if(err){
          console.log(err)
        }

        console.log('CPUs:', ((numCPUs>cpu_limit)?cpu_limit:numCPUs))

        for(let i = 0; i<cpu_limit && i<numCPUs; i++){
          children.push(cp.fork(__dirname + '/child'))
          childState.push(-1)
          childStates.push({})
          childJobs.push(-1)
          children[i].send({func:'init', params:{id:i, dir:rootDir}})
          children[i].on('message', function(m) {
            module[m.func](m.params)
          })
        }

        //Give the webserver time to start up
        setTimeout(function(){
          console.log('begin processing')
          module.next()
        }, 10000)

        callback()
      })
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
    db.query("SELECT job_id, params FROM svift_queue WHERE status = 0 ORDER BY start_time ASC", function(err, result) {
      let rows = result.rows
      if(err){
        console.log(err)
      }else if ( rows && rows.length >= 1 ) {
        var ri = 0
        children.some( function (child, ci) {
          if(childState[ci] === 0){
            childState[ci] = 1
            childJobs[ci] = rows[ri].job_id
            //TODO: this object should be automatically generated through the available render methods??
            childStates[ci] = {svg:0,html:0,png:0,gif:0,mpeg:0}
            db.query("UPDATE svift_queue SET status = 1, start_time = NOW() WHERE job_id = $1", [rows[ri].job_id], function (err) {
              if (err) {
                console.log(err.message)
              }
            })
            child.send({func:'start',params:{id:rows[ri].job_id, params:JSON.parse(rows[ri].params)}})
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

   db.query("INSERT INTO svift_queue (job_id, status, added, params) VALUES ($1,$2, NOW() ,$3) RETURNING id", [uuid(), 0, JSON.stringify(job_params)], function (err, result) {
    if (err) {
      console.log(err.message)
    }

    let lastID = result.rows[0].id

    db.query("SELECT job_id FROM svift_queue WHERE id = $1", [lastID], function(err, result){
      if(err){
        console.log(err.message)
      }

      callback(result.rows[0].job_id)

      module.next()

    })
   })
  }

  module.jobUpdate = function (params) {
    module.updateStat(params.child_id, params.type, params.state)
  }

  module.jobDone = function (params) {
    db.query("UPDATE svift_queue SET status = 2, end_time = NOW() WHERE job_id = $1", [params.job_id], function (err) {
      if (err) {
        console.log(err.message)
      }

      childState[params.child_id] = 0

      module.next()
    })
  }

  module.jobStat = function (job_id, callback){
    db.query("SELECT status FROM svift_queue WHERE job_id = $1", [job_id], function(err, result){
      let rows = result.rows
      if(rows.length<1){
        callback('job_id not found', null)
      }else{
        var r = {status:rows[0].status}
        if(rows[0].status == 1){
          childJobs.forEach(function(j, ji){
            if(j == job_id){
              r['full'] = childStates[ji]
            }
          })
        }
        callback(null, r)
      }
    })
  }

  module.updateStat = function(child_id, type, state){
    if(!(type in childStates[child_id])){
      childStates[child_id][type] = 0
    }
    childStates[child_id][type] = state
  }

  module.stats = function ( callback ) {
    db.query("SELECT status, COUNT(*) FROM svift_queue GROUP BY status", function(err, result) {
      if (err) {
        console.log(err.message)
      }

      callback(result.rows)
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