var queue = require('../index'),
  sqlite3 = require('sqlite3').verbose()

var db = new sqlite3.Database(':memory:')

let ids = [], done = [], intervals = []

queue.init(db, function () {
  for(let i = 0; i<10; i++){
    queue.addJob({some:'data'}, function(id){
      ids.push(id)
      console.log('job added',id)
      intervals.push(setInterval(function(){
        queue.jobStat(id, function(err, stat){
          console.log(id, stat)
          if(stat === 2 && done.indexOf(id) < 0){
            done.push(id)
            if(done.length === ids.length){
              queue.stats(function(stats){
                console.log('last stats', stats)
                console.log('test successful')
                queue.exit()
                process.exit()
              })          
            }
          }
        })
      }, 1000))
    })
  }

  let interval = setInterval(function(){
    queue.stats(function(stats){
      console.log('stats', stats)
    })
  }, 1000)
})