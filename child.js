/*
 *
 * 
 *
 */

'use strict';

var render = require('svift-render')

var queue_child = (function () {
 
  var module = {},
    id = null,
    job_id = null,
    rootDir = null
  
  module.process = process

  process.on('message', function(m) {
    if(m.func in module){
      module[m.func](m.params)  
    }else{
      console.log(m.func + 'not found')
    }
  });

  module.init = function (obj) {
    id = obj.id
    rootDir = obj.dir

    render.init(rootDir, function(){
      module.process.send({func:'initDone', params:{
        child_id: id
      }})
    }, module.update)
  }

  module.start = function (params) {
    job_id = params.id
    render.render(params, module.done);
  }

  module.done = function () {
    module.process.send({func:'jobDone', params:{
      job_id :job_id,
      child_id:id
    }})
  }

  module.update = function (type, state) {
    module.process.send({func:'jobUpdate', params:{
      job_id :job_id,
      child_id:id,
      type:type,
      state:state
    }})
  }

  return module;
 
})();

module.exports = queue_child;