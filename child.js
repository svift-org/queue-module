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
    job_id = null

  process.on('message', function(m) {
    module[m.func](m.params)
  });

  module.init = function (pid) {
    id = pid

    //init render-module

    process.send({func:'initDone', params:{
      child_id: id
    }})
  }

  module.start = function (params) {
    job_id = params.id
    setTimeout(module.done, 3000)
  }

  module.done = function () {
    process.send({func:'jobDone', params:{
      job_id :job_id,
      child_id:id
    }})
  }

  return module;
 
})();

module.exports = queue_child;