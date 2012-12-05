octopi = {};

require(['octopi'], function(o) {
  _.extend(octopi, o);
  $('body').trigger("onready");
});
