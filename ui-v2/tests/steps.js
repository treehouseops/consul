import models from './steps/doubles/model';
import http from './steps/doubles/http';
import visit from './steps/interactions/visit';
import click from './steps/interactions/click';
import form from './steps/interactions/form';
import debug from './steps/debug/index';
import assertHttp from './steps/assertions/http';
import assertModel from './steps/assertions/model';
import assertPage from './steps/assertions/page';
import assertDom from './steps/assertions/dom';
import assertForm from './steps/assertions/form';

// const dont = `( don't| shouldn't| can't)?`;

export default function(assert, library, pages, utils) {
  var currentPage;
  const getCurrentPage = function() {
    return currentPage;
  };
  const setCurrentPage = function(page) {
    currentPage = page;
    return page;
  };

  const pauseUntil = function(run, message = 'assertion timed out') {
    return new Promise(function(r, reject) {
      let count = 0;
      let resolved = false;
      const resolve = function() {
        resolved = true;
        r();
      };
      (function tick() {
        run(resolve, reject).then(function() {
          if (!resolved) {
            setTimeout(function() {
              if (++count >= 50) {
                assert.ok(false, message);
                reject();
                return;
              }
              tick();
            }, 100);
          }
        });
      })();
    });
  };
  const mb = function(path) {
    return function(obj) {
      return (
        path.map(function(prop) {
          obj = obj || {};
          if (isNaN(parseInt(prop))) {
            return (obj = obj[prop]);
          } else {
            return (obj = obj.objectAt(parseInt(prop)));
          }
        }) && obj
      );
    };
  };
  const find = function(path) {
    const page = getCurrentPage();
    const parts = path.split('.');
    const last = parts.pop();
    let obj;
    let parent = mb(parts)(page) || page;
    if (typeof parent.objectAt === 'function') {
      parent = parent.objectAt(0);
    }
    obj = parent[last];
    if (typeof obj === 'undefined') {
      throw new Error(`The '${path}' object doesn't exist`);
    }
    if (typeof obj === 'function') {
      obj = obj.bind(parent);
    }
    return obj;
  };
  const clipboard = function() {
    return window.localStorage.getItem('clipboard');
  };
  models(library, utils.create);
  http(library, utils.respondWith, utils.set);
  visit(library, pages, setCurrentPage);
  click(library, pauseUntil, find, utils.click);
  form(library, find, utils.fillIn, utils.triggerKeyEvent, getCurrentPage);
  debug(library, assert, utils.currentURL);
  assertHttp(library, assert, pauseUntil, utils.lastNthRequest);
  assertModel(library, assert, find, getCurrentPage, pauseUntil, utils.pluralize);
  assertPage(library, assert, find, getCurrentPage);
  assertDom(library, assert, pauseUntil, utils.find, utils.currentURL, clipboard);
  assertForm(library, assert, find, getCurrentPage);

  return library.given(["I'm using a legacy token"], function(number, model, data) {
    window.localStorage['consul:token'] = JSON.stringify({ AccessorID: null, SecretID: 'id' });
  });
}
