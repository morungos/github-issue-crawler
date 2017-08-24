const log4js = require('log4js');
const logger = log4js.getLogger('execute');

const url = require('url');

const MongoClient = require('mongodb').MongoClient;

log4js.configure({appenders: [{type: "console"}], levels: {"[all]" : "DEBUG"}});

const fetch = require('node-fetch');

const SECONDS = 1000;
const MINUTES = 60 * SECONDS;
const HOURS = 60 * MINUTES;
const DAYS = 24 * HOURS;

const config = {
  github: {
    user: "<REDACTED>",
    token: "<REDACTED>",
  },
  database: {
    url: 'mongodb://localhost:27017/github',
    options: {},
  }
};

const forEach = (fn, values) => {

  const forEachIndex = (fn, values, index) => {
    if (index === values.length) {
      return Promise.resolve();
    } else {
      const value = values[index];
      const result = fn.call(null, value);
      if (typeof result !== 'undefined' && result !== null && typeof result.then !== 'undefined') {
        return result.then(() => forEachIndex(fn, values, index + 1));
      } else {
        return forEachIndex(fn, values, index + 1);
      }
    }
  }

  return forEachIndex(fn, values, 0);
}


class Crawler {

  constructor(options = {}) {

    this.delayTime = (options.delayTime || 500);
    this.sleepDelayTime = 5*SECONDS;

    this.requestQueue = [];

    this.requestHeaders = {}
    this.requestLimit = options.requestLimit;

    const token = config['github']['user'] + ":" + config['github']['token'];
    const encoded = Buffer.from(token, 'latin1').toString('base64');
    logger.debug("Authentication token", token, encoded);

    this.requestHeaders['authorization'] = `Basic ${encoded}`;
    this.requestHeaders['accept'] = "application/json, application/vnd.github.squirrel-girl-preview";
  }

  connect() {
    logger.debug("MongoRepository#connect");
    return MongoClient.connect(config['database']['url'], config['database']['options'])
      .then((handle) => {
        this.db = handle;
        return this.db.collection('messages').createIndex({repository_url: 1});
      })
      .then(() => this.db.collection('sources').createIndex({check: 1}))
      .then(() => this.db);
  }

  disconnect() {
    return new Promise((resolve, reject) => this.db.close());
  }

  delay(n) {
    return new Promise((resolve, reject) => setTimeout(resolve, n));
  }

  connected(fn) {
    return this.connect()
      .then((handle) => fn.call(this))
      .catch((error) => logger.error(error))
      .then(() => this.disconnect());
  }

  updateMessage(source, identifier, message) {
    return this.db.collection('messages')
      .findOneAndUpdate({_id: identifier}, {$set: message}, {upsert: true});
  }

  getMessage(identifier) {
    return this.db.collection('messages')
      .findOne({_id: identifier});
  }

  setSourceValues(source, values) {
    return this.db.collection('sources')
      .updateOne({_id: source}, {$set: values}, {upsert: true});
  }

  // Small tweak so that we switch to more repositories if ever the queue is
  // empty, as a fallback. Should enable us to cut through slack time better.
  getNextSource() {
    const current = Date.now();
    return this.db.collection('sources')
      .findOne({check: {$lte: current}}, {sort: 'check'})
      .then((record) => {
        if (record) {
          return record;
        } else {
          return this.db.collection('sources')
            .findOne({_id: "https://api.github.com/repositories"})
        }
      })
      .then((record) => record || null);
  }

  loadQueue() {
    return this.getNextSource()
      .then((source) => {
        if (! source) {
          return;
        }
        const request = Object.assign({}, source, {source: source._id, url: source.next || source._id});
        this.requestQueue.unshift(request);
      })
  }

  hasNextRequest() {
    return this.requestQueue.length > 0;
  }

  getNextRequest() {
    return this.requestQueue.shift();
  }

  queueRequest(request) {
    this.requestQueue.unshift(request);
  }

  // Only used when we first find a repository.
  getIssuesSource(request, repository, nextCheck, index) {
    // logger.debug("Adding source: ", repository.issues_url.replace(/\{[^\}]*\}/, '') + "?state=all&sort=created&direction=asc");
    return {
      url: repository.issues_url.replace(/\{[^\}]*\}/, '') + "?state=all&sort=created&direction=asc&per_page=100",
      values: {
        type: 'issues',
        check: nextCheck + index,
        data: {
          fork: repository.fork,
        },
      }
    };
  }

  getCommentsSource(request, issue, nextCheck, index) {
    // logger.debug("Adding source: ", issue.repository_url + "/issues/comments?sort=created&direction=asc");
    return {
      url: issue.repository_url + "/issues/comments?sort=created&direction=asc&per_page=100",
      values: {
        type: 'comments',
        check: nextCheck + index,
      }
    };
  }

  getIssueMessage(issue) {
    const result = Object.assign({}, issue);
    result.messageId = result.url;
    result.originatingMessageId = result.url;
    delete result.assignee;
    delete result.assignees;
    delete result.milestone;
    if (result.user) {
      result.user_id = result.user.id;
      result.user_avatar_url = result.user.avatar_url;
      result.user = result.user.login;
    } else {
      result.user_id = null;
      result.user_avatar_url = null;
      result.user = null;
    }
    result.type = 'issue';
    delete result.html_url;
    return result;
  }

  getCommentMessage(comment) {
    const result = Object.assign({}, comment);
    result.messageId = result.url;
    result.originatingMessageId = result.issue_url;
    if (result.user) {
      result.user_id = result.user.id;
      result.user_avatar_url = result.user.avatar_url;
      result.user = result.user.login;
    } else {
      result.user_id = null;
      result.user_avatar_url = null;
      result.user = null;
    }
    result.type = 'comment';
    delete result.issue_url;
    delete result.html_url;
    return result;
  }

  getNextLink(headers) {
    const linkHeader = headers.get('link');
    if (! linkHeader) {
      return null;
    }
    const links = linkHeader.split(/,\s*/);
    for (const link of links) {
      const matches = link.match(/<([^>]+)>; rel=["'](\w+)["']/);
      if (matches && matches[2] === "next") {
        return matches[1];
      }
    }
    return null;
  }

  queueNextLink(source, headers) {
    const linkHeader = headers.get('link');
    if (! linkHeader) {
      return false;
    }

    const links = linkHeader.split(/,\s*/);
    for (const link of links) {
      const matches = link.match(/<([^>]+)>; rel=["'](\w+)["']/);
      if (matches && matches[2] === "next") {
        logger.debug("queueNextLink", matches[1]);
        this.queueRequest({source: source, url: matches[1]})
        break
      }
    }
    return true;
  }

  doRequest(request) {
    if (this.requestLimit !== null && this.requestLimit-- == 0)
      return Promise.reject(new Error("Limit exceeded"));

    const checked = Date.now();
    return fetch(request.url, {timeout: 20000, headers: this.requestHeaders})
      .then((response) => this.doResponse(request, response, checked))
      .catch((error) => logger.error("Got an error", error))
  }

  doResponse(request, response, checked) {
    const nextLink = this.getNextLink(response.headers);
    return response.json()
      .then((data) => {
        if (! Array.isArray(data)) {

          // If it's an API limit thing, back off by waiting a moment and then
          // rejecting.
          if (data.message && /API rate limit/.test(data.message)) {
            logger.warn("Rate limit exceeded, pausing and retrying", request.url);
            return this.delay(1*MINUTES)
              .then(() => Promise.reject(new Error(data.message)));
          }

          logger.warn("Can't access URL: ", request.url);
          return Promise.resolve({check: Date.now() + 30*DAYS, response: data});
        }
        return this.doResponseData(request, response, data, checked, nextLink)
      })
      .then((values) => {
        return this.setSourceValues(request.source, values);
      });
  }

  /**
   * Handles a request and its data. Should return a new set of source values
   * to apply to the source. This can, and probably should, adapt to the source
   * type, and whether or not there is a next link, which is a proxy for
   * completeness.
   */
  doResponseData(request, response, data, checked, nextLink) {

    const parsed = url.parse(request.url);
    const messages = [];
    const sources = [];
    const values = {};
    let index = 0;
    const thisCheck = Date.now();
    values.check = thisCheck;
    values.continued = true;
    if (nextLink) {
      values.next = nextLink;
    }

    const etag = response.headers.get('etag');

    logger.info(`${request.url}: ${data.length} records obtained${nextLink ? " (more follow)" : ""}`);

    if (request.type === 'issues') {
      // Special case. First iteration of the issues, if we have >0 issues, add a comments source.
      if (! request.continued && data.length > 0) {
        sources.push(this.getCommentsSource(request, data[0], thisCheck, index++));
      }
      for (const issue of data) {
        messages.push(Object.assign({source: request.source}, this.getIssueMessage(issue)));
      }
      if (values.next) {
        values.check += 10*SECONDS;
      } else {
        values.check += 180*DAYS;
        if (etag) values.etag = etag;
        values.next = request.url.replace(/\?.*/, '') + `?state=all&sort=updated&direction=asc&per_page=100&since=${(new Date(thisCheck)).toISOString()}`;
        delete values.continued;
      }
    } else if (request.type === 'comments') {
      for (const comment of data) {
        messages.push(Object.assign({source: request.source}, this.getCommentMessage(comment)));
      }
      if (values.next) {
        values.check += 5*SECONDS;
      } else {
        values.check += 180*DAYS;
        if (etag) values.etag = etag;
        values.next = request.url.replace(/\?.*/, '') + `?sort=updated&direction=asc&per_page=100&since=${(new Date(thisCheck)).toISOString()}`;
        delete values.continued;
      }
    } else if (request.type === 'repositories') {
      for (const repository of data) {
        sources.push(this.getIssuesSource(request, repository, thisCheck, index++));
      }
      values.check += 100*SECONDS;
    } else {
      logger.debug("No messages to resolve")
    }

    return forEach(((message) => this.updateMessage(request.source, message.url, message)), messages)
      .then(() => forEach(((source) => this.setSourceValues(source.url, source.values)), sources))
      .then(() => values);
  }

  // ==========================================================================
  // The main loop
  execute() {

    const doNextRequest = () => {
      if (this.hasNextRequest()) {
        const request = this.getNextRequest();
        return this.doRequest(request)
          .then(() => this.delay(this.delayTime))
          .then(() => doNextRequest())
          .then(() => true);
      } else {
        return Promise.resolve(false);
      }
    }

    const cycle = () => {
      return this.loadQueue()
        .then(() => doNextRequest())
        .then((handled) => {
          if (handled) {
            return cycle();
          } else {
            logger.info("No pending requests: sleeping");
            return this.delay(this.sleepDelayTime)
              .then(() => cycle())
          }
        })
        .catch((error) => logger.error(error));
    }

    return cycle();
  }

}


const crawler = new Crawler();

crawler.connected(() => crawler.execute());
