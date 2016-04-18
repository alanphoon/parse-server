import { md5Hash, newObjectId } from './cryptoUtils';
import { logger } from './logger';

const PUSH_STATUS_COLLECTION = '_PushStatus';

export function flatten(array) {
  return array.reduce((memo, element) => {
    if (Array.isArray(element)) {
      memo = memo.concat(flatten(element));
    } else {
      memo = memo.concat(element);
    }
    return memo;
  }, []);
}

export default function pushStatusHandler(config) {

  let initialPromise;
  let pushStatus;
  let objectId = newObjectId();
  let database = config.database.WithoutValidation();

  let setInitial = function(body = {}, where, options = {source: 'rest'}) {
    let now = new Date();
    let data =  body.data || {};
    let payloadString = JSON.stringify(data);
    let object = {
      _id: objectId,
      pushTime: now.toISOString(),
      _created_at: now,
      query: JSON.stringify(where),
      payload: payloadString,
      source: options.source,
      title: options.title,
      expiry: body.expiration_time,
      status: "pending",
      numSent: 0,
      pushHash: md5Hash(payloadString),
      // lockdown!
      _wperm: [],
      _rperm: []
    }
<<<<<<< HEAD
    initialPromise = collection().then((collection) => {
      return collection.insertOne(object);
    }).then((res) => {
=======

    return database.create(PUSH_STATUS_COLLECTION, object).then(() => {
>>>>>>> upstream/master
      pushStatus = {
        objectId
      };
      return Promise.resolve(pushStatus);
    });
  }

  let setRunning = function(installations) {
    logger.verbose('sending push to %d installations', installations.length);
<<<<<<< HEAD
    return initialPromise.then(() => {
      return collection();
    }).then((collection) => {
      return collection.updateOne({status:"pending", _id: objectId}, {$set: {status: "running"}});
   });
=======
    return database.update(PUSH_STATUS_COLLECTION,
      {status:"pending", objectId: objectId},
      {status: "running"});
>>>>>>> upstream/master
  }

  let complete = function(results) {
    let update = {
      status: 'succeeded',
      numSent: 0,
      numFailed: 0,
    };
    if (Array.isArray(results)) {
      results = flatten(results);
      results.reduce((memo, result) => {
        // Cannot handle that
        if (!result.device || !result.device.deviceType) {
          return memo;
        }
        let deviceType = result.device.deviceType;
        if (result.transmitted)
        {
          memo.numSent++;
          memo.sentPerType = memo.sentPerType || {};
          memo.sentPerType[deviceType] = memo.sentPerType[deviceType] || 0;
          memo.sentPerType[deviceType]++;
        } else {
          memo.numFailed++;
          memo.failedPerType = memo.failedPerType || {};
          memo.failedPerType[deviceType] = memo.failedPerType[deviceType] || 0;
          memo.failedPerType[deviceType]++;
        }
        return memo;
      }, update);
    }
    logger.verbose('sent push! %d success, %d failures', update.numSent, update.numFailed);
<<<<<<< HEAD
    return initialPromise.then(() => {
      return collection();
    }).then((collection) => {
      return collection.updateOne({status:"running", _id: objectId}, {$set: update});
    });
=======
    return database.update('_PushStatus', {status:"running", objectId }, update);
>>>>>>> upstream/master
  }

  let fail = function(err) {
    let update = {
      errorMessage: JSON.stringify(err),
      status: 'failed'
    }
    logger.error('error while sending push', err);
<<<<<<< HEAD
    return initialPromise.then(() => {
      return collection();
    }).then((collection) => {
      return collection.updateOne({_id: objectId}, {$set: update});
    });
=======
    return database.update('_PushStatus', { objectId }, update);
>>>>>>> upstream/master
  }

  return Object.freeze({
    objectId,
    setInitial,
    setRunning,
    complete,
    fail
  })
}
