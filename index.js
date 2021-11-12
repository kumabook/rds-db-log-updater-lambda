const AWS = require('aws-sdk');
const https = require('https');

async function retrieveLastWrittenTime(prefix) {
  const s3 = new AWS.S3({apiVersion: '2006-03-01'});
  const params = { Bucket: process.env['S3_BUCKET'] };
  const res = await s3.headBucket(params).promise();
  try {
    const res2 = await s3.getObject({
      ...params,
      Key: `${prefix}/lastRecievedMarker`
    }).promise();
    return parseInt(res2.Body.toString());
  } catch (e) {
    return 0;
  }
}

async function saveLastWrittenTime(prefix, time) {
  const s3 = new AWS.S3({apiVersion: '2006-03-01'});
  const params = { Bucket: process.env['S3_BUCKET'] };
  const res = await s3.putObject({
    ...params,
    Key: `${prefix}/lastRecievedMarker`,
    Body: time.toString()
  }).promise();
}

function notifySlack(msg, slackUrl) {
  const url = new URL(process.env['SLACK_URL']);
  const options = {
    hostname: url.hostname,
    port: 443,
    path: url.pathname,
    method: 'POST',
    headers: {
      'Content-Type': 'application/json'
    }
  };
  const req = https.request(options);
  const data = JSON.stringify({ text: msg });
  req.write(data);
  req.end();
}

function notify(msg) {
  console.log(msg);
  const url = process.env['SLACK_URL'];
  if (url) {
    notifySlack(msg, url);
  }
}

async function sleep(t) {
  return await new Promise(r => setTimeout(() => r(), t));
}

exports.handler = async (event) => {
  const prefix = process.env['S3_KEY_PREFIX'];
  const logName = process.env['LOG_NAME'];

  const rds = new AWS.RDS();
  const s3 = new AWS.S3({apiVersion: '2006-03-01'});
  const params = {
    DBInstanceIdentifier: prefix
  };
  const logFiles = [];
  const lastWrittenTime = await retrieveLastWrittenTime(prefix);
//  notify(`Start upload ${logName}`);
  let lastWrittenThisRun = 0;
  try {
    const data = await rds.describeDBLogFiles({
      ...params,
      FilenameContains: logName
    }).promise();
    notify(`Downloading log file since ${lastWrittenTime}`);
    for (let log of data.DescribeDBLogFiles) {
      const timestamp = log.LastWritten;
      const fileName = log.LogFileName;
      if (timestamp > lastWrittenTime) {
//        notify(`Downloading log file: ${fileName} written at ${timestamp}`);
        if (timestamp > lastWrittenThisRun) {
          lastWrittenThisRun = timestamp;
        }
        let logFile = await rds.downloadDBLogFilePortion({
          ...params,
          LogFileName: fileName,
          Marker: '0'
        }).promise();
        let logFileData = logFile['LogFileData'];
        while (logFile['AdditionalDataPending']) {
          logFile = await rds.downloadDBLogFilePortion({
            ...params,
            LogFileName: fileName,
            Marker: logFile['Marker']
          }).promise();
          logFileData += logFile['LogFileData'];
        }
        const objectName = `${prefix}/${fileName}`;
        const res = await s3.putObject({
          Bucket: process.env['S3_BUCKET'],
          Key: objectName,
          Body: logFileData
        }).promise();
        logFiles.push(fileName);
      }
    }
    saveLastWrittenTime(prefix, lastWrittenThisRun);
    notify('Successfully upload logFile');
    await sleep(10);
    notify(`${logFiles.join('\n')}`);
  } catch (e) {
    notify(e.message);
  }
  await sleep(1000);
  const response = {
    statusCode: 200,
    body: JSON.stringify({
      sdkVersion: AWS.VERSION,
      logFiles,
      lastWrittenThisRun
    }),
  };
  return response;
};
