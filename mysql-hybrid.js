#!/bin/env node

const mysql = require('mysql2');
const cluster = require('cluster');
const numCPUs = 1;//require('os').cpus().length - 4;

if (cluster.isMaster) {
  const pool = [];

  for (let i = 0; i < numCPUs; i++) {
    pool.push(cluster.fork());
  }

  const Reader = require('line-by-line');

  const reader = new Reader('RCS_R_F_170108_00555.xml', {
    encoding: 'ascii',
    skipEmptyLines: true,
    start: 354
  });

  const flowProcessor = new FlowProcessor();

  let lineBuffer = [];
  let workerIndex = 0;

  reader.on('line', line => {
    lineBuffer.push(line);

    if (line === "</F>") {
      let records = flowProcessor.processFlow(lineBuffer);

      pool[workerIndex++ % numCPUs].send(records);
      lineBuffer = [];
    }
  });

  reader.on('end', _ => pool.forEach(_ => _.disconnect()));
  reader.on('error', console.log);
}
else if (cluster.isWorker) {
  const worker = new DB();

  process.on('message', worker.addRecords);
  process.on('disconnect', worker.commit);
}

function FlowProcessor() {
  const currentRecord = {};
  let records = [];

  function flow(line) {
    currentRecord.route = line.substring(12, 17);
    currentRecord.origin = line.substring(22, 26);
    currentRecord.destination = line.substring(31, 35);
  }

  function ticketType(line) {
    currentRecord.ticketCode = line.substring(10, 13);
  }

  function fulfilmentMethod(line) {
    currentRecord.p = 'null';
    currentRecord.s = 'null';
    currentRecord.k = 'null';

    line.substring(12, line.length - 2).split(' ').forEach(setProp);

    records.push(currentRecord);
  }

  function setProp(prop) {
    const parts = prop.split('=');
    currentRecord[parts[0]] = parts[1];
  }

  function processFlow(lines) {
    records = [];
    for (let i in lines) {
      const line = lines[i];
      switch (line.charAt(5)) {
        case '"': flow(line); break;
        case 'T': ticketType(line); break;
        case ' ': fulfilmentMethod(line); break;
      }
    }

    return records;
  }

  this.processFlow = processFlow;
}

function DB() {
  let buffer = 'INSERT INTO rcs_flow VALUES ';

  const db = mysql.createConnection({
    // host: 'localhost', 
    // user: 'root',      
    // password: '',      
    // database: 'rcs'
    host: 'challenger-db.test.aws.assertis',
    user: 'assertis',
    password: 'assertis',
    database: 'rcs'
  });

  function commit() {
    db.end();
  }

  function season(s) {
    const set = [];

    if (s.charAt(1) === '1') { return 'week,month,3-months,6-months,year'; }
    if (s.charAt(2) === '1') { set.push('week'); }
    if (s.charAt(3) === '1') { set.push('month'); }
    if (s.charAt(4) === '1') { set.push('3-months'); }
    if (s.charAt(5) === '1') { set.push('6-months'); }
    if (s.charAt(6) === '1') { set.push('year'); }

    return set.join(',');
  }


  function addRecord(currentRecord) {
    buffer += '("'
      +  currentRecord.origin + '","'
      +  currentRecord.destination + '","'
      +  currentRecord.route + '","'
      +  currentRecord.ticketCode + '",'
      +  currentRecord.fm + ',"20'
      +  currentRecord.f.slice(1) + ',"20'
      +  currentRecord.u.slice(1) + ','
      +  currentRecord.p + ','
      +  currentRecord.k + ',"'
      +  season(currentRecord.s) + '")';

    if (buffer.length > 2000000) {
      db.query(buffer);
      buffer = 'INSERT INTO rcs_flow VALUES ';
    }
    else {
      buffer += ',';
    }
  }

  function addRecords(records) {
    records.forEach(addRecord);
  }

  this.commit = commit;
  this.addRecords = addRecords;
}