#!/bin/env node

const Reader = require('line-by-line');
const reader = new Reader('RCS_R_F_170108_00555.xml', {
  encoding: 'ascii',
  skipEmptyLines: true,
  start: 354
});

const worker = new FlowProcessor();
let lineBuffer = [];

reader.on('error', console.error);
reader.on('line', line => {
  lineBuffer.push(line);

  if (line === "</F>") {
    worker.processFlow(lineBuffer);
    lineBuffer = [];
  }
});

function FlowProcessor() {
  let buffer = 'INSERT INTO rcs_flow VALUES ';
  const currentRecord = {};

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

    addRecord();
  }

  function setProp(prop) {
    const parts = prop.split('=');
    currentRecord[parts[0]] = parts[1];
  }

  function addRecord() {
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

    if (buffer.length > 4000000) {
      console.log(buffer + ";");
      buffer = 'INSERT INTO rcs_flow VALUES ';
    }
    else {
      buffer += ',';
    }
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

  function processFlow(lines) {
    for (let i in lines) {
      const line = lines[i];
      switch (line.charAt(5)) {
        case '"': flow(line); break;
        case 'T': ticketType(line); break;
        case ' ': fulfilmentMethod(line); break;
      }
    }
  }

  this.processFlow = processFlow;
}