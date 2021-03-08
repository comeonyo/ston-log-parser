/**
 * 스톤 로그 파일 스트림을 json 스트림으로 변환
 * @module StonLogTransform
 */

const stream = require('stream');
const util = require('util');

const formats = {
  'ston_v1.0': [
    'sDate', 'sTime', 'sIp', 'csMethod', 'csUriStem', 'csUriQuery',
    'sPort', 'csUsername', 'cIp', 'cs', 'scStatus', 'scBytes',
    'timeTaken', 'csReferer', 'scResinfo', 'csRange', 'scCachehit', 'csAcceptencoding',
    'sessionId', 'scContentLength', 'timeResponse', 'xTransactionStatus', 'xVhostlink'
  ]
};

const option_defaults = {
  format: 'ston',
  version: '1.0'
};

/**
 *
 * @param options
 * @constructor
 */
const StonLogTransform = function (options) {
  this.formats = formats;
  this.options = options || {};
  stream.Transform.call(this, {objectMode: true});
};
util.inherits(StonLogTransform, stream.Transform);


/**
 * 로그 파일의 텍스트를 object로 transform
 * @param chunk
 * @param encoding
 * @param done
 * @private
 */
StonLogTransform.prototype._transform = function (chunk, encoding, done) {
  try {
    encoding = encoding || 'utf8';

    if (Buffer.isBuffer(chunk)) {
      if (encoding === 'buffer') chunk = chunk.toString(encoding);
      else chunk = chunk.toString(encoding);
    }

    if (this._lastLineBuffer) chunk = this._lastLineBuffer + chunk;

    const lines = chunk.split('\n');
    this._lastLineBuffer = lines.splice(lines.length - 1, 1)[0];

    parse(lines, this.options).forEach(this.push.bind(this));
    done();
  } catch (e) {
    throw e;
  }
};


/**
 * 이전 버퍼에서 끊긴 데이터 이어서 처리
 * @param done
 * @private
 */
StonLogTransform.prototype._flush = function (done) {
  if (this._lastLineBuffer) {
    parse(this._lastLineBuffer, this.options).forEach(this.push.bind(this));
    this._lastLineBuffer = null;
  }

  done();
};

/**
 * 로그 파일 텍스트에서 주석, 줄바꿈라인 등을 제거하고 json 데이터 리턴
 * @param data 로그 파일에서 읽은 텍스트 데이터
 * @param options 로그 타입 및 버전이 생길 경우
 * @param callback
 * @returns {any[]}
 */
function parse(data, options, callback) {
  let parsed;
  let err;
  try {
    if (Buffer.isBuffer(data)) {
      data = data.toString();
    }

    if (typeof data === 'string') data = data.split('\n');

    parsed = data
      .filter(line => !line.startsWith('#'))
      .filter(line => line.length > 1)
      .map(line => parseLine(line, options));
  } catch (e) {
    err = e;
    if (!callback) throw e;
  }

  if (callback) callback(err, parsed);
  else return parsed;
}

/**
 * 로그 텍스트의 위치에 따라서 key 값 매칭하여 object 리턴
 * @param {string} line 로그 텍스트
 * @param options
 */
function parseLine(line, options) {
  options = options || {};

  let format = options.format;
  if (format === undefined) format = option_defaults.format;

  let version = options.version;
  if (version === undefined) version = option_defaults.version;

  const headings = formats[`${format}_v${version}`];
  if (!headings) throw new Error(`Format not recognized: ${format}`);

  const line_arr = line.split(' ');
  return _zipLine(line_arr, headings);
}

/**
 * headings와 arr 값 순서대로 키 - 값 매칭
 * @param {array} line_arr split 된 로그 데이터 (1라인)
 * @param {array} headings 'sDate', 'sTime', 'sIp', 'csMethod', 'csUriStem', 'csUriQuery', ...
 * @private
 */
function _zipLine(line_arr, headings) {
  const result = {};
  for (let i = 0; i < Math.max(line_arr.length, headings.length); i++) {
    const field = headings[i];
    result[field] = line_arr[i] === undefined? '-' : line_arr[i];
  }

  return result;
}


module.exports = StonLogTransform;
module.exports.parse = parse;
module.exports.parseLine = parseLine;