/* General functions */
const path = require('path'),
  fs = require('fs'),
  YAML = require('yaml')

function readFile(filepath) {
  let contents = null;
  try {
    contents = fs.readFileSync(filepath, 'utf8')
  } catch (err) { console.error(err) }
  return contents
}

function readConfig(filepath) {
  let contents = null;
  try {
    contents = YAML.parse(fs.readFileSync(filepath, 'utf8'))
  } catch (err) { console.error(err) }
  return contents
}

function readObject(filepath) {
  let contents = null;
  try {
    contents = JSON.parse(fs.readFileSync(filepath, 'utf8'))
  } catch (err) { console.error(err) }
  return contents
}

function obj2str(obj) { return JSON.stringify(obj, null, 2) }

function arg(i) {
  if (i<0 || i>=process.argv.length-1) return '';
  return process.argv[i+1];
}

function propstr(sourceObj, propList) {
  return propList.reduce((orig, curr) => {
    if (sourceObj[curr]) orig.push(`${curr}: ${sourceObj[curr]}`)
    return orig
  }, []).join(', ')
}

function convertUnixDate(unixdate) { return new Date(unixdate * 1000).toLocaleString() }

function isNonBlank(item) {
  if (Array.isArray(item)) return item.length>0
  return (!(item===null || item===undefined || item==''))
}

// Copy a specified list of attributes of an object into a new object
// example: copy 'name' and 'email' from sample -> copyFields(sample, ['name', 'email'])
function copyFields(source, fieldList, target={}) {
  return fieldList.reduce((orig, field) => {
    if (source[field] && isNonBlank(source[field])) orig[field] = source[field]
    return orig
  }, target)
}

// Remove within a given object any array element or attribute which is blank
function noblanks(obj) {
  if (Array.isArray(obj)) {
    obj=obj.filter(item => item.toString().trim()!='');
    obj.forEach(item => { if (item instanceof Object) return this.noblanks(item); })
  } else if (obj instanceof Object) {
    Object.keys(obj).forEach(key => {
      let chd=obj[key];
      if (chd==='') delete obj[key];
      else if (chd instanceof Object) return this.noblanks(chd);
    })
  } else { if (obj=='') obj='--'; } // NOT an array OR object
  return obj;
}

module.exports = {
  readFile: readFile,
  readConfig: readConfig,
  readObject: readObject,
  obj2str: obj2str,
  arg: arg,
  propstr: propstr,
  convertUnixDate: convertUnixDate,
  copyFields: copyFields,
  noblanks: noblanks
}
