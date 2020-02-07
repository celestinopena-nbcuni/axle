module.exports = (function() {
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
  return {
    readFile: readFile,
    readConfig: readConfig,
    readObject: readObject,
    obj2str: obj2str,
    arg: arg,
    propstr: propstr
  }
})()