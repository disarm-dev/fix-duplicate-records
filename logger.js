const fs = require('fs')
const path = require('path')


class Logger {

  constructor() {
    this.counter = 0
    this.folder_name = new Date().toISOString().replace(/\:/g,'').split('.')[0]
    this.folder_path = path.join('run', this.folder_name)
    // create dir
    // fs.mkdirSync('run')
    fs.mkdirSync(this.folder_path)
  }

  write(object) {
    this.counter++
    const filename = `${this.counter}`.padStart(5, '0') + '.json'
    const json_string = JSON.stringify(object)
    const file_path = path.join(this.folder_path, filename)

    fs.writeFileSync(file_path, json_string)
  }
}

module.exports = {Logger}