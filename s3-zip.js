const s3Files = require('s3-files')
const archiver = require('archiver')

const s3Zip = {}
module.exports = s3Zip

//s3Zip.archive({ s3Client: client, region, bucket }, folder, files, archiveFiles);
s3Zip.archive = async function (opts) { //buckets, folder, filesS3, filesZip
  const self = this

  self.debug = opts.debug || false

  const paths = opts.paths;
  const region = opts.region;

  this.zipArchive = archiver(this.format || 'zip', this.archiverOpts || {})

  for (let index = 0; index < paths.length; index++) {
    const path = paths[index];
    const { s3, bucket, folder, files, archiveFiles } = path;
    let connectionConfig

    if ('s3' in path) {
      connectionConfig = {
        s3: s3
      }
    } else {
      connectionConfig = {
        region: region
      }
    }

    connectionConfig.bucket = bucket;

    const client = s3Files.connect(connectionConfig)
    const keyStream = client.createKeyStream(folder, files)

    const preserveFolderStructure = opts.preserveFolderStructure === true || archiveFiles
    const fileStream = s3Files.createFileStream(keyStream, preserveFolderStructure)
    const isLastOne = index === paths.length - 1;

    await self.archiveStream(fileStream, isLastOne, files, archiveFiles, folder)

  };

  return this.zipArchive
}

s3Zip.archiveStream = async function (stream, isLastOne, filesS3, filesZip, folder) {
  return new Promise((resolve, reject) => {
    const self = this
    const folder = folder || ''
    if (this.registerFormat) {
      archiver.registerFormat(this.registerFormat, this.formatModule)
    }

    this.zipArchive.on('error', function (err) {
      self.debug && console.log('archive error', err)
    })
    stream
      .on('data', function (file) {
        if (file.path[file.path.length - 1] === '/') {
          self.debug && console.log('don\'t append to zip', file.path)
          return
        }
        let fname
        if (filesZip) {
          // Place files_s3[i] into the archive as files_zip[i]
          const i = filesS3.indexOf(file.path.startsWith(folder) ? file.path.substr(folder.length) : file.path)
          fname = (i >= 0 && i < filesZip.length) ? filesZip[i] : file.path
          filesS3[i] = ''
        } else {
          // Just use the S3 file name
          fname = file.path
        }
        const entryData = typeof fname === 'object' ? fname : { name: fname }
        self.debug && console.log('append to zip', fname)

        if (file.data.length === 0) {
          self.zipArchive.append('', entryData)
        } else {
          self.zipArchive.append(file.data, entryData)
        }
      })
      .on('end', function () {
        self.debug && console.log('end -> finalize')
        if (isLastOne) {
          self.zipArchive.finalize()
        }
        resolve(self.zipArchive)
      })
      .on('error', function (err) {
        self.zipArchive.emit('error', err)
        reject(err)
      })
  });

}

s3Zip.setFormat = function (format) {
  this.format = format
  return this
}

s3Zip.setArchiverOptions = function (archiverOpts) {
  this.archiverOpts = archiverOpts
  return this
}

s3Zip.setRegisterFormatOptions = function (registerFormat, formatModule) {
  this.registerFormat = registerFormat
  this.formatModule = formatModule
  return this
}

