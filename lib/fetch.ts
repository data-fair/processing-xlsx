import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import type { ProcessingContext } from '@data-fair/lib-common-types/processings.js'
import { pipeline } from 'node:stream/promises'

import fs from 'fs-extra'
import * as path from 'path'

/**
 * Introduction of an error type in case of a file not found.
 */
class FileNotFoundError extends Error {
  constructor (message: string) {
    super(message)
    this.name = 'FileNotFoundError'
  }
}

/**
 * Allows you to download a file using the HTML protocol
 * @param processingConfig  Processing configuration, obtained from the form data (processing-config-schema.json)
 * @param secrets           Sensitive information if necessary (such as a password, for example)
 * @param tmpFile           Name of the temporary file to transfer the downloaded file
 * @param axios             Server for API requests
 * @returns Name of extracted file
 */
export const fetchHTTP = async (processingConfig: ProcessingConfig, secrets: ProcessingContext['secrets'], tmpFile: string, axios: ProcessingContext['axios']) => {
  const password = secrets?.password ?? processingConfig.password
  const opts: any = { responseType: 'stream', maxRedirects: 4 }
  if (processingConfig.username && password) {
    opts.auth = { username: processingConfig.username, password }
  }

  // File retrieval and download
  let res
  try {
    res = await axios.get(processingConfig.url, opts)
  } catch (err: any) {
    if (err.response?.status === 404) throw new FileNotFoundError(`File not found: ${processingConfig.url}`)
    throw err
  }
  await pipeline(res.data, fs.createWriteStream(tmpFile))

  // Retrieving the file name
  if (processingConfig.filename) return processingConfig.filename
  if (res.headers['content-disposition'] && res.headers['content-disposition'].includes('filename=')) {
    if (res.headers['content-disposition'].match(/filename=(.*);/)) return res.headers['content-disposition'].match(/filename=(.*);/)[1]
    if (res.headers['content-disposition'].match(/filename="(.*)"/)) return res.headers['content-disposition'].match(/filename="(.*)"/)[1]
    if (res.headers['content-disposition'].match(/filename=(.*)/)) return res.headers['content-disposition'].match(/filename=(.*)/)[1]
  }
  if (res.request && res.request.res && res.request.res.responseUrl) return decodeURIComponent(path.parse(res.request.res.responseUrl).base)
}
