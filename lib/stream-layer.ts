import { spawn } from 'child_process'

import type { XlsxProcessingContext } from './context.ts'

/**
 * Number of lines to send per data transmission
 */
const BATCH_SIZE = 1000

/**
 * Allows sending data to the corresponding dataset in batches.
 * @param idStream            Stream ID for displaying progress
 * @param tmpFile             Full path of the file to be processed
 * @param layerName           Name of the layer from which the data is extracted
 * @param layerFeatureCount   Number of rows of data to extract
 * @param datasetId           Identifier of the dataset to which the data is sent
 * @param axios               Server for API requests
 * @param log                 Log system that is displayed on the user interface
 * @param isStopped           Function allowing the program to stop if requested
 * @param datasetName         Dataset name, empty by default (use for update)
 */
export const streamLayerToDataset = async (idStream : number, tmpFile: string, layerName: string, layerFeatureCount: number, datasetId: string, axios : XlsxProcessingContext['axios'], log : XlsxProcessingContext['log'], isStopped: () => boolean, datasetName : string = '') => {
  // Table containing the data being sent
  const batch: object[] = []
  let total = 0 // Data sent counter
  let corrupted = 0

  const progressName = `${idStream}. Envoi des données ${datasetName.length > 0 ? `- ${datasetName} ` : ''}- ${layerName}`
  await log.task(progressName)
  await log.progress(progressName, 0, layerFeatureCount)

  // Function to clear the batch array and sends the data it contained
  const flushBatch = async () => {
    if (batch.length === 0) return
    const toSend = batch.splice(0)
    total += toSend.length
    await axios.post(`api/v1/datasets/${datasetId}/_bulk_lines`, toSend)

    await log.progress(progressName, total, layerFeatureCount)
  }

  // Launch a child process to retrieve the data
  // -f GeoJSONSeq : Output format, one JSON feature per line
  // !! We don't use `runCommand` here for optimization reasons! We load and unload the data little by little rather than all at once
  const proc = spawn('ogr2ogr', ['-f', 'GeoJSONSeq', '/vsistdout/', tmpFile, layerName])

  // Creating listeners to stop the child process, retrieve its error outputs, and system errors
  const stderrChunks: Buffer[] = []
  proc.stderr.on('data', (d: Buffer) => {
    stderrChunks.push(d)
  })

  const procClosed = new Promise<number>((resolve) => {
    proc.on('close', (code) => {
      resolve(code ?? 0)
    })
  })
  proc.on('error', (err) => { throw err })

  // Data reading loop
  let textBuffer = ''
  let lineCount = 0

  // Analysis chunk by chunk; a chunk corresponds to a piece of data, not necessarily a complete line
  for await (const chunk of proc.stdout) {
    if (isStopped()) {
      proc.kill()
      await procClosed
      return
    }

    // Text accumulation
    textBuffer += chunk.toString()
    const lines = textBuffer.split('\n')
    textBuffer = lines.pop() ?? ''

    // Reading of each extracted line
    for (const line of lines) {
      if (!line.trim()) continue // If the line is empty, we move on to the next one.
      lineCount++
      try {
        const feature = JSON.parse(line)

        // We only keep the properties, the data recorded by column, and the geometry
        if (!feature.properties || !feature.geometry) {
          corrupted += 1
          continue
        }

        batch.push({
          ...feature.properties,
          geometry: JSON.stringify(feature.geometry)
        })

        // If the number of lines to be sent exceeds the limit, they are sent.
        if (batch.length >= BATCH_SIZE) {
          await flushBatch()
        }
      } catch {
        // If the JSON is invalid, no error is triggered; the process moves to the next line.
        await log.debug(`Ligne malformée ignorée (ligne ${lineCount})`)
      }
    }
  }
  if (isStopped()) return

  // If there is still data after the read loop (remaining line without \n), we retrieve it and add it.
  if (textBuffer.trim()) {
    try {
      const feature = JSON.parse(textBuffer)
      if (feature.properties && feature.geometry) {
        batch.push({
          ...feature.properties,
          geometry: JSON.stringify(feature.geometry)
        })
      }
    } catch {
      await log.debug('Buffer résiduel non parseable, ignoré')
    }
  }

  // We send the remaining data, features that have not reached BATCH_SIZE
  await flushBatch()
  if (isStopped()) return

  // We wait for the command to finish and then check the exit code.
  const exitCode = await procClosed
  if (exitCode !== 0) {
    const stderr = Buffer.concat(stderrChunks).toString()
    throw new Error(`ogr2ogr en échec avec le code ${exitCode}: ${stderr}`)
  }

  if (corrupted > 0) {
    await log.warning(`${corrupted} lignes sont mal formées, elles ont été ignorées`)
  }
}
