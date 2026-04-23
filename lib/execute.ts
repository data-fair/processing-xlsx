import type { RunFunction } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import { formatBytes } from '@data-fair/lib-utils/format/bytes.js'

import util from 'util'
import fs from 'fs-extra'
import path from 'path'
import FormData from 'form-data'

import type { XlsxProcessingContext } from './context.ts'
import { fetchHTTP } from './fetch.ts'
import { streamLayerToDataset } from './stream-layer.ts'
import { createTmpFile } from './tmp-file.ts'
import { runCommand } from './spawn-process.ts'

/**
 * Allows for a requested program shutdown to be scheduled.
 */
let shouldBeStopped = false

export const stop: () => Promise<void> = async () => { shouldBeStopped = true }

type LayersFieldList = {
  [idLayer: number]: { name: string, fields: any[], featureCount: number }
}

/**
 * Input function, allows data processing to begin
 * @param context Context of the request
 */
export const run: RunFunction<ProcessingConfig> = async (context) => {
  shouldBeStopped = false

  // Retrieving the contextual elements necessary for processing
  const { processingConfig, patchConfig } = context
  const tmpFile = await download(context)

  if (shouldBeStopped) return
  if (!tmpFile) return
  const layersFieldList = await extraction(context, tmpFile)

  if (shouldBeStopped) return
  if (!layersFieldList) return

  if (processingConfig.datasetMode === 'create') {
    const updateConfig = await createDatasets(context, layersFieldList, tmpFile)

    if (updateConfig?.length) await patchConfig({ datasetMode: 'update', datasets: updateConfig, editableUpdate: processingConfig.dataset.editableCreate })
  } else if (processingConfig.datasetMode === 'update') {
    await updateDatasets(context, layersFieldList, tmpFile)
  } else {
    await patchConfig({ datasetMode: 'create', dataset: { prefix: '' } })
  }
}

/**
 * Allows you to download the file and place it in a temporary folder for later processing.
 * We only process .zip and .xlsx formats; any other format will result in an error.
 * @param processingConfig  Processing configuration, obtained from the form data (processing-config-schema.json)
 * @param tmpDir            Directory where to download the file
 * @param axios             Server for API requests
 * @param log               Log system that is displayed on the user interface
 * @returns Full path of the file to be processed
 */
const download = async ({ processingConfig, tmpDir, axios, log } : XlsxProcessingContext) => {
  await fs.ensureDir(tmpDir)

  await log.step('Téléchargement du fichier')
  let tmpFile = path.join(tmpDir, 'file')
  await fs.ensureFile(tmpFile)
  if (shouldBeStopped) return

  const url = new URL(processingConfig.url)
  let filename = decodeURIComponent(path.basename(url.pathname))
  if (shouldBeStopped) return

  filename = await fetchHTTP(processingConfig, tmpFile, axios) || filename
  if (shouldBeStopped) return

  // Try to prevent weird bug with NFS by forcing syncing file before reading it
  const fd = await fs.open(tmpFile, 'r')
  await fs.fsync(fd)
  await fs.close(fd)
  await log.info(`Le fichier a été téléchargé (${filename})`)
  if (shouldBeStopped) return

  let xlsxFilename

  // Check the file format
  if (filename.toLowerCase().endsWith('.zip')) {
    await log.info(`Dézippage du fichier ${filename}`)

    // Unzip
    await runCommand('unzip', ['-j', tmpFile, '-d', `${tmpFile}-dezip`])

    // We are looking for the .xlsx files contained in the .zip file.
    const filesXlsx: string[] = []
    const files = await fs.readdir(`${tmpFile}-dezip`)
    for (const file of files) {
      if (file.toLowerCase().endsWith('.xlsx')) {
        filesXlsx.push(`${tmpFile}-dezip/${file}`)
      }
    }

    const nbFiles = filesXlsx.length
    if (shouldBeStopped) return

    if (nbFiles <= 0) {
      throw new Error('Il n\'y a pas de fichiers .xlsx à traiter dans ce zip.')
    } else {
      // We keep the first .xlsx file we find, we ignore the others
      xlsxFilename = path.basename(filesXlsx[0])
      tmpFile = filesXlsx[0]
    }
  } else if (filename.toLowerCase().endsWith('.xlsx')) {
    await log.info('Récupération du fichier xlsx')
    xlsxFilename = filename
  } else {
    await log.info('Le format n\'est pas pris en charge')
    throw new Error('Format non pris en charge')
  }

  await log.info(`Traitement du fichier ${xlsxFilename}`)

  return tmpFile
}

/**
 * Allows you to retrieve the layers of a file and organize their structure
 * @param log       Log system that is displayed on the user interface
 * @param tmpFile   Full path of the file to be processed
 * @returns Dictionary of available layer structures (id: {name, fields, featureCount})
 */
const extraction = async ({ log }: XlsxProcessingContext, tmpFile : string) => {
  await log.step('Récupération de la structure des données')

  // Display layers
  const proc = await runCommand('ogrinfo', ['-json', tmpFile])
  if (shouldBeStopped) return

  const result = proc.stdout
  const jsonStructure = JSON.parse(result)
  if (shouldBeStopped) return

  const layers = jsonStructure.layers
  const layersFieldList: LayersFieldList = {}

  for (let i = 0; i < layers.length; i++) {
    for (let j = 0; j < layers[i].fields.length; j++) {
      if (shouldBeStopped) return

      if (!layers[i].fields[j].type) {
        throw new Error(`Pas de type pour ${layers[i].fields[j].name}`)
      }

      let typeCorrect = layers[i].fields[j].type.toLowerCase()

      // Check the types
      if (typeCorrect.includes('integer')) {
        typeCorrect = 'integer'
      }

      if (typeCorrect.includes('real')) {
        typeCorrect = 'number'
      }

      layers[i].fields[j] = {
        ...layers[i].fields[j],
        key: layers[i].fields[j].name,
        type: typeCorrect,
      }
    }

    // Adding geometries
    layers[i].fields.push({
      title: 'geometry',
      name: 'geometry',
      key: 'geometry',
      type: 'string',
      'x-refersTo': 'https://purl.org/geojson/vocab#geometry',
      'x-capabilities': {
        textAgg: false
      }
    })

    await log.info(`Couche ${i + 1} - ${layers[i].name} - ${layers[i].featureCount} lignes`)
    layersFieldList[i + 1] = { name: layers[i].name, fields: layers[i].fields, featureCount: layers[i].featureCount }
  }

  return layersFieldList
}

/**
 * Allows you to create the requested layer datasets
 * @param processingConfig  Processing configuration, obtained from the form data (processing-config-schema.json)
 * @param processingId      Identifier of the processing currently in use
 * @param axios             Server for API requests
 * @param tmpDir            Directory where to download temporary files
 * @param log               Log system that is displayed on the user interface
 * @param ws                Data Fair's Websocket allows retrieving the dataset response.
 * @param layersFieldList   Dictionary containing the structure of the file's layers (id: {name, fields, featureCount})
 * @param tmpFile           Full path of the file to be processed
 * @returns   A list of objects associating layers and datasets, or nothing at all to stop the program
 */
const createDatasets = async ({ processingConfig, processingId, axios, tmpDir, log, ws } : XlsxProcessingContext, layersFieldList: LayersFieldList, tmpFile: string) => {
  await log.step('Construction des jeux de données')

  // If there are no layers to extract, we stop here to simplify the display of logs on the interface.
  if (!processingConfig.idsLayers || processingConfig.idsLayers.length <= 0) {
    await log.info('Pas de couches renseignées')
    return
  }

  const updateConfig = []
  let idStream = 0

  for (const idLayer of processingConfig.idsLayers) {
    if (shouldBeStopped) return

    if (!(idLayer in layersFieldList)) {
      await log.warning(`La couche ${idLayer} n'est pas présente dans les couches disponibles`)
    } else {
      await log.info(`Création du jeu de données pour la couche ${idLayer} - ${layersFieldList[idLayer].name}`)

      const fields = layersFieldList[idLayer].fields

      // Display names and types of the fields for debug
      await log.debug(`   Champs : ${fields.map(f => `${f.key} (${f.type})`).join(', ')}`)

      let dataset

      if (processingConfig.dataset.editableCreate) {
        // Create the dataset, empty
        dataset = (await axios.post('api/v1/datasets', {
          title: `${processingConfig.dataset.prefix}-${layersFieldList[idLayer].name}`,
          description: '',
          isRest: true,
          schema: fields,
          extras: { processingId }
        })).data
        await log.info(`   Jeu de données créé, id="${dataset.id}", titre="${dataset.title}"`)

        if (shouldBeStopped) return

        // Dataset population
        idStream += 1
        await streamLayerToDataset(idStream, tmpFile, layersFieldList[idLayer].name, layersFieldList[idLayer].featureCount, dataset.id, axios, log, () => shouldBeStopped)
      } else {
        const tmpFileGeoJSON = await createTmpFile(tmpDir, tmpFile, layersFieldList[idLayer].name, log, () => shouldBeStopped)
        if (!tmpFileGeoJSON) return

        const formData = new FormData()
        formData.append('schema', JSON.stringify(fields))
        formData.append('title', `${processingConfig.dataset.prefix}-${layersFieldList[idLayer].name}`)
        formData.append('file', await fs.createReadStream(tmpFileGeoJSON), { filename: path.parse(tmpFileGeoJSON).base })
        formData.getLength = util.promisify(formData.getLength)
        const contentLength = await formData.getLength()
        await log.info(`Chargement de ${formatBytes(contentLength!)}`)

        if (shouldBeStopped) return

        dataset = (await axios({
          method: 'post',
          url: 'api/v1/datasets',
          data: formData,
          maxContentLength: Infinity,
          maxBodyLength: Infinity,
          headers: { ...formData.getHeaders(), 'content-length': contentLength }
        })).data
        await log.info(`   Jeu de données créé, id="${dataset.id}", titre="${dataset.title}"`)
      }

      if (shouldBeStopped) return

      // We are waiting for the dataset to finish processing.
      await ws.waitForJournal(dataset.id, 'finalize-end')
      await log.info('Jeu de données complet')

      const datasetObject = { id: dataset.id, href: dataset.href, title: dataset.title }
      const updateObject = { dataset: datasetObject, idLayer }
      updateConfig.push(updateObject)
    }
    await log.info('')
  }
  return updateConfig
}

/**
 * Allows updating a dataset, either by force (schema reset) or by non-force (data replacement).
 * @param processingConfig  Processing configuration, obtained from the form data (processing-config-schema.json)
 * @param axios             Server for API requests
 * @param tmpDir            Directory where to download temporary files
 * @param log               Log system that is displayed on the user interface
 * @param ws                Data Fair's Websocket allows retrieving the dataset response.
 * @param layersFieldList   Dictionary containing the structure of the file's layers (id: {name, fields, featureCount})
 * @param tmpFile           Full path of the file to be processed
 * @returns   Returns nothing, used to stop the program
 */
const updateDatasets = async ({ processingConfig, axios, tmpDir, log, ws } : XlsxProcessingContext, layersFieldList: LayersFieldList, tmpFile: string) => {
  await log.step('Mise à jour des jeux de données')

  // If there are no updates to extract, we stop here to simplify the display of logs on the interface.
  if (!processingConfig.datasets || processingConfig.datasets.length <= 0) {
    await log.info('Pas de mises à jour renseignées')
    return
  }

  let idStream = 0
  // We add size=10000 to ensure that all datasets are retrieved (12 by default)
  const datasets = (await axios.get(`api/v1/datasets/?size=10000&${processingConfig.editableUpdate ? 'rest' : 'file'}=true`)).data.results
  const datasetsIds = new Set<string>(datasets.map(d => d.id))

  // We process each dataset to be updated
  for (const update of processingConfig.datasets) {
    if (shouldBeStopped) return

    const dataset = update.dataset
    const idLayer = update.idLayer
    const formData = new FormData()

    await log.info(`Mise à jour du jeu ${dataset.title} avec la couche ${idLayer}`)

    // Check if the layer is available
    if (!(idLayer in layersFieldList)) {
      await log.warning(`La couche ${idLayer} n'est pas présente dans les couches disponibles`)
      await log.info('')
      continue
    }

    // Check if the correct update operation can be performed, to avoid permission errors
    if (!(datasetsIds.has(dataset.id))) {
      await log.warning(`Le jeu de données ${dataset.title} n'est pas de type ${processingConfig.editableUpdate ? 'éditable' : 'fichier'}`)
      await log.info('')
      continue
    }

    // Retrieving the dataset schema
    const datasetSchema : { key: string, type: string }[] = (await axios.get(`api/v1/datasets/${dataset.id}`)).data.schema
    if (shouldBeStopped) return

    if (update.forceUpdate) {
      await log.info('Mise à jour forcée du schéma')

      if (processingConfig.editableUpdate) {
        // Drop the old data
        await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines?drop=true`, [])
        if (shouldBeStopped) return

        // We are waiting for the dataset to finish processing.
        await ws.waitForJournal(dataset.id, 'finalize-end')

        // Update the schema
        await axios.post(`api/v1/datasets/${dataset.id}`, {
          schema: layersFieldList[idLayer].fields
        })
        if (shouldBeStopped) return

        // We are waiting for the dataset to finish processing.
        await ws.waitForJournal(dataset.id, 'finalize-end')
      } else {
        formData.append('schema', JSON.stringify(layersFieldList[idLayer].fields))
      }
    } else {
      // Check if the schemas match.
      await log.info('Vérification de la compatibilité des schémas')

      let compatible = true
      const datasetSchemaMap = new Map(datasetSchema.map(datasetField => [datasetField.key, datasetField.type]))

      // We don't establish equality in both directions because of the attributes added during the processing of the dataset, such as the update date, for example.
      for (const field of layersFieldList[idLayer].fields) {
        if (shouldBeStopped) return

        const typeFieldDataset = datasetSchemaMap.get(field.name)

        if (typeFieldDataset !== field.type) {
          compatible = false
          break
        }
      }

      if (compatible) {
        // Drop the old data
        if (shouldBeStopped) return
        if (processingConfig.editableUpdate) await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines?drop=true`, [])

        // We are waiting for the dataset to finish processing.
        await ws.waitForJournal(dataset.id, 'finalize-end')
      } else {
        await log.warning(`Les schémas du jeu de données ${dataset.title} et de la couche ${idLayer} ne sont pas compatibles`)
        await log.info('')
        continue
      }
    }

    // Data update
    if (processingConfig.editableUpdate) {
      idStream += 1
      await streamLayerToDataset(idStream, tmpFile, layersFieldList[idLayer].name, layersFieldList[idLayer].featureCount, dataset.id, axios, log, () => shouldBeStopped, dataset.title)
    } else {
      const tmpFileGeoJSON = await createTmpFile(tmpDir, tmpFile, layersFieldList[idLayer].name, log, () => shouldBeStopped)
      if (!tmpFileGeoJSON) return

      formData.append('file', await fs.createReadStream(tmpFileGeoJSON), { filename: path.parse(tmpFileGeoJSON).base })
      formData.getLength = util.promisify(formData.getLength)
      const contentLength = await formData.getLength()
      await log.info(`Chargement de ${formatBytes(contentLength!)}`)

      if (shouldBeStopped) return

      await axios({
        method: 'post',
        url: `api/v1/datasets/${dataset.id}`,
        data: formData,
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        headers: { ...formData.getHeaders(), 'content-length': contentLength }
      })
    }

    if (shouldBeStopped) return
    // We are waiting for the dataset to finish processing.
    await ws.waitForJournal(dataset.id, 'finalize-end')

    await log.info('Mise à jour complète')
    await log.info('')
  }
}
