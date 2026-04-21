import type { RunFunction, LogFunctions, DataFairWsClient } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import type { AxiosInstance } from 'axios'

import { spawn } from 'child_process'
import fs from 'fs-extra'
import * as path from 'path'

import { fetchHTTP } from './fetch.ts'
import { streamLayerToDataset } from './stream-layer.ts'

/**
 * Allows for a requested program shutdown to be scheduled.
 */
let shouldBeStopped = false

export const stop: () => Promise<void> = async () => { shouldBeStopped = true }
/**
 * Input function, allows data processing to begin
 * @param context Context of the request
 */
export const run: RunFunction<ProcessingConfig> = async (context) => {
  shouldBeStopped = false

  // Retrieving the contextual elements necessary for processing
  const { processingConfig, processingId, secrets, tmpDir, axios, log, patchConfig, ws } = context
  const tmpFile = await download(processingConfig, secrets, tmpDir, axios, log)

  if (shouldBeStopped) return
  const layersFieldList = await extraction(tmpFile!, log)

  if (shouldBeStopped) return

  if (processingConfig.datasetMode === 'create') {
    const updateConfig = await createDatasets(processingConfig, processingId, axios, layersFieldList, tmpFile, log)
    if (updateConfig && updateConfig.length > 0) await patchConfig({ datasetMode: 'update', datasets: updateConfig })
  } else if (processingConfig.datasetMode === 'update') {
    await updateDatasets(processingConfig, axios, layersFieldList!, tmpFile!, log, ws)
  } else {
    await patchConfig({ datasetMode: 'create', dataset: { prefix: '' } })
  }
}

/**
 * Allows you to download the file and place it in a temporary folder for later processing.
 * We only process .zip and .xlsx formats; any other format will result in an error.
 * @param processingConfig  Processing configuration, obtained from the form data (processing-config-schema.json)
 * @param secrets           Sensitive information if necessary (such as a password, for example)
 * @param dir               Directory where to download the file
 * @param axios             Server for API requests
 * @param log               Log system that is displayed on the user interface
 * @returns Full path of the file to be processed
 */
const download = async (processingConfig : ProcessingConfig, secrets, dir : string, axios : AxiosInstance, log : LogFunctions) => {
  await fs.ensureDir(dir)

  await log.step('Téléchargement du fichier')
  let tmpFile = path.join(dir, 'file')
  await fs.ensureFile(tmpFile)
  if (shouldBeStopped) return

  let filename = decodeURIComponent(path.parse(processingConfig.url).base)
  if (shouldBeStopped) return

  filename = await fetchHTTP(processingConfig, secrets, tmpFile, axios) || filename
  if (shouldBeStopped) return

  // Try to prevent weird bug with NFS by forcing syncing file before reading it
  const fd = await fs.open(tmpFile, 'r')
  await fs.fsync(fd)
  await fs.close(fd)
  await log.info(`Le fichier a été téléchargé (${filename})`)
  if (shouldBeStopped) return

  let xlsxFilename

  // Check the file format
  if (filename.endsWith('.zip')) {
    await log.info(`Dézippage du fichier ${filename}`)

    // Unzip
    const proc = spawn('unzip', [tmpFile, '-d', `${tmpFile}-dezip`])
    let result = ''
    for await (const chunk of proc.stdout) {
      result += chunk.toString()
    }

    if (result.length <= 0) {
      throw new Error('Erreur au niveau du dézippage')
    }

    // We are looking for the .xlsx files contained in the .zip file.
    const filesxlsx: string[] = []
    await fs.readdir(`${tmpFile}-dezip`)
      .then((files) => {
        for (const file of files) {
          if (file.endsWith('.xlsx')) {
            filesxlsx.push(`${tmpFile}-dezip/${file}`)
          }
        }
      })

    const nbFichiers = filesxlsx.length
    if (shouldBeStopped) return

    if (nbFichiers <= 0) {
      throw new Error('Il n\'y a pas de fichiers .xlsx à traiter dans ce zip.')
    } else {
      // We keep the first .xlsx file we find, we ignore the others
      const tabSplit = filesxlsx[0].split('/')
      xlsxFilename = tabSplit[tabSplit.length - 1]
      tmpFile = filesxlsx[0]
    }
  } else if (filename.endsWith('.xlsx')) {
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
 * @param tmpFile   Full path of the file to be processed
 * @param log       Log system that is displayed on the user interface
 * @returns Dictionary of available layer structures (id: {name, fields, featureCount})
 */
const extraction = async (tmpFile : string, log : LogFunctions) => {
  await log.step('Récupération de la structure des données')

  // Display layers
  const proc = spawn('ogrinfo', ['-json', tmpFile])
  let result = ''
  if (shouldBeStopped) return

  for await (const chunk of proc.stdout) {
    if (shouldBeStopped) return
    result += chunk.toString()
  }

  const jsonStructure = await JSON.parse(result)
  if (shouldBeStopped) return

  const layers = jsonStructure.layers
  const layersFieldList: { [idLayer: number]: { name: string, fields: any[], featureCount: number } } = []

  for (let i = 0; i < layers.length; i++) {
    for (let j = 0; j < layers[i].fields.length; j++) {
      if (shouldBeStopped) return
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
        type: typeCorrect
      }
      if (!layers[i].fields[j].type) {
        throw new Error(`Pas de type pour ${layers[i].fields[j].name}`)
      }
    }

    // If there are no attributes (columns), it is considered unnecessary to retrieve the layer.
    if (layers[i].fields.length <= 0) {
      await log.warning(`Couche ${i + 1} - ${layers[i].name} - Pas d'attributs, INUTILISABLE`)
    } else {
      await log.info(`Couche ${i + 1} - ${layers[i].name} - ${layers[i].featureCount} lignes`)
      layersFieldList[i + 1] = { name: layers[i].name, fields: layers[i].fields, featureCount: layers[i].featureCount }
    }
  }

  return layersFieldList
}

/**
 * Allows you to create the requested layer datasets
 * @param processingConfig  Processing configuration, obtained from the form data (processing-config-schema.json)
 * @param processingId      Identifier of the processing currently in use
 * @param axios             Server for API requests
 * @param layersFieldList   Dictionary containing the structure of the file's layers (id: {name, fields, featureCount})
 * @param tmpFile           Full path of the file to be processed
 * @param log               Log system that is displayed on the user interface
 * @returns   A list of objects associating layers and datasets, or nothing at all to stop the program
 */
const createDatasets = async (processingConfig : ProcessingConfig, processingId, axios : AxiosInstance, layersFieldList: { [idLayer: number]: { name: string, fields: any[], featureCount: number } }, tmpFile: string, log : LogFunctions) => {
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

      // Display names and types of the fields
      for (const field of layersFieldList[idLayer].fields) {
        await log.debug(`   Nom : ${field.key} - Type : ${field.type}`)
      }

      // Create the dataset, empty
      const fields = layersFieldList[idLayer].fields
      const dataset = (await axios.post('api/v1/datasets', {
        title: `${processingConfig.dataset.prefix}-${layersFieldList[idLayer].name}`,
        description: '',
        isRest: true,
        schema: fields,
        extras: { processingId }
      })).data
      await log.info(`   Jeu de données créé, id="${dataset.id}", titre="${dataset.title}"`)

      const datasetObject = { id: dataset.id, href: dataset.href, title: dataset.title }
      const updateObject = { dataset: datasetObject, idLayer }
      updateConfig.push(updateObject)

      if (shouldBeStopped) return
      // Dataset population
      idStream += 1
      await streamLayerToDataset(idStream, tmpFile, layersFieldList[idLayer].name, layersFieldList[idLayer].featureCount, dataset.id, axios, log, () => shouldBeStopped)

      await log.info('Jeu de données complet')
    }
    await log.info('')
  }
  return updateConfig
}

/**
 * Allows updating a dataset, either by force (schema reset) or by non-force (data replacement).
 * @param processingConfig  Processing configuration, obtained from the form data (processing-config-schema.json)
 * @param axios             Server for API requests
 * @param layersFieldList   Dictionary containing the structure of the file's layers (id: {name, fields, featureCount})
 * @param tmpFile           Full path of the file to be processed
 * @param log               Log system that is displayed on the user interface
 * @returns   Returns nothing, used to stop the program
 */
const updateDatasets = async (processingConfig : ProcessingConfig, axios : AxiosInstance, layersFieldList: { [idLayer: number]: { name: string, fields: any[], featureCount: number } }, tmpFile: string, log : LogFunctions, ws : DataFairWsClient) => {
  await log.step('Mise à jour des jeux de données')

  // If there are no updates to extract, we stop here to simplify the display of logs on the interface.
  if (!processingConfig.datasets || processingConfig.datasets.length <= 0) {
    await log.info('Pas de mise à jour renseignées')
    return
  }

  let idStream = 0

  // We process each dataset to be updated
  for (const update of processingConfig.datasets) {
    if (shouldBeStopped) return

    const dataset = update.dataset
    const idLayer = update.idLayer
    await log.info(`Mise à jour du jeu ${dataset.title} avec la couche ${idLayer}`)

    // Check if the layer is available
    if (!(idLayer in layersFieldList)) {
      await log.warning(`La couche ${idLayer} n'est pas présente dans les couches disponibles`)
      await log.info('')
      continue
    }

    // Retrieving the dataset schema
    const datasetSchema = (await axios.get(`api/v1/datasets/${dataset.id}`)).data.schema
    if (shouldBeStopped) return

    if (update.forceUpdate) {
      await log.info('Mise à jour forcée du schéma')

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
    } else {
      try {
        // Check if the schemas match.
        await log.info('Vérification de la compatibilité des schémas')

        for (const field of layersFieldList[idLayer].fields) {
          if (shouldBeStopped) return
          let find = false
          for (const datasetField of datasetSchema) {
            if (shouldBeStopped) return
            if (datasetField.name === field.name && datasetField.type === field.type) {
              find = true
              break
            }
          }
          if (!find) {
            throw new Error('Non compatibilité des schémas')
          }
        }
        for (const datasetField of datasetSchema) {
          if (shouldBeStopped) return
          let find = false
          for (const field of layersFieldList[idLayer].fields) {
            if (shouldBeStopped) return
            if (datasetField.name === field.name && datasetField.type === field.type) {
              find = true
              break
            }
          }
          if (!find) {
            throw new Error('Non compatibilité des schémas')
          }
        }

        // Drop the old data
        if (shouldBeStopped) return
        await axios.post(`api/v1/datasets/${dataset.id}/_bulk_lines?drop=true`, [])
      } catch (err) {
        // Instead of triggering an error, we issue a warning to allow subsequent updates to proceed.
        await log.warning(`Les schémas du jeu de données ${dataset.title} et de la couche ${idLayer} ne sont pas compatibles`)
        await log.info('')
        continue
      }
    }

    // Data update
    idStream += 1
    await streamLayerToDataset(idStream, tmpFile, layersFieldList[idLayer].name, layersFieldList[idLayer].featureCount, dataset.id, axios, log, () => shouldBeStopped, dataset.title)

    await log.info('Mise à jour complète')
    await log.info('')
  }
}
