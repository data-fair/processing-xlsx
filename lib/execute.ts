import type { RunFunction } from '@data-fair/lib-common-types/processings.js'
import type { ProcessingConfig } from '#types/processingConfig/index.ts'
import { formatBytes } from '@data-fair/lib-utils/format/bytes.js'

import util from 'util'
import fs from 'fs-extra'
import path from 'path'
import Excel from 'exceljs'
import FormData from 'form-data'

import type { XlsxProcessingContext } from './context.ts'
import { fetchHTTP } from './fetch.ts'
import { createTmpFile } from './tmp-file.ts'
import { runCommand } from './spawn-process.ts'

/**
 * Allows for a requested program shutdown to be scheduled.
 */
let shouldBeStopped = false

export const stop: () => Promise<void> = async () => { shouldBeStopped = true }

type SheetsList = {
  [idSheet: number]: { name: string, featureCount: number }
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
  const sheetsList = await extraction(context, tmpFile)

  if (shouldBeStopped) return
  if (!sheetsList) return

  if (processingConfig.datasetMode === 'create') {
    const updateConfig = await createDatasets(context, sheetsList, tmpFile)

    if (updateConfig?.length) await patchConfig({ datasetMode: 'update', datasets: updateConfig })
  } else if (processingConfig.datasetMode === 'update') {
    await updateDatasets(context, sheetsList, tmpFile)
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
 * Allows you to retrieve the sheets of a file and organize their structure
 * @param log       Log system that is displayed on the user interface
 * @param tmpFile   Full path of the file to be processed
 * @returns Dictionary of available sheet structures (id: {name, fields, featureCount})
 */
const extraction = async ({ log }: XlsxProcessingContext, tmpFile : string) => {
  await log.step('Récupération de la structure des données')

  // Display sheets
  const workbook = new Excel.Workbook()
  await workbook.xlsx.readFile(tmpFile)

  const sheetsList: SheetsList = []

  for (const sheet of workbook.worksheets) {
    // await log.info(`${sheet.columns}`)

    if (sheet.columnCount <= 0) {
      await log.warning(`Feuille ${sheet.id} - ${sheet.name} - Pas d'attributs, INUTILISABLE`)
    } else {
      await log.info(`Feuille ${sheet.id} - ${sheet.name} - ${sheet.actualRowCount - 1} lignes`)
      sheetsList[sheet.id] = { name: sheet.name, featureCount: sheet.actualRowCount - 1 }
    }
  }

  return sheetsList
}

/**
 * Allows you to create the requested sheet datasets
 * @param processingConfig  Processing configuration, obtained from the form data (processing-config-schema.json)
 * @param axios             Server for API requests
 * @param tmpDir            Directory where to download temporary files
 * @param log               Log system that is displayed on the user interface
 * @param ws                Data Fair's Websocket allows retrieving the dataset response.
 * @param sheetsList   Dictionary containing the structure of the file's sheets (id: {name, fields, featureCount})
 * @param tmpFile           Full path of the file to be processed
 * @returns   A list of objects associating sheets and datasets, or nothing at all to stop the program
 */
const createDatasets = async ({ processingConfig, axios, tmpDir, log, ws } : XlsxProcessingContext, sheetsList: SheetsList, tmpFile: string) => {
  await log.step('Construction des jeux de données')

  // If there are no sheets to extract, we stop here to simplify the display of logs on the interface.
  if (!processingConfig.idsSheets || processingConfig.idsSheets.length <= 0) {
    await log.info('Pas de feuilles renseignées')
    return
  }

  const updateConfig = []

  for (const idSheet of processingConfig.idsSheets) {
    if (shouldBeStopped) return

    if (!(idSheet in sheetsList)) {
      await log.warning(`La feuille ${idSheet} n'est pas présente dans les feuilles disponibles`)
    } else {
      await log.info(`Création du jeu de données pour la feuille ${idSheet} - ${sheetsList[idSheet].name}`)

      const tmpFileXLSX = await createTmpFile(tmpDir, tmpFile, sheetsList[idSheet].name, log, () => shouldBeStopped)
      if (!tmpFileXLSX) return

      const formData = new FormData()
      formData.append('title', `${processingConfig.dataset.prefix}-${sheetsList[idSheet].name}`)
      formData.append('file', await fs.createReadStream(tmpFileXLSX), { filename: path.parse(tmpFileXLSX).base })
      formData.getLength = util.promisify(formData.getLength)
      const contentLength = await formData.getLength()
      await log.info(`Chargement de ${formatBytes(contentLength!)}`)

      if (shouldBeStopped) return

      const dataset = (await axios({
        method: 'post',
        url: 'api/v1/datasets',
        data: formData,
        maxContentLength: Infinity,
        maxBodyLength: Infinity,
        headers: { ...formData.getHeaders(), 'content-length': contentLength }
      })).data
      await log.info(`   Jeu de données créé, id="${dataset.id}", titre="${dataset.title}"`)

      if (shouldBeStopped) return

      // We are waiting for the dataset to finish processing.
      await ws.waitForJournal(dataset.id, 'finalize-end')
      await log.info('Jeu de données complet')

      const datasetObject = { id: dataset.id, href: dataset.href, title: dataset.title }
      const updateObject = { dataset: datasetObject, idSheet }
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
 * @param sheetsList   Dictionary containing the structure of the file's sheets (id: {name, fields, featureCount})
 * @param tmpFile           Full path of the file to be processed
 * @returns   Returns nothing, used to stop the program
 */
const updateDatasets = async ({ processingConfig, axios, tmpDir, log, ws } : XlsxProcessingContext, sheetsList: SheetsList, tmpFile: string) => {
  await log.step('Mise à jour des jeux de données')

  // If there are no updates to extract, we stop here to simplify the display of logs on the interface.
  if (!processingConfig.datasets || processingConfig.datasets.length <= 0) {
    await log.info('Pas de mises à jour renseignées')
    return
  }

  // ---------------------------------
  // SECURITY (normally not necessary): we verify that we have a file dataset
  // ---------------------------------

  // We add size=10000 to ensure that all datasets are retrieved (12 by default)
  const datasets = (await axios.get('api/v1/datasets/?size=10000&file=true')).data.results
  const datasetsIds = new Set<string>(datasets.map(d => d.id))

  // We process each dataset to be updated
  for (const update of processingConfig.datasets) {
    if (shouldBeStopped) return

    const dataset = update.dataset
    const idSheet = update.idSheet
    const formData = new FormData()

    await log.info(`Mise à jour du jeu ${dataset.title} avec la feuille ${idSheet}`)

    // Check if the sheet is available
    if (!(idSheet in sheetsList)) {
      await log.warning(`La feuille ${idSheet} n'est pas présente dans les feuilles disponibles`)
      await log.info('')
      continue
    }

    // Check if the correct update operation can be performed, to avoid permission errors
    if (!(datasetsIds.has(dataset.id))) {
      await log.warning(`Le jeu de données ${dataset.title} n'est pas de type fichier`)
      await log.info('')
      continue
    }

    if (shouldBeStopped) return

    if (update.forceUpdate) await log.info('Mise à jour forcée du schéma')

    // Data update
    const tmpFileXLSX = await createTmpFile(tmpDir, tmpFile, sheetsList[idSheet].name, log, () => shouldBeStopped)
    if (!tmpFileXLSX) return

    formData.append('file', await fs.createReadStream(tmpFileXLSX), { filename: path.parse(tmpFileXLSX).base })
    formData.getLength = util.promisify(formData.getLength)
    const contentLength = await formData.getLength()
    await log.info(`Chargement de ${formatBytes(contentLength!)}`)

    if (shouldBeStopped) return

    await log.info(`api/v1/datasets/${dataset.id}${update.forceUpdate ? '' : '?draft=true'}`)
    await axios({
      method: 'post',
      url: `api/v1/datasets/${dataset.id}${update.forceUpdate ? '' : '?draft=true'}`,
      data: formData,
      maxContentLength: Infinity,
      maxBodyLength: Infinity,
      headers: { ...formData.getHeaders(), 'content-length': contentLength }
    })

    if (shouldBeStopped) return

    // We are waiting for the dataset to finish processing.
    const journal = await ws.waitForJournal(dataset.id, 'finalize-end')

    // At the end of the update, if the dataset is in draft mode, it means there was a schema compatibility issue.
    if (journal.draft !== undefined || journal.draft) {
      await log.warning('Les schémas ne sont pas compatibles. Votre jeu de données est passé en mode brouillon, à vous de le valider ou non.')
    } else {
      await log.info('Mise à jour complète')
    }

    await log.info('')
  }
}
