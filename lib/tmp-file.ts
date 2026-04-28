import fs from 'fs-extra'
import path from 'path'
import Excel from 'exceljs'

import type { XlsxProcessingContext } from './context.ts'

type NamedWorksheetReader = Excel.stream.xlsx.WorksheetReader & { name: string, destroy: () => void }

/**
 * Allows you to create a temporary .xlsx file from a sheet in the data file, to be sent to create a file dataset.
 * @param dir         Directory where to store the file
 * @param tmpFile     Name of the temporary file containing the original data (multi-sheeted xlsx)
 * @param sheetName   Name of the sheet to be extracted
 * @param log         Log system that is displayed on the user interface
 * @param isStopped   Function allowing the program to stop if requested
 * @returns   Name of the temporary file created to send
 */
export const createTmpFile = async (dir : string, tmpFile : string, sheetName : string, log: XlsxProcessingContext['log'], isStopped: () => boolean) => {
  const tmpFileXLSX = path.join(dir, `${sheetName}.xlsx`)

  if (await fs.pathExists(tmpFileXLSX)) return tmpFileXLSX

  await log.info('Création du fichier temporaire')
  if (isStopped()) return

  // Creating a stream for large files
  const workbookReader = new Excel.stream.xlsx.WorkbookReader(tmpFile, {})
  const workbookWriter = new Excel.stream.xlsx.WorkbookWriter({ filename: tmpFileXLSX })
  const sheetWriter = workbookWriter.addWorksheet(sheetName)

  // Retrieving the lines from the correct sheet
  for await (const worksheetReader of workbookReader) {
    // Try to fix a typing problem in ExcelJS
    const reader = worksheetReader as unknown as NamedWorksheetReader
    if (isStopped()) return
    if (reader.name !== sheetName) {
      continue
    }

    for await (const row of worksheetReader) {
      if (isStopped()) return
      sheetWriter.addRow(row.values).commit()
    }
    break
  }

  if (isStopped()) return

  await sheetWriter.commit()
  await workbookWriter.commit()

  return tmpFileXLSX
}
