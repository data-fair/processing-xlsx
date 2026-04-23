import fs from 'fs-extra'
import path from 'path'

import { runCommand } from './spawn-process.ts'
import type { GpkgProcessingContext } from './context.ts'

/**
 * Allows you to create a temporary .geojson file from a layer in the data file, to be sent to create a file dataset.
 * @param dir         Directory where to store the file
 * @param tmpFile     Name of the temporary file containing the original data (multi-layered gpkg)
 * @param layerName   Name of the layer to be extracted
 * @param log         Log system that is displayed on the user interface
 * @param isStopped   Function allowing the program to stop if requested
 * @returns   Name of the temporary file created to send
 */
export const createTmpFile = async (dir : string, tmpFile : string, layerName : string, log: GpkgProcessingContext['log'], isStopped: () => boolean) => {
  const tmpFileGeoJSON = path.join(dir, `${layerName}.geojson`)

  // If there are two updates with the same layer, it is only downloaded once.
  if (!(await fs.pathExists(tmpFileGeoJSON))) {
    await log.info('Création du fichier temporaire')
    if (isStopped()) return

    await runCommand('ogr2ogr', ['-f', 'GeoJSON', '-lco', 'RFC7946=YES', '-t_srs', 'EPSG:4326', tmpFileGeoJSON, tmpFile, layerName])
  }

  return tmpFileGeoJSON
}
