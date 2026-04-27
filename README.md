# <img alt="Data FAIR logo" src="https://cdn.jsdelivr.net/gh/data-fair/data-fair@master/ui/public/assets/logo.svg" width="30"> @data-fair/processing-xlsx

A plugin that allows the creation and management of datasets from Excel files or zip files containing them.

## Features

- **List sheets** — Lists the sheets and corresponding information in your file
- **Dataset management** — Create or update a file dataset from the desired sheets, configurable from the processing parameters.
- **Graceful stop** — honours the stop signal from the platform and exits cleanly mid-run; optionally, the stop can be ignored to test forced termination after timeout.

## Configuration

The plugin configurations change depending on the datasetMode to be applied. There are three of them:
- `list` to list the different sheets
- `create` to create a new dataset
- `update` to target an existing one

Overall, only the URL in the settings tab remains common, representing a stable URL from which the data file is downloaded (this is the only possible option, there is no repository).

### list

For this mode, you only need to enter the URL.

### create

| Tab | Field | Description |
| --- | ----- | ----------- |
| Liste de jeux de données | `prefix` | This corresponds to the title of the datasets to be created. The datasets are named according to this prefix and the name of the corresponding sheet |
| Paramètres | `idsLayers` | Identifier of the sheets to extract to create datasets |

### update

| Tab | Field | Description |
| --- | ----- | ----------- |
| Liste de jeux de données | `datasets` | List of datasets to be updated, taking into account the sheet number and the schema update forcing |
| Liste de jeux de données - Jeux à mettre à jour | `dataset` | Name of the dataset to update, selectable from the list of available datasets |
| Liste de jeux de données - Jeux à mettre à jour | `idLayer` | Layer number used to update the corresponding dataset |
| Liste de jeux de données - Jeux à mettre à jour | `forceUpdate` | Indicates whether the scheme update should also be forced |