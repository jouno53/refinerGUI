# refinerGUI

refinerGUI is a local R Shiny application for estimating reference intervals from real-world data with the [`refineR`](https://cran.r-project.org/package=refineR) package.

The app wraps the core refineR workflow in a guided interface for data intake, parameter selection, optional grouped analysis, checkpointing, reproducible settings, and result review.

## Purpose

refineR implements indirect reference interval estimation from routine laboratory or similar real-world measurements. refinerGUI makes that workflow easier to run locally by providing:

- CSV upload and packaged refineR sample dataset intake.
- Numeric analyte validation before analysis.
- Documented refineR model, interval, and display controls.
- Optional sex and age grouping.
- Checkpoint support for longer grouped runs.
- Import and export of reproducible experiment settings.

## Privacy

refinerGUI runs locally in R Shiny. Uploaded CSV files are read by the local R session and are not sent to a hosted service by this application.

## Installation

Install R 4.6.0, then clone or download this repository from GitHub.

From the project directory, restore the project library with `renv`:

```bash
cd refinerGUI
Rscript -e "renv::restore()"
```

If `Rscript` is not on your PATH, run the same command from an R console in the project directory:

```r
renv::restore()
```

## Usage

Start the app from the project root:

```bash
Rscript -e "shiny::runApp('.', host = '127.0.0.1', port = 7448)"
```

Windows users can also use the included launcher:

```powershell
.\run_app.cmd
```

The first launch restores the R package library from `renv.lock`. That step requires internet access and may take several minutes.

Basic workflow:

1. Choose a packaged refineR sample dataset or upload a CSV.
2. Select the numeric analyte column.
3. Optionally map sex and age metadata for grouped analysis.
4. Review parameter preflight status.
5. Run refineR estimation.
6. Inspect the summary, interval table, and plot.

## Dependency Management

This project uses `renv` for reproducible dependency management. The committed `renv.lock` file records the R package versions used by the stable app state.

To reproduce the project library, run `renv::restore()` from the project root.

## Requirements

- R 4.6.0
- A local browser
- Internet access for the initial `renv::restore()` dependency installation

## License

refinerGUI is licensed under GPL-3.

Copyright (C) 2026 refinerGUI contributors.
