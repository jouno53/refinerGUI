# refinerGUI

refinerGUI is a local R Shiny application for estimating reference intervals from real-world data with the [`refineR`](https://cran.r-project.org/package=refineR) package.

The app wraps the core refineR workflow in a desktop Shiny workspace for data intake, parameter selection, optional grouped analysis, checkpointing, reproducible settings, and result review.

refinerGUI is not a reimplementation of the refineR algorithm. It is a Shiny wrapper around refineR that validates local inputs, normalizes app configuration, calls refineR through pure wrapper functions, and presents the saved fit results.

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
4. Review parameter preflight and run-readiness status.
5. Run refineR estimation from the Run panel.
6. Inspect the summary, interval table, plot controls, and plot in the Results workspace.

## refineR Wrapper Contract

refinerGUI calls refineR explicitly through the wrapper layer. For a normalized numeric analyte vector, the execution path is equivalent to:

```r
fit <- refineR::findRI(
  Data = values,
  model = config$model,
  NBootstrap = config$NBootstrap,
  seed = config$seed
)

interval <- refineR::getRI(
  fit,
  RIperc = config$RIperc,
  CIprop = config$CIprop,
  UMprop = config$UMprop,
  pointEst = config$pointEst,
  Scale = config$Scale
)
```

The default refinerGUI execution configuration is:

```r
list(
  model = "BoxCox",
  NBootstrap = 0,
  seed = 123,
  RIperc = c(0.025, 0.975),
  CIprop = 0.95,
  UMprop = 0.90,
  pointEst = "fullDataEst",
  Scale = "original"
)
```

These are explicit refinerGUI defaults. They should not be interpreted as refineR native defaults unless a specific default is verified against the installed refineR package.

## Preprocessing and Grouping

CSV files and packaged refineR sample datasets are loaded locally. The selected analyte column must be numeric. Missing analyte values are removed before refineR execution, and optional sex and age metadata are aligned to the post-NA analyte rows.

Grouped analysis runs one separate refineR fit per planned group. It is not a pooled fit with labels. Group row indices are relative to the normalized analyte vector, not raw CSV row numbers; raw rows are recovered by mapping through the stored usable-row indices. Grouped bootstrap runs derive group-specific seeds from the base seed so each subgroup is reproducible.

Sex grouping requires an explicit raw-value mapping, and age grouping requires explicit age-band definitions. Sex-by-age grouping combines those planned group definitions.

## Display and Checkpoints

Display controls operate on a saved fit. They may change summaries, interval extraction, and plots, but they must not rerun `refineR::findRI()` or mutate the saved fit, interval table, execution config, or execution metadata. This separation is covered by the validation suite.

Checkpoints are intended for grouped CSV runs. Checkpoint compatibility checks include the run signature, config signature, grouping signature, planned groups, and source fingerprint. Incompatible checkpoints are rejected rather than silently reused.

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
