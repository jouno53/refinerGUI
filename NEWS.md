# refinerGUI 0.3.0

## Desktop UI redesign

- Redesigned the main Shiny workspace for desktop clinical laboratory use with a denser Data, Parameters, Run, and Results flow.
- Rebalanced the results review area so summaries, interval tables, plot controls, and plots can be reviewed together without tab switching.
- Reduced explanatory copy and tightened spacing while preserving existing refineR execution, grouping, checkpoint, settings, and display behavior.
- Converted startup health rendering from inline styles to reusable semantic status classes.

# refinerGUI 0.2.1

## Plot resizing

- Added a resizable results plot viewport so copied plot images use the displayed aspect ratio.
- Debounced plot resize updates so Shiny redraws the refineR plot after resizing settles instead of during every intermediate resize frame.

# refinerGUI 0.2.0

## Validation and CI

- Added a formal mandatory testthat validation suite for the wrapper contract.
- Added direct refineR equivalence tests for overall and grouped execution paths.
- Added ingestion, grouping, display/execution separation, and checkpoint compatibility validation.
- Documented the tested wrapper contract and explicit refinerGUI app defaults.
- Added a GitHub Actions workflow for the full mandatory validation suite.

# refinerGUI 0.1.3

## UI workflow polish

- Added workflow step indicators to the main app header and each primary module.
- Added compact data quality, grouping, parameter, and run-readiness summaries to make blockers easier to scan.
- Moved plot display controls into the plot workspace and cleaned up visible label encoding artifacts.

# refinerGUI 0.1.2

## Age bracket boundary handling

- Updated age bracketing so repeated contiguous boundaries assign the boundary value to the earlier bracket.
- Added parsed age-band rules to the grouping status display so users can see the effective interval logic.
- Clarified age-band UI guidance for examples such as `0-18`, `18-30`, and `30+`.

# refinerGUI 0.1.1

## Launcher dependency restore

- Updated the Windows launcher to restore the `renv` package library before app startup so fresh source downloads can install required R packages automatically.
- Clarified first-launch dependency restoration behavior in the README.

# refinerGUI 0.1.0

## Initial public repository baseline

- Established the stable R Shiny application baseline for local refineR analysis.
- Added semantic version tracking with version `0.1.0`.
- Added reproducible dependency management with `renv`.
- Added repository documentation, GPL-3 licensing, and release workflow notes.
- Preserved the current roadmap-driven architecture for ingestion, wrapper, reactive, and UI layers.
