# Tests

This directory contains the mandatory local validation suite for the refinerGUI wrapper contract.

Run the suite from the repository root with the project R installation:

```powershell
Rscript tests/testthat.R
```

The suite contains no optional skips, environment-variable gates, or CRAN-only exclusions. All tests run by default.

The tests cover:

- contract smoke loading without launching Shiny
- execution config defaults and validation
- ingestion and analyte normalization
- group planning and normalized row-index semantics
- direct refineR equivalence for overall and grouped execution
- display/execution separation
- grouped checkpoint compatibility and restoration

Bootstrap validation cases are capped at `NBootstrap <= 50`. Full validation can take several minutes because it runs real refineR equivalence tests rather than checking only object presence.

The older roadmap gap-closure harness remains available at:

```powershell
Rscript scripts/verify_gap_closure.R
```
