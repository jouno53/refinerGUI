snapshot_execution_result <- function(result) {
  list(
    fit = serialize(result$fit, NULL),
    interval_table = serialize(result$interval_table, NULL),
    config = serialize(result$config, NULL),
    metadata = serialize(result$metadata, NULL)
  )
}

expect_execution_result_unchanged <- function(result, snapshot) {
  expect_identical(serialize(result$fit, NULL), snapshot$fit)
  expect_identical(serialize(result$interval_table, NULL), snapshot$interval_table)
  expect_identical(serialize(result$config, NULL), snapshot$config)
  expect_identical(serialize(result$metadata, NULL), snapshot$metadata)
}

create_default_execution_result <- function(config = default_execution_config()) {
  source_data <- load_sample_dataset("testcase4")
  ingest <- prepare_ingest_result(source_data)

  expect_true(isTRUE(ingest$ready))

  result <- run_refiner_estimation(ingest$normalized_values, config)

  expect_s3_class(result, "refiner_execution_result")
  expect_true(isTRUE(result$success))
  result
}

test_that("display config validation does not mutate saved execution result", {
  result <- create_default_execution_result()
  before <- snapshot_execution_result(result)

  display_configs <- list(
    default_display_config(result$config),
    utils::modifyList(default_display_config(result$config), list(Nhist = 40L)),
    utils::modifyList(default_display_config(result$config), list(showMargin = FALSE)),
    utils::modifyList(default_display_config(result$config), list(showPathol = TRUE)),
    utils::modifyList(default_display_config(result$config), list(showValue = FALSE)),
    utils::modifyList(default_display_config(result$config), list(uncertaintyRegion = "uncertaintyMargin")),
    utils::modifyList(default_display_config(result$config), list(colScheme = "green")),
    utils::modifyList(default_display_config(result$config), list(colScheme = "blue")),
    utils::modifyList(default_display_config(result$config), list(pointEst = "fullDataEst")),
    utils::modifyList(default_display_config(result$config), list(Scale = "original"))
  )

  for (display_config in display_configs) {
    expect_true(isTRUE(validate_display_config(display_config, result$config)$ok))
    expect_type(assert_valid_display_config(display_config, result$config), "list")
    expect_execution_result_unchanged(result, before)
  }
})

test_that("bootstrap-dependent display validation follows execution config", {
  non_bootstrap_config <- default_execution_config()

  median_preflight <- validate_display_config(
    utils::modifyList(default_display_config(non_bootstrap_config), list(pointEst = "medianBS")),
    non_bootstrap_config
  )
  expect_false(isTRUE(median_preflight$ok))
  expect_match(paste(median_preflight$errors, collapse = " "), "medianBS requires")

  bootstrap_ci_preflight <- validate_display_config(
    utils::modifyList(default_display_config(non_bootstrap_config), list(uncertaintyRegion = "bootstrapCI")),
    non_bootstrap_config
  )
  expect_false(isTRUE(bootstrap_ci_preflight$ok))
  expect_match(paste(bootstrap_ci_preflight$errors, collapse = " "), "bootstrapCI requires")

  bootstrap_config <- utils::modifyList(default_execution_config(), list(NBootstrap = 20L, pointEst = "medianBS"))
  expect_lte(assert_valid_execution_config(bootstrap_config)$NBootstrap, 50L)

  median_bootstrap_preflight <- validate_display_config(
    utils::modifyList(default_display_config(bootstrap_config), list(pointEst = "medianBS")),
    bootstrap_config
  )
  expect_true(isTRUE(median_bootstrap_preflight$ok))

  bootstrap_ci_valid_preflight <- validate_display_config(
    utils::modifyList(default_display_config(bootstrap_config), list(uncertaintyRegion = "bootstrapCI")),
    bootstrap_config
  )
  expect_true(isTRUE(bootstrap_ci_valid_preflight$ok))
})

test_that("extract_reference_interval does not mutate fit or stored interval table", {
  result <- create_default_execution_result()
  before <- snapshot_execution_result(result)
  alternate_options <- utils::modifyList(result$config, list(RIperc = c(0.05, 0.95), Scale = "original"))

  interval <- extract_reference_interval(result$fit, alternate_options)

  expect_s3_class(interval, "data.frame")
  expect_false(identical(serialize(interval, NULL), before$interval_table))
  expect_execution_result_unchanged(result, before)
})

test_that("capture_refiner_summary does not mutate fit or execution result", {
  result <- create_default_execution_result()
  before <- snapshot_execution_result(result)
  display_options <- utils::modifyList(default_display_config(result$config), list(showValue = FALSE))

  summary <- capture_refiner_summary(result$fit, display_options, result$config)

  expect_type(summary, "character")
  expect_gt(length(summary), 0)
  expect_execution_result_unchanged(result, before)
})

test_that("build_refiner_plot does not mutate fit or execution result", {
  result <- create_default_execution_result()
  before <- snapshot_execution_result(result)
  plot_options <- utils::modifyList(default_display_config(result$config), list(
    showPathol = TRUE,
    showValue = TRUE,
    colScheme = "blue"
  ))
  png_file <- tempfile(fileext = ".png")
  device_open <- FALSE

  grDevices::png(png_file)
  device_open <- TRUE
  on.exit({
    if (isTRUE(device_open)) {
      grDevices::dev.off()
    }
    unlink(png_file)
  }, add = TRUE)

  returned_config <- build_refiner_plot(result$fit, plot_options, result$config)
  grDevices::dev.off()
  device_open <- FALSE

  expect_identical(returned_config, assert_valid_display_config(plot_options, result$config))
  expect_true(file.exists(png_file))
  expect_gt(file.info(png_file)$size, 0)
  expect_execution_result_unchanged(result, before)
})

test_that("display helper tests exercise saved fit only rather than refitting", {
  # Non-invasive refit guard: these display checks intentionally call only display
  # helpers with an already-created result$fit. They do not call run_refiner_estimation()
  # or refineR::findRI(); immutability snapshots above guard the stored result.
  result <- create_default_execution_result()
  before <- snapshot_execution_result(result)
  display_options <- default_display_config(result$config)

  expect_s3_class(extract_reference_interval(result$fit, display_options), "data.frame")
  expect_type(capture_refiner_summary(result$fit, display_options, result$config), "character")
  expect_execution_result_unchanged(result, before)
})
