direct_refiner_interval <- function(values, config) {
  normalized_config <- assert_valid_execution_config(config)
  fit <- refineR::findRI(
    Data = values,
    model = normalized_config$model,
    NBootstrap = normalized_config$NBootstrap,
    seed = normalized_config$seed
  )

  interval <- refineR::getRI(
    fit,
    RIperc = normalized_config$RIperc,
    CIprop = normalized_config$CIprop,
    UMprop = normalized_config$UMprop,
    pointEst = normalized_config$pointEst,
    Scale = normalized_config$Scale
  )

  list(fit = fit, interval = interval, config = normalized_config)
}

expect_interval_table_equal <- function(actual, expected, tolerance = 1e-8) {
  expect_s3_class(actual, "data.frame")
  expect_s3_class(expected, "data.frame")
  expect_identical(names(actual), names(expected))
  expect_identical(nrow(actual), nrow(expected))

  for (column_name in names(expected)) {
    if (is.numeric(expected[[column_name]])) {
      expect_equal(actual[[column_name]], expected[[column_name]], tolerance = tolerance)
    } else {
      expect_identical(actual[[column_name]], expected[[column_name]])
    }
  }
}

load_default_sample_values <- function() {
  source_data <- load_sample_dataset("testcase4")
  ingest <- prepare_ingest_result(source_data)

  expect_true(isTRUE(ingest$ready))
  ingest$normalized_values
}

test_that("packaged sample default wrapper result matches direct refineR interval extraction", {
  values <- load_default_sample_values()
  config <- default_execution_config()
  direct <- direct_refiner_interval(values, config)

  result <- run_refiner_estimation(values, config)

  expect_s3_class(result, "refiner_execution_result")
  expect_true(isTRUE(result$success))
  expect_s3_class(result$fit, "RWDRI")
  expect_interval_table_equal(result$interval_table, direct$interval)
  expect_identical(result$config, assert_valid_execution_config(config))
  expect_identical(result$metadata$analyte_length, length(values))
})

test_that("string-input config normalizes before matching direct refineR output", {
  values <- load_default_sample_values()
  config <- utils::modifyList(default_execution_config(), list(
    NBootstrap = "0",
    seed = "123",
    RIperc = "0.025, 0.975",
    CIprop = "0.95",
    UMprop = "0.90"
  ))
  normalized_config <- assert_valid_execution_config(config)
  direct <- direct_refiner_interval(values, normalized_config)

  result <- run_refiner_estimation(values, config)

  expect_true(isTRUE(result$success))
  expect_identical(result$config, normalized_config)
  expect_interval_table_equal(result$interval_table, direct$interval)
})

test_that("alternate model wrapper output matches direct refineR output", {
  values <- load_default_sample_values()

  fast_config <- utils::modifyList(default_execution_config(), list(model = "modBoxCoxFast"))
  fast_direct <- direct_refiner_interval(values, fast_config)
  fast_result <- run_refiner_estimation(values, fast_config)

  expect_true(isTRUE(fast_result$success))
  expect_interval_table_equal(fast_result$interval_table, fast_direct$interval)

  slow_config <- utils::modifyList(default_execution_config(), list(model = "modBoxCox"))
  slow_direct <- direct_refiner_interval(values, slow_config)
  slow_result <- run_refiner_estimation(values, slow_config)

  expect_true(isTRUE(slow_result$success))
  expect_interval_table_equal(slow_result$interval_table, slow_direct$interval)
})

test_that("alternate interval extraction options match direct getRI output", {
  values <- load_default_sample_values()
  config <- utils::modifyList(default_execution_config(), list(RIperc = c(0.05, 0.95), Scale = "original"))
  direct <- direct_refiner_interval(values, config)

  result <- run_refiner_estimation(values, config)

  expect_true(isTRUE(result$success))
  expect_interval_table_equal(result$interval_table, direct$interval)
})

test_that("bootstrap median point estimate is deterministic against direct refineR output", {
  values <- load_default_sample_values()
  config <- utils::modifyList(default_execution_config(), list(
    NBootstrap = 20L,
    seed = 123L,
    pointEst = "medianBS"
  ))
  expect_lte(assert_valid_execution_config(config)$NBootstrap, 50L)
  direct <- direct_refiner_interval(values, config)

  result <- run_refiner_estimation(values, config)

  expect_true(isTRUE(result$success))
  expect_identical(result$config, assert_valid_execution_config(config))
  expect_interval_table_equal(result$interval_table, direct$interval, tolerance = 1e-8)
})

test_that("missing execution values fail through wrapper validation before refineR equivalence", {
  # This is intentionally wrapper-specific validation: app execution rejects missing
  # analyte values before calling refineR, so there is no direct-call comparison here.
  result <- run_refiner_estimation(c(1, 2, NA, 4), default_execution_config())

  expect_s3_class(result, "refiner_execution_result")
  expect_false(isTRUE(result$success))
  expect_match(result$error_message, "missing analyte values")
  expect_null(result$fit)
  expect_null(result$interval_table)
})
