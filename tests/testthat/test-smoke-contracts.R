test_that("app contract functions load without launching Shiny", {
  expect_true(requireNamespace("refineR", quietly = TRUE))

  expect_true(exists("sample_dataset_choices", mode = "function"))
  expect_identical(sample_dataset_choices(), c("testcase3", "testcase4", "testcase5"))

  expect_true(exists("default_execution_config", mode = "function"))
  execution_config <- default_execution_config()
  expect_type(execution_config, "list")

  execution_preflight <- validate_execution_config(execution_config)
  expect_true(isTRUE(execution_preflight$ok))

  expect_true(exists("default_display_config", mode = "function"))
  display_config <- default_display_config(execution_config)
  expect_type(display_config, "list")

  display_preflight <- validate_display_config(display_config, execution_config)
  expect_true(isTRUE(display_preflight$ok))

  expect_true(exists("run_refiner_estimation", mode = "function"))
  expect_true(exists("extract_reference_interval", mode = "function"))
  expect_true(exists("run_group_refiner_estimation", mode = "function"))
  expect_true(exists("run_grouped_refiner_estimation", mode = "function"))
})

test_that("single-run wrapper executes a packaged sample dataset", {
  source_data <- load_sample_dataset("testcase4")
  ingest <- prepare_ingest_result(source_data)
  config <- default_execution_config()

  result <- run_refiner_estimation(ingest$normalized_values, config)

  expect_s3_class(result, "refiner_execution_result")
  expect_true(isTRUE(result$success))
  expect_false(is.null(result$fit))
  expect_s3_class(result$interval_table, "data.frame")
  expect_identical(result$config, assert_valid_execution_config(config))
})
