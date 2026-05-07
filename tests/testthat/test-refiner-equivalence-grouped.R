direct_refiner_interval_grouped <- function(values, config) {
  validated <- assert_valid_execution_config(config)
  fit <- refineR::findRI(
    Data = values,
    model = validated$model,
    NBootstrap = validated$NBootstrap,
    seed = validated$seed
  )
  interval <- refineR::getRI(
    fit,
    RIperc = validated$RIperc,
    CIprop = validated$CIprop,
    UMprop = validated$UMprop,
    pointEst = validated$pointEst,
    Scale = validated$Scale
  )

  list(fit = fit, interval_table = interval, config = validated)
}

expect_group_interval_table_equal <- function(actual, expected, tolerance = 1e-8) {
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

load_grouped_execution_ingest <- function(path = "fixtures/grouped_execution_valid.csv") {
  source_data <- load_local_csv(test_path("../../", path))
  ingest <- prepare_ingest_result(
    source_data,
    column_name = "analyte",
    sex_column = "sex_code",
    age_column = "age_years"
  )

  expect_true(isTRUE(ingest$ready))
  expect_true(isTRUE(ingest$metadata_ready))
  ingest
}

plan_grouped_fixture <- function(ingest, mode, age_band_text = "0-17\n18-64\n65+") {
  plan_grouped_analysis(
    analyte_values = ingest$normalized_values,
    grouping_mode = mode,
    sex_values = if (mode %in% c("sex", "sex_age")) ingest$sex_values else NULL,
    sex_mapping_text = "female = F\nmale = M",
    age_values = if (mode %in% c("age", "sex_age")) ingest$age_values else NULL,
    age_band_text = age_band_text
  )
}

expect_group_result_matches_direct <- function(ingest, group_plan, group_result, group_index, config, tolerance = 1e-8) {
  group_label <- group_plan$groups$group_label[[group_index]]
  expected_indices <- extract_group_row_indices(ingest, group_plan, group_label)
  group_seed <- derive_group_seed(config$seed, group_index)
  direct_config <- utils::modifyList(config, list(seed = group_seed))
  direct <- direct_refiner_interval_grouped(ingest$normalized_values[expected_indices], direct_config)

  expect_identical(group_result$group_label, group_label)
  expect_identical(group_result$row_indices, expected_indices)
  expect_identical(group_result$status, "completed")
  expect_true(isTRUE(group_result$execution_result$success))
  expect_identical(group_result$execution_result$config$seed, group_seed)
  expect_group_interval_table_equal(group_result$execution_result$interval_table, direct$interval_table, tolerance = tolerance)
}

test_that("single group execution matches direct refineR call with derived seed", {
  ingest <- load_grouped_execution_ingest()
  group_plan <- plan_grouped_fixture(ingest, "sex")
  config <- default_execution_config()
  group_index <- 1L
  group_label <- group_plan$groups$group_label[[group_index]]
  row_indices <- extract_group_row_indices(ingest, group_plan, group_label)
  group_seed <- derive_group_seed(config$seed, group_index)
  direct_config <- utils::modifyList(config, list(seed = group_seed))
  direct <- direct_refiner_interval_grouped(ingest$normalized_values[row_indices], direct_config)

  group_result <- run_group_refiner_estimation(
    ingest_result = ingest,
    group_plan = group_plan,
    config = config,
    group_index = group_index
  )

  expect_identical(group_result$status, "completed")
  expect_identical(group_result$row_indices, row_indices)
  expect_true(isTRUE(group_result$execution_result$success))
  expect_identical(group_result$execution_result$config$seed, group_seed)
  expect_group_interval_table_equal(group_result$execution_result$interval_table, direct$interval_table)
})

test_that("full grouped sex-by-age execution matches direct refineR calls for every completed group", {
  ingest <- load_grouped_execution_ingest()
  group_plan <- plan_grouped_fixture(ingest, "sex_age")
  config <- default_execution_config()

  grouped_result <- run_grouped_refiner_estimation(ingest, group_plan, config)

  expect_true(isTRUE(grouped_result$success_overall))
  expect_identical(grouped_result$metadata$groups_completed, nrow(group_plan$groups))
  expect_true(all(grouped_result$summary_table$status == "completed"))

  for (group_index in seq_len(nrow(group_plan$groups))) {
    group_result <- grouped_result$groups_results[[group_index]]
    expect_group_result_matches_direct(ingest, group_plan, group_result, group_index, config)

    interval_table <- group_result$execution_result$interval_table
    summary_row <- grouped_result$summary_table[group_index, , drop = FALSE]
    expect_identical(summary_row$lower_bound[[1]], as.character(interval_table$PointEst[[1]]))
    expect_identical(summary_row$upper_bound[[1]], as.character(interval_table$PointEst[[nrow(interval_table)]]))
  }
})

test_that("sex grouping execution matches direct refineR calls on exact sex subsets", {
  ingest <- load_grouped_execution_ingest()
  group_plan <- plan_grouped_fixture(ingest, "sex")
  config <- default_execution_config()

  grouped_result <- run_grouped_refiner_estimation(ingest, group_plan, config)

  expect_true(isTRUE(grouped_result$success_overall))
  expect_identical(group_plan$groups$group_label, c("female", "male"))

  for (group_index in seq_len(nrow(group_plan$groups))) {
    expect_group_result_matches_direct(ingest, group_plan, grouped_result$groups_results[[group_index]], group_index, config)
  }
})

test_that("age grouping execution matches direct refineR calls on exact age-band subsets", {
  ingest <- load_grouped_execution_ingest()
  group_plan <- plan_grouped_fixture(ingest, "age")
  config <- default_execution_config()

  grouped_result <- run_grouped_refiner_estimation(ingest, group_plan, config)

  expect_true(isTRUE(grouped_result$success_overall))
  expect_identical(group_plan$groups$group_label, c("0-17", "18-64", "65+"))

  for (group_index in seq_len(nrow(group_plan$groups))) {
    expect_group_result_matches_direct(ingest, group_plan, grouped_result$groups_results[[group_index]], group_index, config)
  }
})

test_that("sex-by-age grouping labels row indices and direct equivalence match current contract", {
  ingest <- load_grouped_execution_ingest()
  group_plan <- plan_grouped_fixture(ingest, "sex_age")
  config <- default_execution_config()

  expect_true(isTRUE(group_plan$ok))
  expect_identical(
    group_plan$groups$group_label,
    c("female / 0-17", "male / 0-17", "female / 18-64", "male / 18-64", "female / 65+", "male / 65+")
  )
  expect_true(all(group_plan$groups$row_count == 12L))

  group_result <- run_group_refiner_estimation(ingest, group_plan, config, group_index = 4L)
  expect_identical(group_result$row_indices, extract_group_row_indices(ingest, group_plan, "male / 18-64"))
  expect_group_result_matches_direct(ingest, group_plan, group_result, 4L, config)
})

test_that("post-NA grouped equivalence uses normalized row indices with raw-row recovery", {
  raw_source <- load_local_csv(test_path("../../fixtures/grouped_execution_valid.csv"))
  raw_data <- raw_source$data
  raw_data$analyte[c(2, 15, 38, 61)] <- NA
  source_data <- create_source_data("csv", "post-na-grouped-fixture", raw_data)
  ingest <- prepare_ingest_result(
    source_data,
    column_name = "analyte",
    sex_column = "sex_code",
    age_column = "age_years"
  )
  group_plan <- plan_grouped_fixture(ingest, "sex")
  config <- default_execution_config()
  group_index <- 1L
  group_label <- group_plan$groups$group_label[[group_index]]
  normalized_row_indices <- extract_group_row_indices(ingest, group_plan, group_label)
  recovered_raw_rows <- ingest$usable_row_indices[normalized_row_indices]
  group_seed <- derive_group_seed(config$seed, group_index)
  direct_config <- utils::modifyList(config, list(seed = group_seed))
  direct <- direct_refiner_interval_grouped(ingest$normalized_values[normalized_row_indices], direct_config)

  group_result <- run_group_refiner_estimation(ingest, group_plan, config, group_index = group_index)

  expect_identical(ingest$usable_row_indices, which(!is.na(raw_data$analyte)))
  expect_identical(group_result$row_indices, normalized_row_indices)
  expect_identical(ingest$usable_row_indices[group_result$row_indices], recovered_raw_rows)
  expect_false(identical(group_result$row_indices, recovered_raw_rows))
  expect_group_interval_table_equal(group_result$execution_result$interval_table, direct$interval_table)
})

test_that("bootstrap grouped execution matches direct refineR calls with derived seeds", {
  ingest <- load_grouped_execution_ingest()
  group_plan <- plan_grouped_fixture(ingest, "sex")
  config <- utils::modifyList(default_execution_config(), list(
    NBootstrap = 20L,
    pointEst = "medianBS"
  ))

  expect_lte(assert_valid_execution_config(config)$NBootstrap, 50L)

  grouped_result <- run_grouped_refiner_estimation(ingest, group_plan, config)

  expect_true(isTRUE(grouped_result$success_overall))
  expect_true(all(grouped_result$summary_table$status == "completed"))

  for (group_index in seq_len(nrow(group_plan$groups))) {
    expect_group_result_matches_direct(
      ingest,
      group_plan,
      grouped_result$groups_results[[group_index]],
      group_index,
      assert_valid_execution_config(config),
      tolerance = 1e-8
    )
  }
})

test_that("grouped failure retains failed groups while successful groups complete", {
  ingest <- load_grouped_execution_ingest("fixtures/grouped_mixed_outcome.csv")
  group_plan <- plan_grouped_fixture(ingest, "sex_age")
  config <- default_execution_config()

  grouped_result <- run_grouped_refiner_estimation(ingest, group_plan, config)

  expect_true(isTRUE(grouped_result$success_overall))
  expect_true(any(grouped_result$summary_table$status == "completed"))
  expect_true(any(grouped_result$summary_table$status == "failed"))
  expect_identical(grouped_result$metadata$groups_completed, 3L)
  expect_identical(grouped_result$metadata$groups_failed, 3L)

  failed_groups <- Filter(function(group_result) identical(group_result$status, "failed"), grouped_result$groups_results)
  completed_groups <- Filter(function(group_result) identical(group_result$status, "completed"), grouped_result$groups_results)

  expect_gt(length(failed_groups), 0)
  expect_gt(length(completed_groups), 0)
  expect_true(all(vapply(failed_groups, function(group_result) nzchar(group_result$error_message), logical(1))))
  expect_true(all(vapply(completed_groups, function(group_result) isTRUE(group_result$execution_result$success), logical(1))))
})
