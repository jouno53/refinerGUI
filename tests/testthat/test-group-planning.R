make_group_ingest <- function(values, sex = NULL, age = NULL, usable_row_indices = seq_along(values)) {
  create_ingest_result(
    source_type = "test",
    source_label = "group-fixture",
    raw_data = data.frame(),
    numeric_candidate_columns = "analyte",
    selected_column = "analyte",
    normalized_values = values,
    usable_row_indices = usable_row_indices,
    missing_count = 0L,
    row_count = length(usable_row_indices),
    preview_data = data.frame(),
    selected_sex_column = if (!is.null(sex)) "sex" else NULL,
    selected_age_column = if (!is.null(age)) "age" else NULL,
    sex_values = sex,
    age_values = age
  )
}

test_that("overall grouping includes all normalized rows", {
  values <- c(10, 12, 14, 16)
  plan <- plan_grouped_analysis(values, grouping_mode = "overall")
  ingest <- make_group_ingest(values)

  expect_s3_class(plan, "refiner_group_plan")
  expect_true(isTRUE(plan$ok))
  expect_identical(plan$mode, "overall")
  expect_identical(plan$groups$group_label, "Overall")
  expect_identical(plan$groups$row_count, 4L)
  expect_identical(plan$included_rows, 4L)
  expect_identical(plan$excluded_rows, 0L)
  expect_identical(extract_group_row_indices(ingest, plan, "Overall"), seq_along(values))
})

test_that("sex grouping uses explicit mapping labels and normalized row indices", {
  values <- c(10, 12, 14, 16, 18, 20)
  sex <- c("F", "Female", "M", "Male", "F", "M")
  mapping <- "Female = F, Female\nMale = M, Male"
  plan <- plan_grouped_analysis(
    values,
    grouping_mode = "sex",
    sex_values = sex,
    sex_mapping_text = mapping
  )
  ingest <- make_group_ingest(values, sex = sex)

  expect_true(isTRUE(plan$ok))
  expect_identical(plan$groups$group_label, c("Female", "Male"))
  expect_identical(plan$groups$row_count, c(3L, 3L))
  expect_identical(extract_group_row_indices(ingest, plan, "Female"), c(1L, 2L, 5L))
  expect_identical(extract_group_row_indices(ingest, plan, "Male"), c(3L, 4L, 6L))
})

test_that("sex grouping edge cases fail or exclude rows clearly", {
  values <- c(10, 12, 14, 16)

  missing_mapping <- plan_grouped_analysis(
    values,
    grouping_mode = "sex",
    sex_values = c("F", "M", "F", "M"),
    sex_mapping_text = ""
  )
  expect_false(isTRUE(missing_mapping$ok))
  expect_match(paste(missing_mapping$errors, collapse = " "), "requires at least one explicit sex mapping")

  unmapped <- plan_grouped_analysis(
    values,
    grouping_mode = "sex",
    sex_values = c("F", "M", "Unknown", "F"),
    sex_mapping_text = "Female = F\nMale = M"
  )
  expect_false(isTRUE(unmapped$ok))
  expect_match(paste(unmapped$errors, collapse = " "), "does not cover")

  missing_values <- plan_grouped_analysis(
    values,
    grouping_mode = "sex",
    sex_values = c("F", "", NA, "M"),
    sex_mapping_text = "Female = F\nMale = M"
  )
  expect_true(isTRUE(missing_values$ok))
  expect_identical(missing_values$included_rows, 2L)
  expect_identical(missing_values$excluded_rows, 2L)
  expect_match(paste(missing_values$warnings, collapse = " "), "excluded from grouped planning")
  expect_identical(missing_values$groups$row_count, c(1L, 1L))

  duplicate_raw <- plan_grouped_analysis(
    values,
    grouping_mode = "sex",
    sex_values = c("F", "M", "F", "M"),
    sex_mapping_text = "Female = F\nAlso Female = F\nMale = M"
  )
  expect_false(isTRUE(duplicate_raw$ok))
  expect_match(paste(duplicate_raw$errors, collapse = " "), "Duplicate raw values")
})

test_that("age band parsing documents boundary and operator semantics", {
  age_bands <- parse_age_band_text("0-18\n18-30\n30+")

  expect_identical(match_age_band(c(18, 18.1, 30, 30.1), age_bands), c("0-18", "18-30", "18-30", "30+"))

  operator_bands <- parse_age_band_text("<18\n>=18")
  expect_identical(match_age_band(c(17.9, 18, 30), operator_bands), c("<18", ">=18", ">=18"))

  expect_error(parse_age_band_text("young"), "Unsupported age band syntax")
  expect_error(parse_age_band_text("0-18\n0-18"), "labels must be unique")

  overlapping_bands <- parse_age_band_text("0-20\n10-30")
  expect_error(match_age_band(c(15), overlapping_bands), "overlap")

  uncovered_bands <- parse_age_band_text("0-18\n18-30")
  expect_error(match_age_band(c(45), uncovered_bands), "do not cover")
})

test_that("age grouping returns expected row indices", {
  values <- c(10, 12, 14, 16, 18, 20)
  age <- c(10, 18, 19, 30, 31, 45)
  age_band_text <- "0-18\n18-30\n30+"
  plan <- plan_grouped_analysis(
    values,
    grouping_mode = "age",
    age_values = age,
    age_band_text = age_band_text
  )
  ingest <- make_group_ingest(values, age = age)

  expect_true(isTRUE(plan$ok))
  expect_identical(plan$groups$group_label, c("0-18", "18-30", "30+"))
  expect_identical(plan$groups$row_count, c(2L, 2L, 2L))
  expect_identical(extract_group_row_indices(ingest, plan, "0-18"), c(1L, 2L))
  expect_identical(extract_group_row_indices(ingest, plan, "18-30"), c(3L, 4L))
  expect_identical(extract_group_row_indices(ingest, plan, "30+"), c(5L, 6L))
})

test_that("sex-by-age grouping labels and empty-group behavior are explicit", {
  values <- c(10, 12, 14, 16, 18, 20)
  sex <- c("F", "F", "M", "M", "F", "M")
  age <- c(10, 20, 10, 20, 40, 40)
  plan <- plan_grouped_analysis(
    values,
    grouping_mode = "sex_age",
    sex_values = sex,
    sex_mapping_text = "Female = F\nMale = M",
    age_values = age,
    age_band_text = "0-18\n18-30\n30+"
  )
  ingest <- make_group_ingest(values, sex = sex, age = age)

  expect_true(isTRUE(plan$ok))
  expect_identical(
    plan$groups$group_label,
    c("Female / 0-18", "Male / 0-18", "Female / 18-30", "Male / 18-30", "Female / 30+", "Male / 30+")
  )
  expect_identical(extract_group_row_indices(ingest, plan, "Female / 0-18"), 1L)
  expect_identical(extract_group_row_indices(ingest, plan, "Male / 0-18"), 3L)
  expect_identical(extract_group_row_indices(ingest, plan, "Female / 18-30"), 2L)
  expect_identical(extract_group_row_indices(ingest, plan, "Male / 18-30"), 4L)
  expect_identical(extract_group_row_indices(ingest, plan, "Female / 30+"), 5L)
  expect_identical(extract_group_row_indices(ingest, plan, "Male / 30+"), 6L)

  empty_group_plan <- plan_grouped_analysis(
    values,
    grouping_mode = "sex_age",
    sex_values = c("F", "F", "M", "M", "F", "F"),
    sex_mapping_text = "Female = F\nMale = M",
    age_values = age,
    age_band_text = "0-18\n18-30\n30+"
  )
  expect_false(isTRUE(empty_group_plan$ok))
  expect_match(paste(empty_group_plan$errors, collapse = " "), "empty groups")

  missing_metadata_plan <- plan_grouped_analysis(
    values,
    grouping_mode = "sex_age",
    sex_values = c("F", "", "M", "M", "F", "M"),
    sex_mapping_text = "Female = F\nMale = M",
    age_values = c(10, 20, 10, NA, 40, 40),
    age_band_text = "0-18\n18-30\n30+"
  )
  expect_false(isTRUE(missing_metadata_plan$ok))
  expect_identical(missing_metadata_plan$included_rows, 4L)
  expect_identical(missing_metadata_plan$excluded_rows, 2L)
  expect_match(paste(missing_metadata_plan$warnings, collapse = " "), "excluded from grouped planning")
  expect_match(paste(missing_metadata_plan$errors, collapse = " "), "empty groups")
})

test_that("group row indices are relative to post-NA normalized analyte values", {
  df <- data.frame(
    analyte = c(10, NA, 12, 14, NA, 16),
    sex = c("F", "M", "F", "M", "F", "M"),
    age = c(10, 20, 30, 40, 50, 60),
    stringsAsFactors = FALSE
  )
  source_data <- create_source_data("test", "post-na-fixture", df)
  ingest <- prepare_ingest_result(
    source_data,
    column_name = "analyte",
    sex_column = "sex",
    age_column = "age"
  )
  plan <- plan_grouped_analysis(
    ingest$normalized_values,
    grouping_mode = "sex",
    sex_values = ingest$sex_values,
    sex_mapping_text = "Female = F\nMale = M"
  )

  expect_identical(ingest$usable_row_indices, c(1L, 3L, 4L, 6L))
  expect_true(isTRUE(plan$ok))

  female_normalized_rows <- extract_group_row_indices(ingest, plan, "Female")
  male_normalized_rows <- extract_group_row_indices(ingest, plan, "Male")

  expect_identical(female_normalized_rows, c(1L, 2L))
  expect_identical(male_normalized_rows, c(3L, 4L))
  expect_identical(ingest$usable_row_indices[female_normalized_rows], c(1L, 3L))
  expect_identical(ingest$usable_row_indices[male_normalized_rows], c(4L, 6L))
})

test_that("unsupported grouping mode fails cleanly", {
  plan <- plan_grouped_analysis(c(10, 12, 14), grouping_mode = "unsupported")

  expect_s3_class(plan, "refiner_group_plan")
  expect_false(isTRUE(plan$ok))
  expect_match(paste(plan$errors, collapse = " "), "Unsupported grouping mode")
})
