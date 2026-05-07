test_that("packaged sample source loading exposes expected datasets", {
  expected_datasets <- c("testcase3", "testcase4", "testcase5")

  expect_identical(sample_dataset_choices(), expected_datasets)

  for (dataset_name in expected_datasets) {
    source_data <- load_sample_dataset(dataset_name)

    expect_s3_class(source_data, "refiner_source_data")
    expect_identical(source_data$source_type, "sample")
    expect_identical(source_data$source_label, dataset_name)
    expect_s3_class(as_tabular_data(source_data$data), "data.frame")
    expect_identical(source_data$source_fingerprint, dataset_name)
    expect_gt(length(list_numeric_candidate_columns(source_data$data)), 0)
  }
})

test_that("CSV source loading records label path and fingerprint", {
  csv_path <- tempfile(fileext = ".csv")
  on.exit(unlink(csv_path), add = TRUE)

  write.csv(
    data.frame(analyte = c(1, 2, 3), label = c("a", "b", "c")),
    csv_path,
    row.names = FALSE
  )

  default_label_source <- load_local_csv(csv_path)
  explicit_label_source <- load_local_csv(csv_path, source_label = "custom.csv")
  first_fingerprint <- default_label_source$source_fingerprint

  expect_s3_class(default_label_source, "refiner_source_data")
  expect_identical(default_label_source$source_type, "csv")
  expect_identical(default_label_source$source_label, basename(csv_path))
  expect_identical(explicit_label_source$source_label, "custom.csv")
  expect_true(nzchar(default_label_source$source_path))
  expect_true(file.exists(default_label_source$source_path))
  expect_true(nzchar(first_fingerprint))

  write.csv(
    data.frame(analyte = c(1, 2, 3, 4), label = c("a", "b", "c", "d")),
    csv_path,
    row.names = FALSE
  )
  changed_source <- load_local_csv(csv_path)

  expect_false(identical(first_fingerprint, changed_source$source_fingerprint))
})

test_that("as_tabular_data normalizes supported data shapes", {
  input_df <- data.frame(a = 1:2, b = c("x", "y"), stringsAsFactors = FALSE)
  expect_identical(as_tabular_data(input_df), input_df)

  input_matrix <- matrix(1:4, nrow = 2, dimnames = list(NULL, c("left", "right")))
  matrix_df <- as_tabular_data(input_matrix)
  expect_s3_class(matrix_df, "data.frame")
  expect_identical(names(matrix_df), c("left", "right"))

  input_array <- array(1:4, dim = c(2, 2), dimnames = list(NULL, c("x", "y")))
  array_df <- as_tabular_data(input_array)
  expect_s3_class(array_df, "data.frame")
  expect_identical(names(array_df), c("x", "y"))

  vector_df <- as_tabular_data(c(10, 20), default_column_name = "requested_name")
  expect_s3_class(vector_df, "data.frame")
  expect_identical(names(vector_df), "requested_name")
  expect_identical(vector_df$requested_name, c(10, 20))
})

test_that("numeric analyte candidate detection is deterministic", {
  data <- data.frame(
    numeric_col = c(1.1, 2.2),
    integer_col = 1:2,
    character_col = c("1", "2"),
    factor_col = factor(c("a", "b")),
    logical_all_na = c(NA, NA),
    stringsAsFactors = FALSE
  )

  candidates <- list_numeric_candidate_columns(data)

  expect_true("numeric_col" %in% candidates)
  expect_true("integer_col" %in% candidates)
  expect_false("character_col" %in% candidates)
  expect_false("factor_col" %in% candidates)
  # Current app behavior treats all-NA logical columns as selectable numeric candidates.
  expect_true("logical_all_na" %in% candidates)
})

test_that("analyte normalization drops missing values and records usable row indices", {
  df <- data.frame(
    analyte = c(10, NA, 12, 14, NA, 16),
    analyte2 = c(1, 2, 3, 4, 5, 6),
    sex = c("F", "M", "F", "M", "F", "M"),
    age = c(10, 20, 30, 40, 50, 60),
    text_col = c("a", "b", "c", "d", "e", "f"),
    stringsAsFactors = FALSE
  )

  normalized <- normalize_analyte_vector(df, "analyte")

  expect_identical(normalized$selected_column, "analyte")
  expect_identical(normalized$normalized_values, c(10, 12, 14, 16))
  expect_identical(normalized$usable_row_indices, c(1L, 3L, 4L, 6L))
  expect_identical(normalized$missing_count, 2L)
  expect_identical(normalized$row_count, 6L)
  expect_match(paste(normalized$warnings, collapse = " "), "missing values were removed")
  expect_true(isTRUE(normalized$ready))

  text_result <- normalize_analyte_vector(df, "text_col")
  expect_false(isTRUE(text_result$ready))
  expect_match(paste(text_result$errors, collapse = " "), "not numeric")
})

test_that("prepare_ingest_result aligns metadata to post-NA analyte rows", {
  df <- data.frame(
    analyte = c(10, NA, 12, 14, NA, 16),
    analyte2 = c(1, 2, 3, 4, 5, 6),
    sex = c("F", "M", "F", "M", "F", "M"),
    age = c(10, 20, 30, 40, 50, 60),
    text_col = c("a", "b", "c", "d", "e", "f"),
    stringsAsFactors = FALSE
  )
  source_data <- create_source_data("test", "fixture", df)

  ingest <- prepare_ingest_result(
    source_data,
    column_name = "analyte",
    sex_column = "sex",
    age_column = "age"
  )

  expect_identical(ingest$normalized_values, c(10, 12, 14, 16))
  expect_identical(ingest$usable_row_indices, c(1L, 3L, 4L, 6L))
  expect_identical(ingest$sex_values, c("F", "F", "M", "M"))
  expect_identical(ingest$age_values, c(10, 30, 40, 60))
})

test_that("sex normalization handles optional missing and invalid selections", {
  df <- data.frame(
    sex = c(" F ", "", NA, "M", "F"),
    other = 1:5,
    stringsAsFactors = FALSE
  )

  optional <- normalize_sex_vector(df)
  expect_null(optional$selected_column)
  expect_null(optional$values)
  expect_identical(optional$errors, character())

  invalid <- normalize_sex_vector(df, "missing_sex")
  expect_identical(invalid$selected_column, "missing_sex")
  expect_null(invalid$values)
  expect_match(paste(invalid$errors, collapse = " "), "not found")

  normalized <- normalize_sex_vector(df, "sex")
  expect_identical(normalized$values, c("F", NA, NA, "M", "F"))
  expect_identical(normalized$missing_count, 2L)
  expect_identical(normalized$unique_values, c("F", "M"))
})

test_that("age normalization handles optional missing invalid and blank values", {
  df <- data.frame(
    age = c("10", "", NA, "40"),
    bad_age = c("10", "twenty", "30", "40"),
    negative_age = c(10, -1, 30, 40),
    stringsAsFactors = FALSE
  )

  optional <- normalize_age_vector(df)
  expect_null(optional$selected_column)
  expect_null(optional$values)
  expect_identical(optional$errors, character())

  invalid <- normalize_age_vector(df, "missing_age")
  expect_identical(invalid$selected_column, "missing_age")
  expect_null(invalid$values)
  expect_match(paste(invalid$errors, collapse = " "), "not found")

  nonnumeric <- normalize_age_vector(df, "bad_age")
  expect_match(paste(nonnumeric$errors, collapse = " "), "must be numeric")

  negative <- normalize_age_vector(df, "negative_age")
  expect_match(paste(negative$errors, collapse = " "), "must not contain negative")

  normalized <- normalize_age_vector(df, "age")
  expect_identical(normalized$values, c(10, NA, NA, 40))
  expect_identical(normalized$missing_count, 2L)
  expect_identical(normalized$errors, character())
})
