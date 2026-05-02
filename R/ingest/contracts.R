sample_dataset_choices <- function() {
  c("testcase3", "testcase4", "testcase5")
}

create_source_data <- function(source_type, source_label, data, source_path = NULL, source_fingerprint = NULL) {
  structure(
    list(
      source_type = source_type,
      source_label = source_label,
      data = data,
      source_path = source_path,
      source_fingerprint = source_fingerprint
    ),
    class = c("refiner_source_data", "list")
  )
}

build_csv_source_fingerprint <- function(file_path) {
  if (is.null(file_path) || !nzchar(file_path) || !file.exists(file_path)) {
    stop("CSV file path is missing or does not exist.", call. = FALSE)
  }

  file_info <- file.info(file_path)
  md5_value <- unname(tools::md5sum(file_path))

  paste(
    if (!is.na(file_info$size)) as.character(file_info$size) else "",
    md5_value,
    sep = "::"
  )
}

create_ingest_result <- function(
  source_type,
  source_label,
  raw_data,
  numeric_candidate_columns,
  selected_column,
  normalized_values,
  usable_row_indices,
  missing_count,
  row_count,
  preview_data,
  sex_candidate_columns = character(),
  age_candidate_columns = character(),
  selected_sex_column = NULL,
  selected_age_column = NULL,
  sex_values = NULL,
  age_values = NULL,
  sex_missing_count = 0L,
  age_missing_count = 0L,
  sex_warnings = character(),
  sex_errors = character(),
  age_warnings = character(),
  age_errors = character(),
  metadata_warnings = character(),
  metadata_errors = character(),
  warnings = character(),
  errors = character()
) {
  structure(
    list(
      source_type = source_type,
      source_label = source_label,
      raw_data = raw_data,
      numeric_candidate_columns = numeric_candidate_columns,
      selected_column = selected_column,
      normalized_values = normalized_values,
      usable_row_indices = usable_row_indices,
      missing_count = missing_count,
      row_count = row_count,
      preview_data = preview_data,
      sex_candidate_columns = sex_candidate_columns,
      age_candidate_columns = age_candidate_columns,
      selected_sex_column = selected_sex_column,
      selected_age_column = selected_age_column,
      sex_values = sex_values,
      age_values = age_values,
      sex_missing_count = sex_missing_count,
      age_missing_count = age_missing_count,
      sex_warnings = sex_warnings,
      sex_errors = sex_errors,
      age_warnings = age_warnings,
      age_errors = age_errors,
      metadata_warnings = metadata_warnings,
      metadata_errors = metadata_errors,
      warnings = warnings,
      errors = errors,
      ready = length(errors) == 0 && length(normalized_values) > 0,
      metadata_ready = length(metadata_errors) == 0
    ),
    class = c("refiner_ingest_result", "list")
  )
}

create_group_plan <- function(
  mode,
  groups = data.frame(),
  total_rows = 0L,
  included_rows = 0L,
  excluded_rows = 0L,
  warnings = character(),
  errors = character(),
  metadata = list()
) {
  structure(
    list(
      ok = length(errors) == 0,
      mode = mode,
      groups = groups,
      total_rows = total_rows,
      included_rows = included_rows,
      excluded_rows = excluded_rows,
      warnings = warnings,
      errors = errors,
      metadata = metadata
    ),
    class = c("refiner_group_plan", "list")
  )
}

as_tabular_data <- function(data, default_column_name = "value") {
  if (is.data.frame(data)) {
    return(data)
  }

  if (is.matrix(data) || is.array(data)) {
    return(as.data.frame(data, stringsAsFactors = FALSE))
  }

  data.frame(
    stats::setNames(list(data), default_column_name),
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
}

load_sample_dataset <- function(dataset_name) {
  valid_datasets <- sample_dataset_choices()

  if (!dataset_name %in% valid_datasets) {
    stop(sprintf("Unsupported sample dataset: %s", dataset_name), call. = FALSE)
  }

  dataset <- getExportedValue("refineR", dataset_name)

  create_source_data(
    source_type = "sample",
    source_label = dataset_name,
    data = as_tabular_data(dataset),
    source_path = NULL,
    source_fingerprint = dataset_name
  )
}

load_local_csv <- function(file_path, source_label = NULL) {
  if (is.null(file_path) || !nzchar(file_path) || !file.exists(file_path)) {
    stop("CSV file path is missing or does not exist.", call. = FALSE)
  }

  data <- utils::read.csv(
    file = file_path,
    header = TRUE,
    stringsAsFactors = FALSE,
    check.names = FALSE
  )

  create_source_data(
    source_type = "csv",
    source_label = if (!is.null(source_label) && nzchar(source_label)) source_label else basename(file_path),
    data = as_tabular_data(data),
    source_path = normalizePath(file_path, winslash = "/", mustWork = TRUE),
    source_fingerprint = build_csv_source_fingerprint(file_path)
  )
}

create_source_reference_resolution <- function(
  status,
  source_reference,
  source_data = NULL,
  errors = character(),
  warnings = character(),
  metadata = list()
) {
  structure(
    list(
      ok = identical(status, "resolved") && length(errors) == 0,
      status = status,
      source_reference = source_reference,
      source_data = source_data,
      errors = errors,
      warnings = warnings,
      metadata = metadata
    ),
    class = c("refiner_source_reference_resolution", "list")
  )
}

normalize_source_reference_scalar <- function(value) {
  if (is.null(value) || length(value) == 0) {
    return("")
  }

  trimws(as.character(value[[1]]))
}

normalize_source_reference <- function(source_reference = list()) {
  defaults <- list(
    source_type = "",
    source_label = "",
    source_path = "",
    source_fingerprint = ""
  )
  normalized_reference <- utils::modifyList(defaults, source_reference)

  normalized_reference$source_type <- normalize_source_reference_scalar(normalized_reference$source_type)
  normalized_reference$source_label <- normalize_source_reference_scalar(normalized_reference$source_label)
  normalized_reference$source_path <- normalize_source_reference_scalar(normalized_reference$source_path)
  normalized_reference$source_fingerprint <- normalize_source_reference_scalar(normalized_reference$source_fingerprint)

  normalized_reference
}

resolve_experiment_settings_source_reference <- function(source_reference = list()) {
  normalized_reference <- normalize_source_reference(source_reference)

  if (!normalized_reference$source_type %in% c("sample", "csv")) {
    return(create_source_reference_resolution(
      status = "unsupported",
      source_reference = normalized_reference,
      errors = "Settings source_type must be either sample or csv."
    ))
  }

  if (!nzchar(normalized_reference$source_label)) {
    return(create_source_reference_resolution(
      status = "unsupported",
      source_reference = normalized_reference,
      errors = "Settings source_label is required."
    ))
  }

  if (identical(normalized_reference$source_type, "sample")) {
    source_value <- tryCatch(
      load_sample_dataset(normalized_reference$source_label),
      error = function(error) error
    )

    if (inherits(source_value, "error")) {
      return(create_source_reference_resolution(
        status = "unsupported",
        source_reference = normalized_reference,
        errors = conditionMessage(source_value)
      ))
    }

    normalized_reference$source_fingerprint <- source_value$source_fingerprint

    return(create_source_reference_resolution(
      status = "resolved",
      source_reference = normalized_reference,
      source_data = source_value,
      metadata = list(source_kind = "sample")
    ))
  }

  if (!nzchar(normalized_reference$source_path)) {
    return(create_source_reference_resolution(
      status = "missing",
      source_reference = normalized_reference,
      errors = "CSV settings do not include a reusable local source path."
    ))
  }

  if (!file.exists(normalized_reference$source_path)) {
    return(create_source_reference_resolution(
      status = "missing",
      source_reference = normalized_reference,
      errors = sprintf("Referenced CSV file does not exist: %s", normalized_reference$source_path)
    ))
  }

  current_fingerprint <- tryCatch(
    build_csv_source_fingerprint(normalized_reference$source_path),
    error = function(error) error
  )

  if (inherits(current_fingerprint, "error")) {
    return(create_source_reference_resolution(
      status = "unsupported",
      source_reference = normalized_reference,
      errors = conditionMessage(current_fingerprint)
    ))
  }

  if (nzchar(normalized_reference$source_fingerprint) && !identical(normalized_reference$source_fingerprint, current_fingerprint)) {
    return(create_source_reference_resolution(
      status = "fingerprint_mismatch",
      source_reference = normalized_reference,
      errors = "Referenced CSV file fingerprint does not match the exported settings.",
      metadata = list(current_fingerprint = current_fingerprint)
    ))
  }

  source_value <- tryCatch(
    load_local_csv(normalized_reference$source_path, source_label = normalized_reference$source_label),
    error = function(error) error
  )

  if (inherits(source_value, "error")) {
    return(create_source_reference_resolution(
      status = "unsupported",
      source_reference = normalized_reference,
      errors = conditionMessage(source_value)
    ))
  }

  normalized_reference$source_fingerprint <- current_fingerprint

  create_source_reference_resolution(
    status = "resolved",
    source_reference = normalized_reference,
    source_data = source_value,
    metadata = list(
      source_kind = "csv",
      current_fingerprint = current_fingerprint
    )
  )
}

normalize_lookup_value <- function(value) {
  normalized <- trimws(as.character(value))
  normalized[normalized == ""] <- NA_character_
  tolower(normalized)
}

list_sex_candidate_columns <- function(data, max_unique_values = 10L) {
  tabular_data <- as_tabular_data(data)

  names(tabular_data)[vapply(
    tabular_data,
    FUN = function(column) {
      if (is.list(column)) {
        return(FALSE)
      }

      normalized <- trimws(as.character(column))
      normalized[normalized == ""] <- NA_character_
      unique_values <- unique(normalized[!is.na(normalized)])

      length(unique_values) > 0 &&
        length(unique_values) <= max_unique_values
    },
    FUN.VALUE = logical(1)
  )]
}

list_age_candidate_columns <- function(data) {
  tabular_data <- as_tabular_data(data)

  names(tabular_data)[vapply(
    tabular_data,
    FUN = function(column) {
      if (!(is.numeric(column) || is.integer(column))) {
        return(FALSE)
      }

      non_missing <- column[!is.na(column)]

      if (length(non_missing) == 0) {
        return(FALSE)
      }

      all(is.finite(non_missing)) && min(non_missing) >= 0 && max(non_missing) <= 150
    },
    FUN.VALUE = logical(1)
  )]
}

parse_explicit_mapping <- function(mapping_text, field_name = "mapping") {
  if (is.null(mapping_text) || !nzchar(trimws(mapping_text))) {
    return(stats::setNames(character(), character()))
  }

  lines <- trimws(unlist(strsplit(mapping_text, "\n", fixed = FALSE)))
  lines <- lines[nzchar(lines)]

  if (length(lines) == 0) {
    return(stats::setNames(character(), character()))
  }

  mapping <- stats::setNames(character(), character())

  for (line in lines) {
    if (!grepl("=", line, fixed = TRUE)) {
      stop(sprintf("Each %s entry must use 'label = raw1, raw2' syntax.", field_name), call. = FALSE)
    }

    parts <- strsplit(line, "=", fixed = TRUE)[[1]]

    if (length(parts) != 2) {
      stop(sprintf("Each %s entry must contain exactly one '=' separator.", field_name), call. = FALSE)
    }

    label <- trimws(parts[[1]])
    raw_values <- trimws(strsplit(parts[[2]], ",", fixed = TRUE)[[1]])
    raw_values <- raw_values[nzchar(raw_values)]

    if (!nzchar(label) || length(raw_values) == 0) {
      stop(sprintf("Each %s entry must define a label and at least one raw value.", field_name), call. = FALSE)
    }

    normalized_keys <- normalize_lookup_value(raw_values)

    if (any(is.na(normalized_keys))) {
      stop(sprintf("Blank raw values are not allowed in %s entries.", field_name), call. = FALSE)
    }

    duplicate_keys <- intersect(names(mapping), normalized_keys)

    if (length(duplicate_keys) > 0) {
      stop(sprintf("Duplicate raw values detected in %s: %s", field_name, paste(raw_values[normalized_keys %in% duplicate_keys], collapse = ", ")), call. = FALSE)
    }

    mapping <- c(mapping, stats::setNames(rep(label, length(normalized_keys)), normalized_keys))
  }

  mapping
}

parse_age_band_text <- function(age_band_text) {
  if (is.null(age_band_text) || !nzchar(trimws(age_band_text))) {
    stop("Age bands are required for age-based grouping.", call. = FALSE)
  }

  tokens <- gsub(";", "\n", age_band_text, fixed = TRUE)
  tokens <- trimws(unlist(strsplit(tokens, "\n", fixed = FALSE)))
  tokens <- tokens[nzchar(tokens)]

  if (length(tokens) == 0) {
    stop("Age bands are required for age-based grouping.", call. = FALSE)
  }

  parse_band <- function(token) {
    if (grepl("^[0-9]+(?:\\.[0-9]+)?\\s*-\\s*[0-9]+(?:\\.[0-9]+)?$", token)) {
      bounds <- trimws(strsplit(token, "-", fixed = TRUE)[[1]])
      lower <- as.numeric(bounds[[1]])
      upper <- as.numeric(bounds[[2]])

      if (lower > upper) {
        stop(sprintf("Age band '%s' has a lower bound greater than its upper bound.", token), call. = FALSE)
      }

      return(list(label = token, lower = lower, upper = upper, lower_inclusive = TRUE, upper_inclusive = TRUE))
    }

    if (grepl("^[0-9]+(?:\\.[0-9]+)?\\+$", token)) {
      lower <- as.numeric(sub("\\+$", "", token))
      return(list(label = token, lower = lower, upper = Inf, lower_inclusive = TRUE, upper_inclusive = TRUE))
    }

    if (grepl("^<=\\s*[0-9]+(?:\\.[0-9]+)?$", token)) {
      upper <- as.numeric(sub("^<=\\s*", "", token))
      return(list(label = token, lower = -Inf, upper = upper, lower_inclusive = TRUE, upper_inclusive = TRUE))
    }

    if (grepl("^<\\s*[0-9]+(?:\\.[0-9]+)?$", token)) {
      upper <- as.numeric(sub("^<\\s*", "", token))
      return(list(label = token, lower = -Inf, upper = upper, lower_inclusive = TRUE, upper_inclusive = FALSE))
    }

    if (grepl("^>=\\s*[0-9]+(?:\\.[0-9]+)?$", token)) {
      lower <- as.numeric(sub("^>=\\s*", "", token))
      return(list(label = token, lower = lower, upper = Inf, lower_inclusive = TRUE, upper_inclusive = TRUE))
    }

    if (grepl("^>\\s*[0-9]+(?:\\.[0-9]+)?$", token)) {
      lower <- as.numeric(sub("^>\\s*", "", token))
      return(list(label = token, lower = lower, upper = Inf, lower_inclusive = FALSE, upper_inclusive = TRUE))
    }

    stop(sprintf("Unsupported age band syntax: %s", token), call. = FALSE)
  }

  parsed <- lapply(tokens, parse_band)
  labels <- vapply(parsed, function(entry) entry$label, character(1))

  if (anyDuplicated(labels) > 0) {
    stop(sprintf("Age band labels must be unique: %s", paste(labels[duplicated(labels)], collapse = ", ")), call. = FALSE)
  }

  data.frame(
    label = labels,
    lower = vapply(parsed, function(entry) entry$lower, numeric(1)),
    upper = vapply(parsed, function(entry) entry$upper, numeric(1)),
    lower_inclusive = vapply(parsed, function(entry) entry$lower_inclusive, logical(1)),
    upper_inclusive = vapply(parsed, function(entry) entry$upper_inclusive, logical(1)),
    stringsAsFactors = FALSE,
    check.names = FALSE
  )
}

match_age_band <- function(values, age_bands) {
  if (length(values) == 0) {
    return(character())
  }

  match_matrix <- vapply(
    seq_len(nrow(age_bands)),
    FUN = function(index) {
      band <- age_bands[index, , drop = FALSE]
      lower_ok <- if (isTRUE(band$lower_inclusive)) values >= band$lower else values > band$lower
      upper_ok <- if (isTRUE(band$upper_inclusive)) values <= band$upper else values < band$upper
      lower_ok & upper_ok
    },
    FUN.VALUE = logical(length(values))
  )

  if (is.null(dim(match_matrix))) {
    match_matrix <- matrix(match_matrix, ncol = 1)
  }

  overlap_rows <- which(rowSums(match_matrix) > 1)

  if (length(overlap_rows) > 0) {
    stop(sprintf("Age bands overlap for value(s): %s", paste(unique(values[overlap_rows]), collapse = ", ")), call. = FALSE)
  }

  uncovered_rows <- which(rowSums(match_matrix) == 0)

  if (length(uncovered_rows) > 0) {
    stop(sprintf("Age bands do not cover value(s): %s", paste(unique(values[uncovered_rows]), collapse = ", ")), call. = FALSE)
  }

  matched_index <- max.col(match_matrix, ties.method = "first")
  age_bands$label[matched_index]
}

list_numeric_candidate_columns <- function(data) {
  tabular_data <- as_tabular_data(data)

  names(tabular_data)[vapply(
    tabular_data,
    FUN = function(column) {
      is.numeric(column) || (is.logical(column) && all(is.na(column)))
    },
    FUN.VALUE = logical(1)
  )]
}

normalize_analyte_vector <- function(data, column_name, na_policy = "drop-with-warning") {
  tabular_data <- as_tabular_data(data)
  candidate_columns <- list_numeric_candidate_columns(tabular_data)
  row_count <- nrow(tabular_data)
  preview_rows <- utils::head(tabular_data, 8)

  if (length(candidate_columns) == 0) {
    return(create_ingest_result(
      source_type = "unknown",
      source_label = "",
      raw_data = tabular_data,
      numeric_candidate_columns = character(),
      selected_column = NULL,
      normalized_values = numeric(),
      usable_row_indices = integer(),
      missing_count = 0,
      row_count = row_count,
      preview_data = preview_rows,
      errors = "No numeric analyte columns were found in the selected dataset."
    ))
  }

  selected_column <- if (!is.null(column_name) && nzchar(column_name)) {
    column_name
  } else {
    candidate_columns[[1]]
  }

  if (!selected_column %in% candidate_columns) {
    return(create_ingest_result(
      source_type = "unknown",
      source_label = "",
      raw_data = tabular_data,
      numeric_candidate_columns = candidate_columns,
      selected_column = selected_column,
      normalized_values = numeric(),
      usable_row_indices = integer(),
      missing_count = 0,
      row_count = row_count,
      preview_data = preview_rows,
      errors = sprintf("Selected analyte column '%s' is not numeric.", selected_column)
    ))
  }

  analyte_values <- as.numeric(tabular_data[[selected_column]])
  missing_count <- sum(is.na(analyte_values))
  warnings <- character()

  if (!identical(na_policy, "drop-with-warning")) {
    stop(sprintf("Unsupported NA policy: %s", na_policy), call. = FALSE)
  }

  usable_row_indices <- which(!is.na(analyte_values))
  normalized_values <- analyte_values[usable_row_indices]

  if (missing_count > 0) {
    warnings <- c(
      warnings,
      sprintf(
        "%s missing value%s removed from analyte column '%s'.",
        missing_count,
        if (missing_count == 1) " was" else "s were",
        selected_column
      )
    )
  }

  errors <- character()

  if (length(normalized_values) == 0) {
    errors <- c(errors, sprintf("Analyte column '%s' has no usable numeric values after NA handling.", selected_column))
  }

  create_ingest_result(
    source_type = "unknown",
    source_label = "",
    raw_data = tabular_data,
    numeric_candidate_columns = candidate_columns,
    selected_column = selected_column,
    normalized_values = normalized_values,
    usable_row_indices = usable_row_indices,
    missing_count = missing_count,
    row_count = row_count,
    preview_data = preview_rows,
    warnings = warnings,
    errors = errors
  )
}

normalize_sex_vector <- function(data, column_name = NULL, usable_row_indices = NULL) {
  tabular_data <- as_tabular_data(data)

  if (is.null(column_name) || !nzchar(trimws(column_name))) {
    return(list(selected_column = NULL, values = NULL, missing_count = 0L, warnings = character(), errors = character(), unique_values = character()))
  }

  if (!column_name %in% names(tabular_data)) {
    return(list(
      selected_column = column_name,
      values = NULL,
      missing_count = 0L,
      warnings = character(),
      errors = sprintf("Selected sex column '%s' was not found in the dataset.", column_name),
      unique_values = character()
    ))
  }

  values <- tabular_data[[column_name]]

  if (!is.null(usable_row_indices)) {
    values <- values[usable_row_indices]
  }

  normalized_values <- trimws(as.character(values))
  normalized_values[normalized_values == ""] <- NA_character_

  list(
    selected_column = column_name,
    values = normalized_values,
    missing_count = sum(is.na(normalized_values)),
    warnings = character(),
    errors = character(),
    unique_values = sort(unique(normalized_values[!is.na(normalized_values)]))
  )
}

normalize_age_vector <- function(data, column_name = NULL, usable_row_indices = NULL) {
  tabular_data <- as_tabular_data(data)

  if (is.null(column_name) || !nzchar(trimws(column_name))) {
    return(list(selected_column = NULL, values = NULL, missing_count = 0L, warnings = character(), errors = character()))
  }

  if (!column_name %in% names(tabular_data)) {
    return(list(
      selected_column = column_name,
      values = NULL,
      missing_count = 0L,
      warnings = character(),
      errors = sprintf("Selected age column '%s' was not found in the dataset.", column_name)
    ))
  }

  values <- tabular_data[[column_name]]

  if (!is.null(usable_row_indices)) {
    values <- values[usable_row_indices]
  }

  raw_values <- trimws(as.character(values))
  raw_values[raw_values == ""] <- NA_character_
  normalized_values <- suppressWarnings(as.numeric(raw_values))
  invalid_rows <- !is.na(raw_values) & is.na(normalized_values)
  errors <- character()

  if (any(invalid_rows)) {
    errors <- c(errors, sprintf("Selected age column '%s' must be numeric for all non-missing values.", column_name))
  }

  if (any(normalized_values[!is.na(normalized_values)] < 0)) {
    errors <- c(errors, sprintf("Selected age column '%s' must not contain negative values.", column_name))
  }

  list(
    selected_column = column_name,
    values = normalized_values,
    missing_count = sum(is.na(raw_values)),
    warnings = character(),
    errors = errors
  )
}

plan_grouped_analysis <- function(
  analyte_values,
  grouping_mode = "overall",
  sex_values = NULL,
  sex_mapping_text = NULL,
  age_values = NULL,
  age_band_text = NULL
) {
  supported_modes <- c("overall", "sex", "age", "sex_age")
  errors <- character()
  warnings <- character()
  total_rows <- length(analyte_values)

  if (!grouping_mode %in% supported_modes) {
    return(create_group_plan(
      mode = grouping_mode,
      total_rows = total_rows,
      errors = sprintf("Unsupported grouping mode: %s", grouping_mode)
    ))
  }

  if (!is.null(sex_values) && length(sex_values) != total_rows) {
    errors <- c(errors, "Sex metadata must align with the analyte values after analyte NA handling.")
  }

  if (!is.null(age_values) && length(age_values) != total_rows) {
    errors <- c(errors, "Age metadata must align with the analyte values after analyte NA handling.")
  }

  if (length(errors) > 0) {
    return(create_group_plan(mode = grouping_mode, total_rows = total_rows, errors = errors))
  }

  included_rows <- rep(TRUE, total_rows)
  normalized_sex <- rep(NA_character_, total_rows)
  age_band_labels <- rep(NA_character_, total_rows)
  sex_mapping <- stats::setNames(character(), character())
  age_bands <- data.frame()

  if (grouping_mode %in% c("sex", "sex_age")) {
    if (is.null(sex_values)) {
      errors <- c(errors, "Sex grouping requires a selected sex column.")
    } else {
      sex_mapping <- tryCatch(
        parse_explicit_mapping(sex_mapping_text, field_name = "sex mapping"),
        error = function(error) {
          errors <<- c(errors, conditionMessage(error))
          stats::setNames(character(), character())
        }
      )

      if (length(sex_mapping) == 0) {
        errors <- c(errors, "Sex grouping requires at least one explicit sex mapping entry.")
      } else {
        normalized_keys <- normalize_lookup_value(sex_values)
        non_missing_rows <- !is.na(normalized_keys)
        normalized_sex[non_missing_rows] <- unname(sex_mapping[normalized_keys[non_missing_rows]])
        unmapped_values <- sort(unique(sex_values[non_missing_rows & is.na(normalized_sex)]))

        if (length(unmapped_values) > 0) {
          errors <- c(errors, sprintf("Sex mapping does not cover value(s): %s", paste(unmapped_values, collapse = ", ")))
        }

        missing_rows <- is.na(normalized_keys)

        if (any(missing_rows)) {
          included_rows[missing_rows] <- FALSE
          warnings <- c(warnings, sprintf("%s row(s) were excluded from grouped planning because the selected sex column is missing.", sum(missing_rows)))
        }
      }
    }
  }

  if (grouping_mode %in% c("age", "sex_age")) {
    if (is.null(age_values)) {
      errors <- c(errors, "Age grouping requires a selected age column.")
    } else {
      age_bands <- tryCatch(
        parse_age_band_text(age_band_text),
        error = function(error) {
          errors <<- c(errors, conditionMessage(error))
          data.frame()
        }
      )

      if (nrow(age_bands) > 0) {
        non_missing_rows <- !is.na(age_values)

        if (any(non_missing_rows)) {
          age_band_labels[non_missing_rows] <- tryCatch(
            match_age_band(age_values[non_missing_rows], age_bands),
            error = function(error) {
              errors <<- c(errors, conditionMessage(error))
              rep(NA_character_, sum(non_missing_rows))
            }
          )
        }

        missing_rows <- is.na(age_values)

        if (any(missing_rows)) {
          included_rows[missing_rows] <- FALSE
          warnings <- c(warnings, sprintf("%s row(s) were excluded from grouped planning because the selected age column is missing.", sum(missing_rows)))
        }
      }
    }
  }

  if (length(errors) > 0) {
    return(create_group_plan(
      mode = grouping_mode,
      total_rows = total_rows,
      included_rows = sum(included_rows),
      excluded_rows = sum(!included_rows),
      warnings = warnings,
      errors = errors,
      metadata = list(sex_mapping = sex_mapping, age_bands = age_bands)
    ))
  }

  groups <- switch(
    grouping_mode,
    overall = data.frame(group_label = "Overall", sex_group = NA_character_, age_band = NA_character_, stringsAsFactors = FALSE),
    sex = data.frame(
      group_label = unique(unname(sex_mapping)),
      sex_group = unique(unname(sex_mapping)),
      age_band = NA_character_,
      stringsAsFactors = FALSE
    ),
    age = data.frame(
      group_label = age_bands$label,
      sex_group = NA_character_,
      age_band = age_bands$label,
      stringsAsFactors = FALSE
    ),
    sex_age = {
      sex_groups <- unique(unname(sex_mapping))
      grid <- expand.grid(sex_group = sex_groups, age_band = age_bands$label, stringsAsFactors = FALSE)
      grid$group_label <- paste(grid$sex_group, grid$age_band, sep = " / ")
      grid[, c("group_label", "sex_group", "age_band")]
    }
  )

  group_labels <- switch(
    grouping_mode,
    overall = rep("Overall", total_rows),
    sex = normalized_sex,
    age = age_band_labels,
    sex_age = paste(normalized_sex, age_band_labels, sep = " / ")
  )

  included_labels <- group_labels[included_rows]
  counts <- table(factor(included_labels, levels = groups$group_label))
  groups$row_count <- as.integer(counts[groups$group_label])

  empty_groups <- groups$group_label[groups$row_count == 0]

  if (length(empty_groups) > 0) {
    errors <- c(errors, sprintf("Requested grouping produced empty groups: %s", paste(empty_groups, collapse = ", ")))
  }

  if (sum(included_rows) == 0) {
    errors <- c(errors, "Grouped planning requires at least one usable analyte row after metadata filtering.")
  }

  create_group_plan(
    mode = grouping_mode,
    groups = groups,
    total_rows = total_rows,
    included_rows = sum(included_rows),
    excluded_rows = sum(!included_rows),
    warnings = warnings,
    errors = errors,
    metadata = list(sex_mapping = sex_mapping, age_bands = age_bands)
  )
}

prepare_ingest_result <- function(source_data, column_name = NULL, na_policy = "drop-with-warning", sex_column = NULL, age_column = NULL) {
  normalized <- normalize_analyte_vector(
    data = source_data$data,
    column_name = column_name,
    na_policy = na_policy
  )

  sex_metadata <- normalize_sex_vector(
    data = source_data$data,
    column_name = sex_column,
    usable_row_indices = normalized$usable_row_indices
  )

  age_metadata <- normalize_age_vector(
    data = source_data$data,
    column_name = age_column,
    usable_row_indices = normalized$usable_row_indices
  )

  create_ingest_result(
    source_type = source_data$source_type,
    source_label = source_data$source_label,
    raw_data = normalized$raw_data,
    numeric_candidate_columns = normalized$numeric_candidate_columns,
    selected_column = normalized$selected_column,
    normalized_values = normalized$normalized_values,
    usable_row_indices = normalized$usable_row_indices,
    missing_count = normalized$missing_count,
    row_count = normalized$row_count,
    preview_data = normalized$preview_data,
    sex_candidate_columns = list_sex_candidate_columns(normalized$raw_data),
    age_candidate_columns = list_age_candidate_columns(normalized$raw_data),
    selected_sex_column = sex_metadata$selected_column,
    selected_age_column = age_metadata$selected_column,
    sex_values = sex_metadata$values,
    age_values = age_metadata$values,
    sex_missing_count = sex_metadata$missing_count,
    age_missing_count = age_metadata$missing_count,
    sex_warnings = sex_metadata$warnings,
    sex_errors = sex_metadata$errors,
    age_warnings = age_metadata$warnings,
    age_errors = age_metadata$errors,
    metadata_warnings = c(sex_metadata$warnings, age_metadata$warnings),
    metadata_errors = c(sex_metadata$errors, age_metadata$errors),
    warnings = normalized$warnings,
    errors = normalized$errors
  )
}