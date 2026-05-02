create_wrapper_stub <- function(action, details = NULL) {
  structure(
    list(
      layer = "wrapper",
      action = action,
      implemented = FALSE,
      details = details
    ),
    class = c("refiner_wrapper_stub", "list")
  )
}

create_execution_result <- function(
  success,
  fit = NULL,
  interval_table = NULL,
  config = list(),
  metadata = list(),
  error_message = NULL
) {
  structure(
    list(
      success = success,
      fit = fit,
      interval_table = interval_table,
      config = config,
      metadata = metadata,
      error_message = error_message
    ),
    class = c("refiner_execution_result", "list")
  )
}

create_group_execution_result <- function(
  group_label,
  group_size,
  row_indices,
  status = "queued",
  execution_result = NULL,
  warning_messages = character(),
  error_message = NULL,
  metadata = list()
) {
  structure(
    list(
      group_label = group_label,
      group_size = group_size,
      row_indices = row_indices,
      status = status,
      execution_result = execution_result,
      warning_messages = warning_messages,
      error_message = error_message,
      metadata = metadata
    ),
    class = c("refiner_group_execution_result", "list")
  )
}

create_grouped_execution_result <- function(
  success_overall,
  groups_results,
  summary_table,
  config,
  grouping_metadata = list(),
  metadata = list(),
  warnings = character(),
  errors = character()
) {
  structure(
    list(
      success_overall = success_overall,
      groups_results = groups_results,
      summary_table = summary_table,
      config = config,
      grouping_metadata = grouping_metadata,
      metadata = metadata,
      warnings = warnings,
      errors = errors
    ),
    class = c("refiner_grouped_execution_result", "list")
  )
}

create_checkpoint_manifest <- function(
  checkpoint_id,
  checkpoint_dir,
  run_signature,
  config_signature,
  grouping_signature,
  source_metadata = list(),
  planned_groups = character(),
  completed_groups = character(),
  failed_groups = character(),
  pending_groups = character(),
  per_group_artifacts = list(),
  status = "in_progress",
  schema_version = 1L,
  created_at = as.character(Sys.time()),
  updated_at = created_at,
  metadata = list()
) {
  structure(
    list(
      schema_version = as.integer(schema_version),
      checkpoint_id = checkpoint_id,
      checkpoint_dir = checkpoint_dir,
      status = status,
      created_at = created_at,
      updated_at = updated_at,
      run_signature = run_signature,
      config_signature = config_signature,
      grouping_signature = grouping_signature,
      source_metadata = source_metadata,
      planned_groups = as.character(planned_groups),
      completed_groups = as.character(completed_groups),
      failed_groups = as.character(failed_groups),
      pending_groups = as.character(pending_groups),
      per_group_artifacts = per_group_artifacts,
      metadata = metadata
    ),
    class = c("refiner_checkpoint_manifest", "list")
  )
}

sanitize_checkpoint_path_segment <- function(value) {
  sanitized_value <- gsub("[^A-Za-z0-9_-]+", "_", as.character(value))
  sanitized_value <- gsub("_+", "_", sanitized_value)
  sanitized_value <- gsub("^_+|_+$", "", sanitized_value)

  if (!nzchar(sanitized_value)) {
    return("group")
  }

  sanitized_value
}

build_group_artifact_filename <- function(group_label) {
  sprintf("group_%s.rds", sanitize_checkpoint_path_segment(group_label))
}

normalize_checkpoint_character_vector <- function(value) {
  if (is.null(value)) {
    return(character())
  }

  as.character(unlist(value, use.names = FALSE))
}

assert_valid_checkpoint_manifest <- function(manifest) {
  required_fields <- c(
    "schema_version", "checkpoint_id", "checkpoint_dir", "status", "created_at", "updated_at",
    "run_signature", "config_signature", "grouping_signature", "source_metadata",
    "planned_groups", "completed_groups", "failed_groups", "pending_groups", "per_group_artifacts", "metadata"
  )
  missing_fields <- setdiff(required_fields, names(manifest))

  if (length(missing_fields) > 0) {
    stop(sprintf("Checkpoint manifest is missing required fields: %s", paste(missing_fields, collapse = ", ")), call. = FALSE)
  }

  if (!manifest$status %in% c("in_progress", "completed", "invalid")) {
    stop("Checkpoint manifest status must be one of in_progress, completed, or invalid.", call. = FALSE)
  }

  if (!is.list(manifest$per_group_artifacts)) {
    stop("Checkpoint manifest per_group_artifacts must be a list.", call. = FALSE)
  }

  manifest
}

build_checkpoint_paths <- function(checkpoint_dir) {
  list(
    manifest_path = file.path(checkpoint_dir, "manifest.json"),
    summary_path = file.path(checkpoint_dir, "metadata.txt")
  )
}

atomic_write_text_file <- function(path, contents) {
  temp_path <- sprintf("%s.tmp", path)

  writeLines(contents, con = temp_path, useBytes = TRUE)

  if (file.exists(path)) {
    unlink(path)
  }

  renamed <- file.rename(temp_path, path)

  if (!isTRUE(renamed)) {
    unlink(temp_path)
    stop(sprintf("Failed to atomically write file: %s", path), call. = FALSE)
  }

  invisible(path)
}

atomic_save_rds <- function(object, path) {
  temp_path <- sprintf("%s.tmp", path)

  saveRDS(object, file = temp_path)

  if (file.exists(path)) {
    unlink(path)
  }

  renamed <- file.rename(temp_path, path)

  if (!isTRUE(renamed)) {
    unlink(temp_path)
    stop(sprintf("Failed to atomically write RDS file: %s", path), call. = FALSE)
  }

  invisible(path)
}

write_checkpoint_manifest <- function(manifest, checkpoint_dir = manifest$checkpoint_dir) {
  validated_manifest <- assert_valid_checkpoint_manifest(manifest)

  if (!dir.exists(checkpoint_dir)) {
    dir.create(checkpoint_dir, recursive = TRUE, showWarnings = FALSE)
  }

  manifest_paths <- build_checkpoint_paths(checkpoint_dir)
  atomic_write_text_file(
    manifest_paths$manifest_path,
    jsonlite::toJSON(validated_manifest, auto_unbox = TRUE, pretty = TRUE, null = "null")
  )

  invisible(manifest_paths$manifest_path)
}

read_checkpoint_manifest <- function(checkpoint_dir) {
  manifest_path <- build_checkpoint_paths(checkpoint_dir)$manifest_path

  if (!file.exists(manifest_path)) {
    stop(sprintf("Checkpoint manifest not found: %s", manifest_path), call. = FALSE)
  }

  manifest <- jsonlite::fromJSON(manifest_path, simplifyVector = FALSE)
  manifest$checkpoint_dir <- checkpoint_dir
  manifest$planned_groups <- normalize_checkpoint_character_vector(manifest$planned_groups)
  manifest$completed_groups <- normalize_checkpoint_character_vector(manifest$completed_groups)
  manifest$failed_groups <- normalize_checkpoint_character_vector(manifest$failed_groups)
  manifest$pending_groups <- normalize_checkpoint_character_vector(manifest$pending_groups)
  assert_valid_checkpoint_manifest(manifest)
}

write_checkpoint_summary <- function(manifest, checkpoint_dir = manifest$checkpoint_dir) {
  validated_manifest <- assert_valid_checkpoint_manifest(manifest)
  manifest_paths <- build_checkpoint_paths(checkpoint_dir)
  summary_lines <- c(
    sprintf("Checkpoint ID: %s", validated_manifest$checkpoint_id),
    sprintf("Status: %s", validated_manifest$status),
    sprintf("Created: %s", validated_manifest$created_at),
    sprintf("Updated: %s", validated_manifest$updated_at),
    sprintf("Source: %s", if (!is.null(validated_manifest$source_metadata$source_label)) validated_manifest$source_metadata$source_label else ""),
    sprintf("Planned groups: %s", length(validated_manifest$planned_groups)),
    sprintf("Completed groups: %s", length(validated_manifest$completed_groups)),
    sprintf("Failed groups: %s", length(validated_manifest$failed_groups)),
    sprintf("Pending groups: %s", length(validated_manifest$pending_groups))
  )

  atomic_write_text_file(manifest_paths$summary_path, summary_lines)
}

initialize_group_checkpoint <- function(
  checkpoint_root,
  run_signature,
  config_signature,
  grouping_signature,
  source_metadata = list(),
  planned_groups = character(),
  checkpoint_id = format(Sys.time(), "%Y%m%d_%H%M%S")
) {
  if (is.null(checkpoint_root) || !nzchar(trimws(checkpoint_root))) {
    stop("checkpoint_root must be a non-empty directory path.", call. = FALSE)
  }

  checkpoint_dir <- file.path(checkpoint_root, sprintf("checkpoint_%s", sanitize_checkpoint_path_segment(checkpoint_id)))

  if (!dir.exists(checkpoint_dir)) {
    dir.create(checkpoint_dir, recursive = TRUE, showWarnings = FALSE)
  }

  manifest <- create_checkpoint_manifest(
    checkpoint_id = checkpoint_id,
    checkpoint_dir = checkpoint_dir,
    run_signature = run_signature,
    config_signature = config_signature,
    grouping_signature = grouping_signature,
    source_metadata = source_metadata,
    planned_groups = planned_groups,
    completed_groups = character(),
    failed_groups = character(),
    pending_groups = planned_groups,
    per_group_artifacts = list(),
    status = "in_progress"
  )

  write_checkpoint_manifest(manifest, checkpoint_dir)
  write_checkpoint_summary(manifest, checkpoint_dir)
  manifest
}

write_group_checkpoint_artifact <- function(checkpoint_dir, group_result) {
  if (!inherits(group_result, "refiner_group_execution_result")) {
    stop("group_result must inherit from refiner_group_execution_result.", call. = FALSE)
  }

  artifact_filename <- build_group_artifact_filename(group_result$group_label)
  artifact_path <- file.path(checkpoint_dir, artifact_filename)
  atomic_save_rds(group_result, artifact_path)

  list(
    group_label = group_result$group_label,
    artifact_filename = artifact_filename,
    artifact_path = artifact_path,
    status = group_result$status,
    updated_at = as.character(Sys.time())
  )
}

read_group_checkpoint_artifact <- function(checkpoint_dir, group_label, artifact_filename = build_group_artifact_filename(group_label)) {
  artifact_path <- file.path(checkpoint_dir, artifact_filename)

  if (!file.exists(artifact_path)) {
    stop(sprintf("Checkpoint group artifact not found: %s", artifact_path), call. = FALSE)
  }

  group_result <- readRDS(artifact_path)

  if (!inherits(group_result, "refiner_group_execution_result")) {
    stop(sprintf("Checkpoint group artifact is not a refiner_group_execution_result: %s", artifact_path), call. = FALSE)
  }

  group_result
}

update_checkpoint_manifest_group_result <- function(manifest, group_result, artifact_entry = NULL) {
  validated_manifest <- assert_valid_checkpoint_manifest(manifest)

  if (!inherits(group_result, "refiner_group_execution_result")) {
    stop("group_result must inherit from refiner_group_execution_result.", call. = FALSE)
  }

  group_label <- group_result$group_label

  if (!group_label %in% validated_manifest$planned_groups) {
    stop(sprintf("Group '%s' is not part of the checkpoint plan.", group_label), call. = FALSE)
  }

  validated_manifest$completed_groups <- setdiff(validated_manifest$completed_groups, group_label)
  validated_manifest$failed_groups <- setdiff(validated_manifest$failed_groups, group_label)
  validated_manifest$pending_groups <- setdiff(validated_manifest$pending_groups, group_label)

  if (identical(group_result$status, "completed")) {
    validated_manifest$completed_groups <- unique(c(validated_manifest$completed_groups, group_label))
  } else if (identical(group_result$status, "failed")) {
    validated_manifest$failed_groups <- unique(c(validated_manifest$failed_groups, group_label))
  } else {
    validated_manifest$pending_groups <- unique(c(validated_manifest$pending_groups, group_label))
  }

  if (is.null(artifact_entry)) {
    artifact_entry <- list(
      group_label = group_label,
      artifact_filename = build_group_artifact_filename(group_label),
      artifact_path = file.path(validated_manifest$checkpoint_dir, build_group_artifact_filename(group_label)),
      status = group_result$status,
      updated_at = as.character(Sys.time())
    )
  }

  validated_manifest$per_group_artifacts[[group_label]] <- artifact_entry
  validated_manifest$updated_at <- as.character(Sys.time())

  if (length(validated_manifest$pending_groups) == 0 && length(validated_manifest$failed_groups) == 0) {
    validated_manifest$status <- "completed"
  }

  validated_manifest
}

list_rerunnable_checkpoint_groups <- function(manifest) {
  validated_manifest <- assert_valid_checkpoint_manifest(manifest)
  unique(c(validated_manifest$pending_groups, validated_manifest$failed_groups))
}

validate_checkpoint_compatibility <- function(
  manifest,
  run_signature,
  config_signature,
  grouping_signature,
  source_type,
  planned_groups = character(),
  source_fingerprint = NULL
) {
  validated_manifest <- assert_valid_checkpoint_manifest(manifest)
  errors <- character()
  warnings <- character()

  if (!identical(source_type, "csv")) {
    errors <- c(errors, "Checkpoint resume is only supported for grouped CSV runs.")
  }

  if (!identical(validated_manifest$run_signature, run_signature)) {
    errors <- c(errors, "Current data selection does not match the stored checkpoint run signature.")
  }

  if (!identical(validated_manifest$config_signature, config_signature)) {
    errors <- c(errors, "Current execution settings do not match the stored checkpoint configuration.")
  }

  if (!identical(validated_manifest$grouping_signature, grouping_signature)) {
    errors <- c(errors, "Current grouping settings do not match the stored checkpoint grouping signature.")
  }

  if (length(planned_groups) > 0 && !identical(as.character(validated_manifest$planned_groups), as.character(planned_groups))) {
    errors <- c(errors, "Current planned groups do not match the stored checkpoint manifest.")
  }

  stored_fingerprint <- validated_manifest$source_metadata$source_fingerprint

  if (!is.null(source_fingerprint) && nzchar(source_fingerprint) && !identical(stored_fingerprint, source_fingerprint)) {
    errors <- c(errors, "Current CSV source does not match the stored checkpoint file fingerprint.")
  }

  if (identical(validated_manifest$status, "invalid")) {
    errors <- c(errors, "Checkpoint manifest is marked invalid and cannot be resumed.")
  }

  if (identical(validated_manifest$status, "completed") && length(list_rerunnable_checkpoint_groups(validated_manifest)) == 0) {
    warnings <- c(warnings, "Checkpoint is already complete; no remaining groups require resume.")
  }

  create_preflight_result(
    config = list(
      manifest = validated_manifest,
      rerunnable_groups = list_rerunnable_checkpoint_groups(validated_manifest)
    ),
    errors = errors,
    warnings = warnings
  )
}

load_checkpoint_group_results <- function(manifest, statuses = c("completed")) {
  validated_manifest <- assert_valid_checkpoint_manifest(manifest)
  restored_results <- vector("list", length(validated_manifest$planned_groups))
  names(restored_results) <- validated_manifest$planned_groups

  for (group_label in validated_manifest$planned_groups) {
    artifact_entry <- validated_manifest$per_group_artifacts[[group_label]]

    if (is.null(artifact_entry) || !artifact_entry$status %in% statuses) {
      next
    }

    restored_results[[group_label]] <- read_group_checkpoint_artifact(
      checkpoint_dir = validated_manifest$checkpoint_dir,
      group_label = group_label,
      artifact_filename = artifact_entry$artifact_filename
    )
  }

  restored_results
}

discard_checkpoint_run <- function(checkpoint_dir) {
  if (!dir.exists(checkpoint_dir)) {
    return(invisible(FALSE))
  }

  unlink(checkpoint_dir, recursive = TRUE, force = TRUE)
  invisible(!dir.exists(checkpoint_dir))
}

list_checkpoint_run_dirs <- function(checkpoint_root) {
  if (is.null(checkpoint_root) || !nzchar(trimws(checkpoint_root)) || !dir.exists(checkpoint_root)) {
    return(character())
  }

  run_dirs <- list.dirs(checkpoint_root, full.names = TRUE, recursive = FALSE)
  run_dirs[grepl("checkpoint_", basename(run_dirs), fixed = TRUE)]
}

read_checkpoint_manifest_safe <- function(checkpoint_dir) {
  tryCatch(
    read_checkpoint_manifest(checkpoint_dir),
    error = function(error) {
      list(
        checkpoint_dir = checkpoint_dir,
        read_error = conditionMessage(error)
      )
    }
  )
}

find_compatible_checkpoint_manifest <- function(
  checkpoint_root,
  run_signature,
  config_signature,
  grouping_signature,
  source_type,
  planned_groups = character(),
  source_fingerprint = NULL
) {
  candidate_dirs <- list_checkpoint_run_dirs(checkpoint_root)

  if (length(candidate_dirs) == 0) {
    return(NULL)
  }

  for (checkpoint_dir in rev(sort(candidate_dirs))) {
    candidate_manifest <- read_checkpoint_manifest_safe(checkpoint_dir)

    if (!is.null(candidate_manifest$read_error)) {
      next
    }

    compatibility <- validate_checkpoint_compatibility(
      candidate_manifest,
      run_signature = run_signature,
      config_signature = config_signature,
      grouping_signature = grouping_signature,
      source_type = source_type,
      planned_groups = planned_groups,
      source_fingerprint = source_fingerprint
    )

    if (isTRUE(compatibility$ok)) {
      return(candidate_manifest)
    }
  }

  NULL
}

normalize_settings_scalar <- function(value, field_name, allow_empty = TRUE) {
  if (is.null(value) || length(value) == 0) {
    if (allow_empty) {
      return("")
    }

    stop(sprintf("%s must be provided.", field_name), call. = FALSE)
  }

  normalized_value <- trimws(as.character(value[[1]]))

  if (!allow_empty && !nzchar(normalized_value)) {
    stop(sprintf("%s must be provided.", field_name), call. = FALSE)
  }

  normalized_value
}

normalize_experiment_source_reference <- function(source_reference = list()) {
  defaults <- list(
    source_type = "sample",
    source_label = "",
    source_path = "",
    source_fingerprint = ""
  )
  normalized_reference <- utils::modifyList(defaults, source_reference)

  normalized_reference$source_type <- normalize_settings_scalar(normalized_reference$source_type, "source_type", allow_empty = FALSE)
  normalized_reference$source_label <- normalize_settings_scalar(normalized_reference$source_label, "source_label", allow_empty = FALSE)
  normalized_reference$source_path <- normalize_settings_scalar(normalized_reference$source_path, "source_path")
  normalized_reference$source_fingerprint <- normalize_settings_scalar(normalized_reference$source_fingerprint, "source_fingerprint")

  if (!normalized_reference$source_type %in% c("sample", "csv")) {
    stop("source_type must be either sample or csv.", call. = FALSE)
  }

  if (identical(normalized_reference$source_type, "sample")) {
    normalized_reference$source_path <- ""

    if (!nzchar(normalized_reference$source_fingerprint)) {
      normalized_reference$source_fingerprint <- normalized_reference$source_label
    }
  }

  if (identical(normalized_reference$source_type, "csv") && !nzchar(normalized_reference$source_fingerprint)) {
    stop("CSV settings must include a source_fingerprint.", call. = FALSE)
  }

  normalized_reference
}

normalize_experiment_data_selection <- function(data_selection = list()) {
  defaults <- list(
    analyte_column = "",
    sex_column = "",
    age_column = ""
  )
  normalized_selection <- utils::modifyList(defaults, data_selection)

  normalized_selection$analyte_column <- normalize_settings_scalar(normalized_selection$analyte_column, "analyte_column", allow_empty = FALSE)
  normalized_selection$sex_column <- normalize_settings_scalar(normalized_selection$sex_column, "sex_column")
  normalized_selection$age_column <- normalize_settings_scalar(normalized_selection$age_column, "age_column")

  normalized_selection
}

normalize_experiment_grouping <- function(grouping = list()) {
  defaults <- list(
    mode = "overall",
    sex_mapping_text = "",
    age_band_text = ""
  )
  normalized_grouping <- utils::modifyList(defaults, grouping)

  normalized_grouping$mode <- normalize_settings_scalar(normalized_grouping$mode, "grouping mode", allow_empty = FALSE)
  normalized_grouping$sex_mapping_text <- normalize_settings_scalar(normalized_grouping$sex_mapping_text, "sex_mapping_text")
  normalized_grouping$age_band_text <- normalize_settings_scalar(normalized_grouping$age_band_text, "age_band_text")

  if (!normalized_grouping$mode %in% c("overall", "sex", "age", "sex_age")) {
    stop("grouping mode must be one of overall, sex, age, or sex_age.", call. = FALSE)
  }

  normalized_grouping
}

build_experiment_settings_summary <- function(
  source_reference,
  data_selection,
  grouping,
  execution,
  display
) {
  grouping_label <- switch(
    grouping$mode,
    overall = "Overall only",
    sex = "Sex only",
    age = "Age only",
    sex_age = "Sex by age",
    grouping$mode
  )
  source_label <- sprintf("%s:%s", source_reference$source_type, source_reference$source_label)

  paste(
    source_label,
    sprintf("Analyte=%s", data_selection$analyte_column),
    sprintf("Grouping=%s", grouping_label),
    sprintf("Model=%s", execution$model),
    sprintf("Bootstrap=%s", execution$NBootstrap),
    sprintf("Display=%s/%s", display$pointEst, display$uncertaintyRegion),
    sep = " | "
  )
}

create_experiment_settings <- function(
  source_reference = list(),
  data_selection = list(),
  grouping = list(),
  execution = list(),
  display = list(),
  schema_version = 1L,
  exported_at = as.character(Sys.time()),
  summary = NULL,
  metadata = list()
) {
  normalized_schema_version <- normalize_integer_like(schema_version, "schema_version")

  if (!identical(normalized_schema_version, 1L)) {
    stop(sprintf("Unsupported experiment settings schema_version: %s", normalized_schema_version), call. = FALSE)
  }

  normalized_source_reference <- normalize_experiment_source_reference(source_reference)
  normalized_data_selection <- normalize_experiment_data_selection(data_selection)
  normalized_grouping <- normalize_experiment_grouping(grouping)
  normalized_execution <- normalize_execution_config(execution)
  normalized_display <- normalize_display_config(display, normalized_execution)
  normalized_summary <- if (is.null(summary) || !nzchar(trimws(as.character(summary[[1]])))) {
    build_experiment_settings_summary(
      source_reference = normalized_source_reference,
      data_selection = normalized_data_selection,
      grouping = normalized_grouping,
      execution = normalized_execution,
      display = normalized_display
    )
  } else {
    trimws(as.character(summary[[1]]))
  }

  if (!is.list(metadata)) {
    stop("metadata must be a list.", call. = FALSE)
  }

  structure(
    list(
      schema_version = normalized_schema_version,
      exported_at = normalize_settings_scalar(exported_at, "exported_at", allow_empty = FALSE),
      summary = normalized_summary,
      source_reference = normalized_source_reference,
      data_selection = normalized_data_selection,
      grouping = normalized_grouping,
      execution = normalized_execution,
      display = normalized_display,
      metadata = metadata
    ),
    class = c("refiner_experiment_settings", "list")
  )
}

assert_valid_experiment_settings <- function(settings) {
  required_fields <- c(
    "schema_version", "exported_at", "summary", "source_reference",
    "data_selection", "grouping", "execution", "display", "metadata"
  )
  missing_fields <- setdiff(required_fields, names(settings))

  if (length(missing_fields) > 0) {
    stop(sprintf("Experiment settings are missing required fields: %s", paste(missing_fields, collapse = ", ")), call. = FALSE)
  }

  create_experiment_settings(
    source_reference = settings$source_reference,
    data_selection = settings$data_selection,
    grouping = settings$grouping,
    execution = settings$execution,
    display = settings$display,
    schema_version = settings$schema_version,
    exported_at = settings$exported_at,
    summary = settings$summary,
    metadata = settings$metadata
  )
}

write_experiment_settings <- function(settings, path) {
  if (is.null(path) || !nzchar(trimws(path))) {
    stop("path must be a non-empty file path.", call. = FALSE)
  }

  validated_settings <- assert_valid_experiment_settings(settings)
  target_dir <- dirname(path)

  if (!dir.exists(target_dir)) {
    dir.create(target_dir, recursive = TRUE, showWarnings = FALSE)
  }

  atomic_write_text_file(
    path,
    jsonlite::toJSON(validated_settings, auto_unbox = TRUE, pretty = TRUE, null = "null")
  )

  invisible(path)
}

read_experiment_settings <- function(path) {
  if (is.null(path) || !nzchar(trimws(path)) || !file.exists(path)) {
    stop("Experiment settings path is missing or does not exist.", call. = FALSE)
  }

  settings <- jsonlite::fromJSON(path, simplifyVector = FALSE)
  assert_valid_experiment_settings(settings)
}

create_preflight_result <- function(config, errors = character(), warnings = character()) {
  structure(
    list(
      ok = length(errors) == 0,
      config = config,
      errors = errors,
      warnings = warnings
    ),
    class = c("refiner_preflight_result", "list")
  )
}

default_execution_config <- function() {
  list(
    model = "BoxCox",
    NBootstrap = 0L,
    seed = 123L,
    RIperc = c(0.025, 0.975),
    CIprop = 0.95,
    UMprop = 0.90,
    pointEst = "fullDataEst",
    Scale = "original"
  )
}

default_display_config <- function(execution_config = default_execution_config()) {
  list(
    RIperc = execution_config$RIperc,
    CIprop = execution_config$CIprop,
    UMprop = execution_config$UMprop,
    pointEst = execution_config$pointEst,
    Scale = execution_config$Scale,
    Nhist = 60L,
    showMargin = TRUE,
    showPathol = FALSE,
    showValue = TRUE,
    uncertaintyRegion = "uncertaintyMargin",
    colScheme = "green"
  )
}

parse_probability_vector <- function(value) {
  if (is.list(value)) {
    value <- unlist(value, use.names = FALSE)
  }

  if (is.numeric(value)) {
    return(as.numeric(value))
  }

  if (length(value) > 1) {
    suppressWarnings(parsed_values <- as.numeric(value))

    if (!any(is.na(parsed_values))) {
      return(parsed_values)
    }
  }

  if (length(value) != 1 || is.null(value) || !nzchar(trimws(value))) {
    stop("RIperc must be provided as a comma-separated numeric vector.", call. = FALSE)
  }

  parsed_values <- strsplit(value, ",", fixed = TRUE)[[1]]
  parsed_values <- trimws(parsed_values)

  if (length(parsed_values) == 0 || any(parsed_values == "")) {
    stop("RIperc must contain only numeric percentile values.", call. = FALSE)
  }

  suppressWarnings(as.numeric(parsed_values))
}

normalize_integer_like <- function(value, field_name) {
  normalized_value <- suppressWarnings(as.numeric(value))

  if (length(normalized_value) != 1 || is.na(normalized_value) || normalized_value %% 1 != 0) {
    stop(sprintf("%s must be an integer value.", field_name), call. = FALSE)
  }

  as.integer(normalized_value)
}

normalize_probability_scalar <- function(value, field_name) {
  normalized_value <- suppressWarnings(as.numeric(value))

  if (length(normalized_value) != 1 || is.na(normalized_value)) {
    stop(sprintf("%s must be numeric.", field_name), call. = FALSE)
  }

  if (normalized_value <= 0 || normalized_value >= 1) {
    stop(sprintf("%s must be strictly between 0 and 1.", field_name), call. = FALSE)
  }

  normalized_value
}

normalize_execution_config <- function(config) {
  defaults <- default_execution_config()
  merged_config <- utils::modifyList(defaults, config)

  merged_config$model <- as.character(merged_config$model)
  merged_config$pointEst <- as.character(merged_config$pointEst)
  merged_config$Scale <- as.character(merged_config$Scale)
  merged_config$NBootstrap <- normalize_integer_like(merged_config$NBootstrap, "NBootstrap")
  merged_config$seed <- normalize_integer_like(merged_config$seed, "seed")
  merged_config$RIperc <- parse_probability_vector(merged_config$RIperc)
  merged_config$CIprop <- normalize_probability_scalar(merged_config$CIprop, "CIprop")
  merged_config$UMprop <- normalize_probability_scalar(merged_config$UMprop, "UMprop")

  merged_config
}

normalize_logical_flag <- function(value, field_name) {
  normalized_value <- suppressWarnings(as.logical(value))

  if (length(normalized_value) != 1 || is.na(normalized_value)) {
    stop(sprintf("%s must be TRUE or FALSE.", field_name), call. = FALSE)
  }

  normalized_value
}

normalize_display_config <- function(display_config = list(), execution_config = default_execution_config()) {
  validated_execution_config <- assert_valid_execution_config(execution_config)
  defaults <- default_display_config(validated_execution_config)
  merged_config <- utils::modifyList(defaults, display_config)

  merged_config$RIperc <- parse_probability_vector(merged_config$RIperc)
  merged_config$CIprop <- normalize_probability_scalar(merged_config$CIprop, "CIprop")
  merged_config$UMprop <- normalize_probability_scalar(merged_config$UMprop, "UMprop")
  merged_config$pointEst <- as.character(merged_config$pointEst)
  merged_config$Scale <- as.character(merged_config$Scale)
  merged_config$Nhist <- normalize_integer_like(merged_config$Nhist, "Nhist")
  merged_config$showMargin <- normalize_logical_flag(merged_config$showMargin, "showMargin")
  merged_config$showPathol <- normalize_logical_flag(merged_config$showPathol, "showPathol")
  merged_config$showValue <- normalize_logical_flag(merged_config$showValue, "showValue")
  merged_config$uncertaintyRegion <- as.character(merged_config$uncertaintyRegion)
  merged_config$colScheme <- as.character(merged_config$colScheme)

  merged_config
}

validate_execution_config <- function(config) {
  required_fields <- c("model", "NBootstrap", "seed", "RIperc", "CIprop", "UMprop", "pointEst", "Scale")
  missing_fields <- setdiff(required_fields, names(config))

  if (length(missing_fields) > 0) {
    return(create_preflight_result(
      config = config,
      errors = sprintf("Execution config is missing required fields: %s", paste(missing_fields, collapse = ", "))
    ))
  }

  errors <- character()
  warnings <- character()

  normalized_config <- tryCatch(
    normalize_execution_config(config),
    error = function(error) {
      errors <<- c(errors, conditionMessage(error))
      NULL
    }
  )

  if (is.null(normalized_config)) {
    return(create_preflight_result(config = config, errors = errors, warnings = warnings))
  }

  if (!normalized_config$model %in% c("BoxCox", "modBoxCoxFast", "modBoxCox")) {
    errors <- c(errors, "model must be one of BoxCox, modBoxCoxFast, or modBoxCox.")
  }

  if (normalized_config$NBootstrap < 0) {
    errors <- c(errors, "NBootstrap must be zero or greater.")
  }

  if (length(normalized_config$RIperc) < 2) {
    errors <- c(errors, "RIperc must contain at least two percentile values.")
  }

  if (any(is.na(normalized_config$RIperc))) {
    errors <- c(errors, "RIperc must contain only numeric percentile values.")
  } else {
    if (any(normalized_config$RIperc <= 0 | normalized_config$RIperc >= 1)) {
      errors <- c(errors, "RIperc values must be strictly between 0 and 1.")
    }

    if (is.unsorted(normalized_config$RIperc, strictly = TRUE)) {
      errors <- c(errors, "RIperc values must be sorted in ascending order with no duplicates.")
    }
  }

  if (!normalized_config$pointEst %in% c("fullDataEst", "medianBS")) {
    errors <- c(errors, "pointEst must be either fullDataEst or medianBS.")
  }

  if (identical(normalized_config$pointEst, "medianBS") && normalized_config$NBootstrap <= 0) {
    errors <- c(errors, "pointEst = medianBS requires NBootstrap greater than 0.")
  }

  if (!normalized_config$Scale %in% c("original", "transformed", "zScore")) {
    errors <- c(errors, "Scale must be one of original, transformed, or zScore.")
  }

  if (normalized_config$NBootstrap == 0) {
    warnings <- c(warnings, "Bootstrap is disabled, so bootstrap-derived confidence intervals may remain unavailable.")
  }

  create_preflight_result(
    config = normalized_config,
    errors = errors,
    warnings = warnings
  )
}

validate_display_config <- function(display_config = list(), execution_config = default_execution_config()) {
  execution_preflight <- validate_execution_config(execution_config)

  if (!isTRUE(execution_preflight$ok)) {
    return(create_preflight_result(
      config = display_config,
      errors = sprintf(
        "Display config requires a valid execution config first: %s",
        paste(execution_preflight$errors, collapse = " ")
      )
    ))
  }

  errors <- character()
  warnings <- character()

  normalized_config <- tryCatch(
    normalize_display_config(display_config, execution_preflight$config),
    error = function(error) {
      errors <<- c(errors, conditionMessage(error))
      NULL
    }
  )

  if (is.null(normalized_config)) {
    return(create_preflight_result(config = display_config, errors = errors, warnings = warnings))
  }

  if (length(normalized_config$RIperc) < 2) {
    errors <- c(errors, "RIperc must contain at least two percentile values.")
  }

  if (any(is.na(normalized_config$RIperc))) {
    errors <- c(errors, "RIperc must contain only numeric percentile values.")
  } else {
    if (any(normalized_config$RIperc <= 0 | normalized_config$RIperc >= 1)) {
      errors <- c(errors, "RIperc values must be strictly between 0 and 1.")
    }

    if (is.unsorted(normalized_config$RIperc, strictly = TRUE)) {
      errors <- c(errors, "RIperc values must be sorted in ascending order with no duplicates.")
    }
  }

  if (!normalized_config$pointEst %in% c("fullDataEst", "medianBS")) {
    errors <- c(errors, "pointEst must be either fullDataEst or medianBS.")
  }

  if (!normalized_config$Scale %in% c("original", "transformed", "zScore")) {
    errors <- c(errors, "Scale must be one of original, transformed, or zScore.")
  }

  if (normalized_config$Nhist <= 0) {
    errors <- c(errors, "Nhist must be greater than 0.")
  }

  if (!normalized_config$uncertaintyRegion %in% c("bootstrapCI", "uncertaintyMargin")) {
    errors <- c(errors, "uncertaintyRegion must be either bootstrapCI or uncertaintyMargin.")
  }

  if (!normalized_config$colScheme %in% c("green", "blue")) {
    errors <- c(errors, "colScheme must be either green or blue.")
  }

  if (identical(normalized_config$pointEst, "medianBS") && execution_preflight$config$NBootstrap <= 0) {
    errors <- c(errors, "pointEst = medianBS requires NBootstrap greater than 0 in the executed fit.")
  }

  if (identical(normalized_config$uncertaintyRegion, "bootstrapCI") && execution_preflight$config$NBootstrap <= 0) {
    errors <- c(errors, "uncertaintyRegion = bootstrapCI requires a fit executed with NBootstrap greater than 0.")
  }

  create_preflight_result(
    config = normalized_config,
    errors = errors,
    warnings = warnings
  )
}

assert_valid_execution_config <- function(config) {
  preflight <- validate_execution_config(config)

  if (!isTRUE(preflight$ok)) {
    stop(paste(preflight$errors, collapse = " "), call. = FALSE)
  }

  preflight$config
}

assert_valid_display_config <- function(display_config = list(), execution_config = default_execution_config()) {
  preflight <- validate_display_config(display_config, execution_config)

  if (!isTRUE(preflight$ok)) {
    stop(paste(preflight$errors, collapse = " "), call. = FALSE)
  }

  preflight$config
}

derive_group_seed <- function(seed, group_index) {
  normalized_seed <- normalize_integer_like(seed, "seed")
  max_seed <- .Machine$integer.max - 1L

  as.integer(((as.double(normalized_seed) - 1 + as.double(group_index) - 1) %% max_seed) + 1)
}

build_group_membership_labels <- function(ingest_result, group_plan) {
  total_rows <- length(ingest_result$normalized_values)

  if (identical(group_plan$mode, "overall")) {
    return(rep("Overall", total_rows))
  }

  sex_labels <- rep(NA_character_, total_rows)
  age_labels <- rep(NA_character_, total_rows)

  if (group_plan$mode %in% c("sex", "sex_age")) {
    normalized_keys <- normalize_lookup_value(ingest_result$sex_values)
    non_missing_rows <- !is.na(normalized_keys)
    sex_mapping <- group_plan$metadata$sex_mapping
    sex_labels[non_missing_rows] <- unname(sex_mapping[normalized_keys[non_missing_rows]])
  }

  if (group_plan$mode %in% c("age", "sex_age")) {
    non_missing_rows <- !is.na(ingest_result$age_values)

    if (any(non_missing_rows)) {
      age_labels[non_missing_rows] <- match_age_band(
        ingest_result$age_values[non_missing_rows],
        group_plan$metadata$age_bands
      )
    }
  }

  switch(
    group_plan$mode,
    sex = sex_labels,
    age = age_labels,
    sex_age = paste(sex_labels, age_labels, sep = " / "),
    rep(NA_character_, total_rows)
  )
}

extract_group_row_indices <- function(ingest_result, group_plan, group_label) {
  membership_labels <- build_group_membership_labels(ingest_result, group_plan)
  which(!is.na(membership_labels) & membership_labels == group_label)
}

run_group_refiner_estimation <- function(ingest_result, group_plan, config, group_index, warning_group_size = 5L) {
  validated_config <- assert_valid_execution_config(config)

  if (!inherits(group_plan, "refiner_group_plan") || !isTRUE(group_plan$ok)) {
    stop("Grouped execution requires a valid group plan.", call. = FALSE)
  }

  if (!isTRUE(ingest_result$ready)) {
    stop("Grouped execution requires validated analyte data.", call. = FALSE)
  }

  if (!is.numeric(group_index) || length(group_index) != 1 || is.na(group_index) || group_index < 1 || group_index > nrow(group_plan$groups)) {
    stop("group_index must identify one planned group.", call. = FALSE)
  }

  group_index <- as.integer(group_index)
  group_label <- group_plan$groups$group_label[[group_index]]
  row_indices <- extract_group_row_indices(ingest_result, group_plan, group_label)
  group_size <- length(row_indices)
  warning_messages <- character()

  if (group_size < warning_group_size) {
    warning_messages <- c(
      warning_messages,
      sprintf(
        "Group '%s' has only %s usable row(s); interpret the RI with caution.",
        group_label,
        group_size
      )
    )
  }

  group_config <- validated_config
  group_config$seed <- derive_group_seed(validated_config$seed, group_index)

  t_group_start <- proc.time()
  execution_result <- run_refiner_estimation(
    analyte_values = ingest_result$normalized_values[row_indices],
    config = group_config
  )
  elapsed_seconds <- unname((proc.time() - t_group_start)[["elapsed"]])

  execution_result$metadata$group_label <- group_label
  execution_result$metadata$group_size <- group_size
  execution_result$metadata$row_indices <- row_indices
  execution_result$metadata$elapsed_seconds <- elapsed_seconds

  create_group_execution_result(
    group_label = group_label,
    group_size = group_size,
    row_indices = row_indices,
    status = if (isTRUE(execution_result$success)) "completed" else "failed",
    execution_result = execution_result,
    warning_messages = warning_messages,
    error_message = if (!isTRUE(execution_result$success)) execution_result$error_message else NULL,
    metadata = list(seed = group_config$seed, elapsed_seconds = elapsed_seconds)
  )
}

build_group_summary_table <- function(group_results) {
  if (length(group_results) == 0) {
    return(data.frame())
  }

  rows <- lapply(group_results, function(group_result) {
    interval_table <- if (!is.null(group_result$execution_result)) group_result$execution_result$interval_table else NULL
    lower_bound <- NA_character_
    upper_bound <- NA_character_

    if (is.data.frame(interval_table) && nrow(interval_table) >= 2 && "PointEst" %in% names(interval_table)) {
      lower_bound <- as.character(interval_table$PointEst[[1]])
      upper_bound <- as.character(interval_table$PointEst[[nrow(interval_table)]])
    }

    data.frame(
      group_label = group_result$group_label,
      row_count = group_result$group_size,
      status = group_result$status,
      lower_bound = lower_bound,
      upper_bound = upper_bound,
      warnings = if (length(group_result$warning_messages) > 0) paste(group_result$warning_messages, collapse = " ") else "",
      error = if (!is.null(group_result$error_message)) group_result$error_message else "",
      stringsAsFactors = FALSE,
      check.names = FALSE
    )
  })

  do.call(rbind, rows)
}

assemble_grouped_execution_result <- function(group_results, group_plan, config, metadata = list()) {
  validated_config <- assert_valid_execution_config(config)
  summary_table <- build_group_summary_table(group_results)
  collected_warnings <- unlist(lapply(group_results, function(group_result) group_result$warning_messages), use.names = FALSE)
  collected_errors <- unlist(lapply(group_results, function(group_result) if (!is.null(group_result$error_message)) group_result$error_message else character()), use.names = FALSE)
  groups_completed <- sum(vapply(group_results, function(group_result) identical(group_result$status, "completed"), logical(1)))
  groups_failed <- sum(vapply(group_results, function(group_result) identical(group_result$status, "failed"), logical(1)))
  default_metadata <- list(
    groups_attempted = length(group_results),
    groups_completed = groups_completed,
    groups_failed = groups_failed,
    groups_skipped = 0L,
    group_timing = lapply(group_results, function(group_result) {
      list(group_label = group_result$group_label, elapsed_seconds = group_result$metadata$elapsed_seconds)
    })
  )

  create_grouped_execution_result(
    success_overall = groups_completed > 0,
    groups_results = group_results,
    summary_table = summary_table,
    config = validated_config,
    grouping_metadata = list(
      mode = group_plan$mode,
      group_plan = group_plan,
      sex_mapping = group_plan$metadata$sex_mapping,
      age_bands = group_plan$metadata$age_bands
    ),
    metadata = utils::modifyList(default_metadata, metadata),
    warnings = collected_warnings,
    errors = collected_errors
  )
}

extract_group_result <- function(grouped_result, group_label) {
  if (!inherits(grouped_result, "refiner_grouped_execution_result")) {
    return(NULL)
  }

  matching_result <- Filter(function(group_result) identical(group_result$group_label, group_label), grouped_result$groups_results)

  if (length(matching_result) == 0) {
    return(NULL)
  }

  matching_result[[1]]$execution_result
}

run_grouped_refiner_estimation <- function(ingest_result, group_plan, config, warning_group_size = 5L) {
  validated_config <- assert_valid_execution_config(config)

  if (!inherits(group_plan, "refiner_group_plan") || !isTRUE(group_plan$ok)) {
    stop("Grouped execution requires a valid group plan.", call. = FALSE)
  }

  if (!isTRUE(ingest_result$ready)) {
    stop("Grouped execution requires validated analyte data.", call. = FALSE)
  }

  if (!is.data.frame(group_plan$groups) || nrow(group_plan$groups) == 0) {
    stop("Grouped execution requires at least one planned group.", call. = FALSE)
  }

  group_results <- vector("list", nrow(group_plan$groups))
  group_timing <- vector("list", nrow(group_plan$groups))
  collected_warnings <- character()
  collected_errors <- character()
  t_grouped_start <- proc.time()

  for (index in seq_len(nrow(group_plan$groups))) {
    group_results[[index]] <- run_group_refiner_estimation(
      ingest_result = ingest_result,
      group_plan = group_plan,
      config = validated_config,
      group_index = index,
      warning_group_size = warning_group_size
    )

    if (!isTRUE(group_results[[index]]$execution_result$success) && !is.null(group_results[[index]]$error_message)) {
      collected_errors <- c(collected_errors, sprintf("%s: %s", group_results[[index]]$group_label, group_results[[index]]$error_message))
    }

    if (length(group_results[[index]]$warning_messages) > 0) {
      collected_warnings <- c(collected_warnings, group_results[[index]]$warning_messages)
    }

    group_timing[[index]] <- list(
      group_label = group_results[[index]]$group_label,
      elapsed_seconds = group_results[[index]]$metadata$elapsed_seconds
    )
  }

  total_elapsed_seconds <- unname((proc.time() - t_grouped_start)[["elapsed"]])

  assemble_grouped_execution_result(
    group_results = group_results,
    group_plan = group_plan,
    config = validated_config,
    metadata = list(
      total_elapsed_seconds = total_elapsed_seconds,
      completed_at = as.character(Sys.time())
    )
  )
}

run_refiner_estimation <- function(analyte_values, config) {
  validated_config <- assert_valid_execution_config(config)

  tryCatch({
    values <- as.numeric(analyte_values)

    if (length(values) == 0) {
      stop("Execution requires at least one usable numeric analyte value.", call. = FALSE)
    }

    if (anyNA(values)) {
      stop("Execution cannot proceed with missing analyte values.", call. = FALSE)
    }

    elapsed <- system.time({
      fit <- refineR::findRI(
        Data = values,
        model = validated_config$model,
        NBootstrap = validated_config$NBootstrap,
        seed = validated_config$seed
      )

      interval_table <- extract_reference_interval(fit, validated_config)
    })

    create_execution_result(
      success = TRUE,
      fit = fit,
      interval_table = interval_table,
      config = validated_config,
      metadata = list(
        analyte_length = length(values),
        fit_class = class(fit),
        elapsed_seconds = unname(elapsed[["elapsed"]]),
        completed_at = as.character(Sys.time())
      )
    )
  }, error = function(error) {
    create_execution_result(
      success = FALSE,
      config = validated_config,
      metadata = list(
        analyte_length = length(analyte_values),
        completed_at = as.character(Sys.time())
      ),
      error_message = conditionMessage(error)
    )
  })
}

extract_reference_interval <- function(model_result, options = list()) {
  refineR::getRI(
    model_result,
    RIperc = options$RIperc,
    CIprop = options$CIprop,
    UMprop = options$UMprop,
    pointEst = options$pointEst,
    Scale = options$Scale
  )
}

capture_refiner_summary <- function(model_result, display_options = list(), execution_config = default_execution_config()) {
  validated_config <- assert_valid_display_config(display_options, execution_config)

  summary_lines <- utils::capture.output(
    print(
      model_result,
      RIperc = validated_config$RIperc,
      uncertaintyRegion = validated_config$uncertaintyRegion,
      CIprop = validated_config$CIprop,
      UMprop = validated_config$UMprop,
      pointEst = validated_config$pointEst
    )
  )

  summary_lines[nzchar(summary_lines)]
}

build_refiner_plot <- function(model_result, plot_options = list(), execution_config = default_execution_config()) {
  validated_config <- assert_valid_display_config(plot_options, execution_config)

  plot(
    model_result,
    RIperc = validated_config$RIperc,
    Scale = validated_config$Scale,
    Nhist = validated_config$Nhist,
    showMargin = validated_config$showMargin,
    showPathol = validated_config$showPathol,
    showValue = validated_config$showValue,
    uncertaintyRegion = validated_config$uncertaintyRegion,
    CIprop = validated_config$CIprop,
    UMprop = validated_config$UMprop,
    pointEst = validated_config$pointEst,
    colScheme = validated_config$colScheme
  )

  invisible(validated_config)
}
