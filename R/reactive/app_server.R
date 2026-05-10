create_startup_status <- function() {
  package_names <- c("shiny", "refineR")
  package_checks <- vapply(
    package_names,
    FUN = requireNamespace,
    FUN.VALUE = logical(1),
    quietly = TRUE
  )

  renv_activated <- identical(
    Sys.getenv("RENV_PROJECT"),
    normalizePath(getwd(), winslash = "/", mustWork = FALSE)
  ) || file.exists("renv/activate.R")

  missing_packages <- names(package_checks)[!package_checks]
  ok <- renv_activated && all(package_checks)

  message <- if (ok) {
    "Environment initialized successfully. The app shell is ready."
  } else {
    paste(
      c(
        if (!renv_activated) {
          "renv activation was not detected."
        },
        if (length(missing_packages) > 0) {
          paste("Missing packages:", paste(missing_packages, collapse = ", "))
        }
      ),
      collapse = " "
    )
  }

  list(
    ok = ok,
    message = message,
    renv_activated = renv_activated,
    package_checks = as.list(package_checks)
  )
}

create_source_error_result <- function(source_type, source_label, message) {
  create_ingest_result(
    source_type = source_type,
    source_label = source_label,
    raw_data = data.frame(),
    numeric_candidate_columns = character(),
    selected_column = NULL,
    normalized_values = numeric(),
    usable_row_indices = integer(),
    missing_count = 0,
    row_count = 0,
    preview_data = data.frame(),
    errors = message
  )
}

format_grouping_mode <- function(mode) {
  switch(
    mode,
    overall = "Overall only",
    sex = "Sex only",
    age = "Age only",
    sex_age = "Sex by age",
    "Unknown"
  )
}

is_grouped_execution_result <- function(result) {
  inherits(result, "refiner_grouped_execution_result")
}

build_group_status_frame <- function(group_plan) {
  if (!is.data.frame(group_plan$groups) || nrow(group_plan$groups) == 0) {
    return(data.frame(
      group_label = character(),
      row_count = integer(),
      status = character(),
      warnings = character(),
      error = character(),
      stringsAsFactors = FALSE
    ))
  }

  data.frame(
    group_label = as.character(group_plan$groups$group_label),
    row_count = as.integer(group_plan$groups$row_count),
    status = rep("queued", nrow(group_plan$groups)),
    warnings = rep("", nrow(group_plan$groups)),
    error = rep("", nrow(group_plan$groups)),
    stringsAsFactors = FALSE
  )
}

format_group_choice_labels <- function(status_frame) {
  if (!is.data.frame(status_frame) || nrow(status_frame) == 0) {
    return(character())
  }

  stats::setNames(
    status_frame$group_label,
    sprintf("%s [%s | n=%s]", status_frame$group_label, status_frame$status, status_frame$row_count)
  )
}

find_group_result <- function(grouped_result, group_label) {
  if (!is_grouped_execution_result(grouped_result) || !nzchar(group_label)) {
    return(NULL)
  }

  matching_index <- which(vapply(
    grouped_result$groups_results,
    function(group_result) identical(group_result$group_label, group_label),
    logical(1)
  ))

  if (length(matching_index) == 0) {
    return(NULL)
  }

  grouped_result$groups_results[[matching_index[[1]]]]
}

pick_default_group_label <- function(grouped_result) {
  if (!is_grouped_execution_result(grouped_result) || length(grouped_result$groups_results) == 0) {
    return("")
  }

  completed_index <- which(vapply(
    grouped_result$groups_results,
    function(group_result) identical(group_result$status, "completed"),
    logical(1)
  ))

  if (length(completed_index) > 0) {
    return(grouped_result$groups_results[[completed_index[[1]]]]$group_label)
  }

  grouped_result$groups_results[[1]]$group_label
}

load_selected_source <- function(source_mode, sample_dataset, csv_path, csv_name = NULL) {
  if (identical(source_mode, "sample")) {
    return(load_sample_dataset(sample_dataset))
  }

  if (is.null(csv_path) || !nzchar(csv_path)) {
    return(NULL)
  }

  load_local_csv(csv_path, source_label = csv_name)
}

is_probably_temp_source_path <- function(path) {
  if (is.null(path) || !nzchar(path)) {
    return(FALSE)
  }

  normalized_path <- tryCatch(
    normalizePath(path, winslash = "/", mustWork = FALSE),
    error = function(error) ""
  )

  if (!nzchar(normalized_path)) {
    return(FALSE)
  }

  grepl("/AppData/Local/Temp/|/Temp/", normalized_path, ignore.case = TRUE)
}

format_validation_messages <- function(messages) {
  if (length(messages) == 0) {
    return("None")
  }

  paste(messages, collapse = " ")
}

compact_message_list <- function(messages) {
  messages <- as.character(messages)
  messages <- unique(messages[!is.na(messages) & nzchar(messages)])

  if (length(messages) == 0) {
    return(NULL)
  }

  shiny::tags$ul(
    class = "compact-message-list",
    lapply(messages, shiny::tags$li)
  )
}

build_compact_status <- function(ok, ready_title, blocked_title, summary = NULL, warnings = character(), errors = character()) {
  status_class <- if (isTRUE(ok)) "status-success" else "status-error"
  warning_items <- compact_message_list(warnings)
  error_items <- compact_message_list(errors)

  shiny::tags$div(
    class = paste("status-compact", status_class),
    shiny::tags$strong(if (isTRUE(ok)) ready_title else blocked_title),
    if (!is.null(summary) && nzchar(summary)) shiny::tags$p(summary),
    if (!is.null(warning_items)) shiny::tagList(
      shiny::tags$p(shiny::tags$strong("Warnings")),
      warning_items
    ),
    if (!is.null(error_items)) shiny::tagList(
      shiny::tags$p(shiny::tags$strong("Blocking issues")),
      error_items
    )
  )
}

build_metric_tile <- function(label, value) {
  shiny::div(
    class = "quality-metric",
    shiny::span(class = "quality-metric-value", format(value, big.mark = ",", scientific = FALSE)),
    shiny::span(class = "quality-metric-label", label)
  )
}

build_grouping_preview_frame <- function(plan) {
  if (!is.data.frame(plan$groups) || nrow(plan$groups) == 0) {
    return(data.frame())
  }

  age_rules <- character()
  if (is.data.frame(plan$metadata$age_bands) && nrow(plan$metadata$age_bands) > 0) {
    age_rules <- stats::setNames(
      format_age_band_rules(plan$metadata$age_bands),
      plan$metadata$age_bands$label
    )
  }

  rule <- rep("", nrow(plan$groups))
  if ("age_band" %in% names(plan$groups) && length(age_rules) > 0) {
    rule <- unname(age_rules[as.character(plan$groups$age_band)])
    rule[is.na(rule)] <- ""
  }

  data.frame(
    Group = as.character(plan$groups$group_label),
    Rule = rule,
    Rows = as.integer(plan$groups$row_count),
    check.names = FALSE,
    stringsAsFactors = FALSE
  )
}

build_readiness_state <- function(current_result, preflight, settings, group_plan) {
  blockers <- character()

  if (!isTRUE(current_result$ready)) {
    blockers <- c(blockers, current_result$errors)
  }

  if (!isTRUE(preflight$ok)) {
    blockers <- c(blockers, preflight$errors)
  }

  if (!identical(settings$mode, "overall") && !isTRUE(group_plan$ok)) {
    blockers <- c(blockers, group_plan$errors)
  }

  blockers <- as.character(blockers)
  blockers <- unique(blockers[!is.na(blockers) & nzchar(blockers)])
  ready <- length(blockers) == 0

  list(
    ready = ready,
    title = if (ready) "Ready to run" else "Run blocked",
    summary = if (ready) {
      if (identical(settings$mode, "overall")) {
        "Validated data and parameters are ready for a single refineR fit."
      } else {
        planned_groups <- if (is.data.frame(group_plan$groups)) nrow(group_plan$groups) else 0L
        sprintf("Validated data, parameters, and %s planned group(s) are ready.", planned_groups)
      }
    } else {
      "Resolve the blocking items below before starting refineR."
    },
    blockers = blockers
  )
}

build_preview_table <- function(ingest_result) {
  preview <- ingest_result$preview_data

  if (!is.data.frame(preview) || nrow(preview) == 0) {
    return(data.frame(Message = "No preview rows available.", stringsAsFactors = FALSE))
  }

  preview
}

build_ingest_signature <- function(ingest_result, grouping_mode = "overall", sex_mapping_text = "", age_band_text = "") {
  paste(
    ingest_result$source_type,
    ingest_result$source_label,
    ingest_result$selected_column,
    first_scalar_or_default(ingest_result$selected_sex_column, ""),
    first_scalar_or_default(ingest_result$selected_age_column, ""),
    ingest_result$row_count,
    ingest_result$missing_count,
    ingest_result$sex_missing_count,
    ingest_result$age_missing_count,
    length(ingest_result$normalized_values),
    grouping_mode,
    gsub("\\s+", " ", trimws(sex_mapping_text)),
    gsub("\\s+", " ", trimws(age_band_text)),
    sep = "::"
  )
}

build_config_signature <- function(config) {
  paste(
    config$model,
    config$NBootstrap,
    config$seed,
    paste(config$RIperc, collapse = ","),
    config$CIprop,
    config$UMprop,
    config$pointEst,
    config$Scale,
    sep = "::"
  )
}

build_grouping_signature <- function(group_plan, grouping_settings) {
  planned_group_signature <- if (is.data.frame(group_plan$groups) && nrow(group_plan$groups) > 0) {
    paste(
      sprintf("%s=%s", group_plan$groups$group_label, group_plan$groups$row_count),
      collapse = "|"
    )
  } else {
    ""
  }

  paste(
    grouping_settings$mode,
    gsub("\\s+", " ", trimws(grouping_settings$sex_mapping_text %||% "")),
    gsub("\\s+", " ", trimws(grouping_settings$age_band_text %||% "")),
    planned_group_signature,
    sep = "::"
  )
}

format_execution_defaults <- function(config) {
  format_value <- function(value) {
    if (length(value) > 1) {
      return(sprintf("c(%s)", paste(value, collapse = ", ")))
    }

    as.character(value)
  }

  paste(
    sprintf("model = %s", format_value(config$model)),
    sprintf("NBootstrap = %s", format_value(config$NBootstrap)),
    sprintf("seed = %s", format_value(config$seed)),
    sprintf("RIperc = %s", format_value(config$RIperc)),
    sprintf("CIprop = %s", format_value(config$CIprop)),
    sprintf("UMprop = %s", format_value(config$UMprop)),
    sprintf("pointEst = %s", format_value(config$pointEst)),
    sprintf("Scale = %s", format_value(config$Scale)),
    sep = " | "
  )
}

first_scalar_or_default <- function(value, default = "") {
  if (is.null(value) || length(value) == 0) {
    return(default)
  }

  as.character(value[[1]])
}

build_app_server <- function() {
  active_sessions <- 0L

  function(input, output, session) {
    active_sessions <<- active_sessions + 1L

    session$onSessionEnded(function() {
      active_sessions <<- max(0L, active_sessions - 1L)

      if (identical(active_sessions, 0L)) {
        message("All browser sessions disconnected; stopping Shiny app.")
        shiny::stopApp()
      }
    })

    startup_status <- create_startup_status()
    execution_state <- shiny::reactiveVal("idle")
    execution_result <- shiny::reactiveVal(NULL)
    active_checkpoint_manifest <- shiny::reactiveVal(NULL)
    group_execution_status <- shiny::reactiveVal(NULL)
    selected_group_label <- shiny::reactiveVal("")
    wall_elapsed_seconds <- shiny::reactiveVal(NULL)
    staged_experiment_settings <- shiny::reactiveVal(NULL)
    staged_experiment_settings_error <- shiny::reactiveVal("")
    experiment_settings_feedback <- shiny::reactiveVal(NULL)
    imported_source_override <- shiny::reactiveVal(NULL)
    applying_imported_settings <- shiny::reactiveVal(FALSE)

    sample_choices <- stats::setNames(sample_dataset_choices(), sample_dataset_choices())
    model_choices <- stats::setNames(c("BoxCox", "modBoxCoxFast", "modBoxCox"), c("BoxCox", "modBoxCoxFast", "modBoxCox"))
    point_est_choices <- stats::setNames(c("fullDataEst", "medianBS"), c("fullDataEst", "medianBS"))
    scale_choices <- stats::setNames(c("original", "transformed", "zScore"), c("original", "transformed", "zScore"))
    col_scheme_choices <- stats::setNames(c("green", "blue"), c("green", "blue"))
    default_config <- default_execution_config()
    default_display <- default_display_config(default_config)
    saved_display_config <- shiny::reactiveVal(default_display)

    shiny::updateSelectInput(session, "sample_dataset", choices = sample_choices, selected = "testcase4")
    shiny::updateSelectInput(session, "param_model", choices = model_choices, selected = default_config$model)
    shiny::updateSelectInput(session, "param_point_est", choices = point_est_choices, selected = default_config$pointEst)
    shiny::updateSelectInput(session, "param_scale", choices = scale_choices, selected = default_config$Scale)
    shiny::updateSelectInput(session, "display_point_est", choices = stats::setNames("fullDataEst", "fullDataEst"), selected = "fullDataEst")
    shiny::updateSelectInput(session, "display_uncertainty_region", choices = stats::setNames("uncertaintyMargin", "uncertaintyMargin"), selected = "uncertaintyMargin")
    shiny::updateSelectInput(session, "display_col_scheme", choices = col_scheme_choices, selected = default_display$colScheme)

    source_data <- shiny::reactive({
      imported_source <- imported_source_override()

      if (inherits(imported_source, "refiner_source_data")) {
        return(imported_source)
      }

      csv_path <- if (is.null(input$csv_file)) "" else input$csv_file$datapath
      csv_name <- if (is.null(input$csv_file)) "" else input$csv_file$name

      tryCatch(
        load_selected_source(
          source_mode = input$data_source,
          sample_dataset = input$sample_dataset,
          csv_path = csv_path,
          csv_name = csv_name
        ),
        error = function(error) {
          create_source_error_result(
            source_type = input$data_source,
            source_label = if (identical(input$data_source, "sample")) input$sample_dataset else csv_name,
            message = conditionMessage(error)
          )
        }
      )
    })

    shiny::observeEvent(list(input$data_source, input$sample_dataset, input$csv_file), {
      if (isTRUE(applying_imported_settings())) {
        return()
      }

      if (!is.null(imported_source_override())) {
        imported_source_override(NULL)
      }
    }, ignoreInit = TRUE)

    ingest_result <- shiny::reactive({
      source_value <- source_data()

      if (is.null(source_value)) {
        return(create_source_error_result(
          source_type = input$data_source,
          source_label = "",
          message = "Select a sample dataset or upload a CSV file to begin validation."
        ))
      }

      if (inherits(source_value, "refiner_ingest_result")) {
        return(source_value)
      }

      prepare_ingest_result(
        source_data = source_value,
        column_name = input$analyte_column,
        sex_column = input$sex_column,
        age_column = input$age_column
      )
    })

    current_source_metadata <- shiny::reactive({
      source_value <- source_data()

      if (inherits(source_value, "refiner_source_data")) {
        return(list(
          source_type = source_value$source_type,
          source_label = source_value$source_label,
          source_path = first_scalar_or_default(source_value$source_path, ""),
          source_fingerprint = first_scalar_or_default(source_value$source_fingerprint, "")
        ))
      }

      list(
        source_type = input$data_source,
        source_label = if (identical(input$data_source, "sample")) input$sample_dataset else first_scalar_or_default(input$csv_file$name, ""),
        source_path = "",
        source_fingerprint = ""
      )
    })

    csv_reference_path <- shiny::reactive({
      trimws(input$csv_reference_path %||% "")
    })

    csv_reference_resolution <- shiny::reactive({
      current_source <- current_source_metadata()

      if (!identical(current_source$source_type, "csv") || !nzchar(current_source$source_fingerprint)) {
        return(NULL)
      }

      resolve_experiment_settings_source_reference(list(
        source_type = "csv",
        source_label = current_source$source_label,
        source_path = csv_reference_path(),
        source_fingerprint = current_source$source_fingerprint
      ))
    })

    current_grouping_settings <- shiny::reactive({
      list(
        mode = input$grouping_mode %||% "overall",
        sex_mapping_text = input$sex_mapping_text %||% "",
        age_band_text = input$age_band_text %||% ""
      )
    })

    grouping_preflight <- shiny::reactive({
      current_result <- ingest_result()
      settings <- current_grouping_settings()

      if (!current_result$ready) {
        return(create_group_plan(
          mode = settings$mode,
          total_rows = length(current_result$normalized_values),
          errors = "Valid analyte data is required before grouped planning can proceed."
        ))
      }

      relevant_warnings <- character()
      relevant_errors <- character()

      if (settings$mode %in% c("sex", "sex_age")) {
        relevant_warnings <- c(relevant_warnings, current_result$sex_warnings)
        relevant_errors <- c(relevant_errors, current_result$sex_errors)
      }

      if (settings$mode %in% c("age", "sex_age")) {
        relevant_warnings <- c(relevant_warnings, current_result$age_warnings)
        relevant_errors <- c(relevant_errors, current_result$age_errors)
      }

      if (length(relevant_errors) > 0) {
        return(create_group_plan(
          mode = settings$mode,
          total_rows = length(current_result$normalized_values),
          warnings = relevant_warnings,
          errors = relevant_errors
        ))
      }

      plan_grouped_analysis(
        analyte_values = current_result$normalized_values,
        grouping_mode = settings$mode,
        sex_values = if (settings$mode %in% c("sex", "sex_age")) current_result$sex_values else NULL,
        sex_mapping_text = settings$sex_mapping_text,
        age_values = if (settings$mode %in% c("age", "sex_age")) current_result$age_values else NULL,
        age_band_text = settings$age_band_text
      )
    })

    current_execution_config_input <- shiny::reactive({
      list(
        model = input$param_model %||% default_config$model,
        NBootstrap = input$param_nbootstrap %||% default_config$NBootstrap,
        seed = input$param_seed %||% default_config$seed,
        RIperc = input$param_riperc %||% paste(default_config$RIperc, collapse = ", "),
        CIprop = input$param_ciprop %||% default_config$CIprop,
        UMprop = input$param_umprop %||% default_config$UMprop,
        pointEst = input$param_point_est %||% default_config$pointEst,
        Scale = input$param_scale %||% default_config$Scale
      )
    })

    execution_preflight <- shiny::reactive({
      validate_execution_config(current_execution_config_input())
    })

    current_execution_config <- shiny::reactive({
      preflight <- execution_preflight()

      if (isTRUE(preflight$ok)) {
        preflight$config
      } else {
        default_execution_config()
      }
    })

    checkpoint_root <- shiny::reactive({
      trimws(input$checkpoint_root %||% "")
    })

    checkpoint_candidate_entries <- shiny::reactive({
      current_result <- ingest_result()
      current_preflight <- execution_preflight()
      current_grouping <- current_grouping_settings()
      current_group_plan <- grouping_preflight()
      current_source <- current_source_metadata()
      selected_root <- checkpoint_root()

      if (
        !nzchar(selected_root) ||
        !isTRUE(current_result$ready) ||
        !isTRUE(current_preflight$ok) ||
        !identical(current_source$source_type, "csv") ||
        identical(current_grouping$mode, "overall") ||
        !isTRUE(current_group_plan$ok)
      ) {
        return(list())
      }

      run_signature <- build_ingest_signature(
        current_result,
        grouping_mode = current_grouping$mode,
        sex_mapping_text = current_grouping$sex_mapping_text,
        age_band_text = current_grouping$age_band_text
      )
      config_signature <- build_config_signature(current_preflight$config)
      grouping_signature <- build_grouping_signature(current_group_plan, current_grouping)
      planned_groups <- as.character(current_group_plan$groups$group_label)

      lapply(rev(sort(list_checkpoint_run_dirs(selected_root))), function(checkpoint_dir) {
        candidate_manifest <- read_checkpoint_manifest_safe(checkpoint_dir)

        if (!is.null(candidate_manifest$read_error)) {
          return(list(checkpoint_dir = checkpoint_dir, read_error = candidate_manifest$read_error))
        }

        compatibility <- validate_checkpoint_compatibility(
          candidate_manifest,
          run_signature = run_signature,
          config_signature = config_signature,
          grouping_signature = grouping_signature,
          source_type = current_source$source_type,
          planned_groups = planned_groups,
          source_fingerprint = current_source$source_fingerprint
        )

        list(
          checkpoint_dir = checkpoint_dir,
          manifest = candidate_manifest,
          compatibility = compatibility
        )
      })
    })

    compatible_checkpoint_entry <- shiny::reactive({
      candidate_entries <- checkpoint_candidate_entries()

      matching_index <- which(vapply(
        candidate_entries,
        function(entry) !is.null(entry$compatibility) && isTRUE(entry$compatibility$ok),
        logical(1)
      ))

      if (length(matching_index) == 0) {
        return(NULL)
      }

      candidate_entries[[matching_index[[1]]]]
    })

    latest_success_result <- shiny::reactive({
      result <- execution_result()

      if (is.null(result)) {
        return(NULL)
      }

      if (is_grouped_execution_result(result) && isTRUE(result$success_overall)) {
        return(result)
      }

      if (!isTRUE(result$success)) {
        return(NULL)
      }

      result
    })

    selected_group_result <- shiny::reactive({
      result <- latest_success_result()

      if (is.null(result) || !is_grouped_execution_result(result)) {
        return(NULL)
      }

      find_group_result(result, selected_group_label())
    })

    display_defaults <- shiny::reactive({
      result <- latest_success_result()
      group_result <- selected_group_result()
      execution_config <- if (!is.null(group_result) && !is.null(group_result$execution_result$config)) {
        group_result$execution_result$config
      } else if (!is.null(result)) {
        result$config
      } else {
        current_execution_config()
      }

      display_preflight <- validate_display_config(saved_display_config(), execution_config)

      if (isTRUE(display_preflight$ok)) {
        display_preflight$config
      } else {
        default_display_config(execution_config)
      }
    })

    shiny::observe({
      result <- latest_success_result()

      if (!is_grouped_execution_result(result)) {
        if (nzchar(selected_group_label())) {
          selected_group_label("")
        }

        return()
      }

      current_selection <- selected_group_label()
      available_labels <- vapply(result$groups_results, function(group_result) group_result$group_label, character(1))

      if (!nzchar(current_selection) || !current_selection %in% available_labels) {
        selected_group_label(pick_default_group_label(result))
      }
    })

    shiny::observe({
      result <- latest_success_result()
      defaults <- display_defaults()
      execution_config <- if (!is.null(result)) result$config else current_execution_config()
      bootstrap_enabled <- execution_config$NBootstrap > 0
      available_point_est_choices <- if (bootstrap_enabled) {
        point_est_choices
      } else {
        stats::setNames("fullDataEst", "fullDataEst")
      }
      available_uncertainty_choices <- if (bootstrap_enabled) {
        stats::setNames(c("uncertaintyMargin", "bootstrapCI"), c("uncertaintyMargin", "bootstrapCI"))
      } else {
        stats::setNames("uncertaintyMargin", "uncertaintyMargin")
      }
      selected_point_est <- if (defaults$pointEst %in% names(available_point_est_choices)) defaults$pointEst else names(available_point_est_choices)[[1]]
      selected_uncertainty <- if (defaults$uncertaintyRegion %in% names(available_uncertainty_choices)) defaults$uncertaintyRegion else names(available_uncertainty_choices)[[1]]

      shiny::updateSelectInput(session, "display_point_est", choices = available_point_est_choices, selected = selected_point_est)
      shiny::updateNumericInput(session, "display_nhist", value = defaults$Nhist)
      shiny::updateCheckboxInput(session, "display_show_margin", value = defaults$showMargin)
      shiny::updateCheckboxInput(session, "display_show_pathol", value = defaults$showPathol)
      shiny::updateCheckboxInput(session, "display_show_value", value = defaults$showValue)
      shiny::updateSelectInput(session, "display_uncertainty_region", choices = available_uncertainty_choices, selected = selected_uncertainty)
      shiny::updateSelectInput(session, "display_col_scheme", choices = col_scheme_choices, selected = defaults$colScheme)
    })

    shiny::observe({
      if (isTRUE(applying_imported_settings())) {
        return()
      }

      execution_config <- current_execution_config()
      input_display_config <- Filter(
        Negate(is.null),
        list(
          pointEst = input$display_point_est,
          Nhist = input$display_nhist,
          showMargin = input$display_show_margin,
          showPathol = input$display_show_pathol,
          showValue = input$display_show_value,
          uncertaintyRegion = input$display_uncertainty_region,
          colScheme = input$display_col_scheme
        )
      )
      merged_display_config <- tryCatch(
        normalize_display_config(utils::modifyList(saved_display_config(), input_display_config), execution_config),
        error = function(error) NULL
      )

      if (!is.null(merged_display_config) && !identical(merged_display_config, saved_display_config())) {
        saved_display_config(merged_display_config)
      }
    })

    current_display_config_input <- shiny::reactive({
      result <- latest_success_result()
      group_result <- selected_group_result()
      execution_config <- if (!is.null(group_result) && !is.null(group_result$execution_result$config)) {
        group_result$execution_result$config
      } else if (!is.null(result)) {
        result$config
      } else {
        current_execution_config()
      }

      list(
        RIperc = execution_config$RIperc,
        CIprop = execution_config$CIprop,
        UMprop = execution_config$UMprop,
        pointEst = input$display_point_est %||% display_defaults()$pointEst,
        Scale = execution_config$Scale,
        Nhist = input$display_nhist %||% display_defaults()$Nhist,
        showMargin = input$display_show_margin %||% display_defaults()$showMargin,
        showPathol = input$display_show_pathol %||% display_defaults()$showPathol,
        showValue = input$display_show_value %||% display_defaults()$showValue,
        uncertaintyRegion = input$display_uncertainty_region %||% display_defaults()$uncertaintyRegion,
        colScheme = input$display_col_scheme %||% display_defaults()$colScheme
      )
    })

    staged_experiment_settings_preflight <- shiny::reactive({
      staged_settings <- staged_experiment_settings()
      staged_error <- staged_experiment_settings_error()

      if (nzchar(staged_error)) {
        return(create_preflight_result(config = NULL, errors = staged_error))
      }

      if (is.null(staged_settings)) {
        return(NULL)
      }

      errors <- character()
      warnings <- character()
      source_resolution <- resolve_experiment_settings_source_reference(staged_settings$source_reference)
      current_source <- current_source_metadata()
      current_source_value <- source_data()

      if (!isTRUE(source_resolution$ok) && identical(staged_settings$source_reference$source_type, "csv")) {
        if (
          inherits(current_source_value, "refiner_source_data") &&
          identical(current_source$source_type, "csv") &&
          nzchar(current_source$source_fingerprint) &&
          identical(current_source$source_fingerprint, staged_settings$source_reference$source_fingerprint)
        ) {
          source_resolution <- create_source_reference_resolution(
            status = "resolved",
            source_reference = staged_settings$source_reference,
            source_data = current_source_value,
            warnings = c(source_resolution$warnings, "Using the currently selected CSV because the exported local reference is unavailable."),
            metadata = list(source_kind = "csv", source_origin = "current_session")
          )
        }
      } else if (isTRUE(source_resolution$ok)) {
        source_resolution$metadata$source_origin <- "stored_reference"
      }

      if (!isTRUE(source_resolution$ok)) {
        errors <- c(errors, source_resolution$errors)
      }
      warnings <- c(warnings, source_resolution$warnings)

      execution_preflight_result <- validate_execution_config(staged_settings$execution)

      if (!isTRUE(execution_preflight_result$ok)) {
        errors <- c(errors, execution_preflight_result$errors)
      }
      warnings <- c(warnings, execution_preflight_result$warnings)

      if (staged_settings$grouping$mode %in% c("sex", "sex_age")) {
        parsed_mapping <- tryCatch(
          parse_explicit_mapping(staged_settings$grouping$sex_mapping_text, field_name = "sex mapping"),
          error = function(error) error
        )

        if (inherits(parsed_mapping, "error")) {
          errors <- c(errors, conditionMessage(parsed_mapping))
        }
      }

      if (staged_settings$grouping$mode %in% c("age", "sex_age")) {
        parsed_age_bands <- tryCatch(
          parse_age_band_text(staged_settings$grouping$age_band_text),
          error = function(error) error
        )

        if (inherits(parsed_age_bands, "error")) {
          errors <- c(errors, conditionMessage(parsed_age_bands))
        }
      }

      normalized_display <- staged_settings$display
      if (isTRUE(execution_preflight_result$ok)) {
        display_preflight_result <- validate_display_config(staged_settings$display, execution_preflight_result$config)

        if (!isTRUE(display_preflight_result$ok)) {
          errors <- c(errors, display_preflight_result$errors)
        } else {
          normalized_display <- display_preflight_result$config
        }

        warnings <- c(warnings, display_preflight_result$warnings)
      }

      create_preflight_result(
        config = list(
          settings = staged_settings,
          source_resolution = source_resolution,
          execution = if (isTRUE(execution_preflight_result$ok)) execution_preflight_result$config else staged_settings$execution,
          display = normalized_display
        ),
        errors = unique(errors),
        warnings = unique(warnings)
      )
    })

    current_experiment_settings_export <- shiny::reactive({
      current_result <- ingest_result()
      current_preflight <- execution_preflight()
      current_grouping <- current_grouping_settings()
      current_group_plan <- grouping_preflight()
      current_source <- current_source_metadata()
      current_csv_reference <- csv_reference_resolution()
      errors <- character()
      warnings <- character()
      export_source_path <- current_source$source_path

      if (!isTRUE(current_result$ready)) {
        errors <- c(errors, current_result$errors)
      }

      if (!isTRUE(current_preflight$ok)) {
        errors <- c(errors, current_preflight$errors)
      }

      if (!identical(current_grouping$mode, "overall") && !isTRUE(current_group_plan$ok)) {
        errors <- c(errors, current_group_plan$errors)
      }

      if (identical(current_source$source_type, "csv")) {
        if (is.null(current_csv_reference) || !isTRUE(current_csv_reference$ok)) {
          reference_error <- if (!is.null(current_csv_reference) && length(current_csv_reference$errors) > 0) {
            paste(current_csv_reference$errors, collapse = " ")
          } else {
            "CSV settings export requires a stable local reference path that matches the uploaded file."
          }

          errors <- c(errors, reference_error)
        } else {
          export_source_path <- current_csv_reference$source_reference$source_path
        }
      }

      if (length(errors) > 0) {
        return(create_preflight_result(config = NULL, errors = unique(errors), warnings = unique(warnings)))
      }

      settings_object <- tryCatch(
        create_experiment_settings(
          source_reference = list(
            source_type = current_source$source_type,
            source_label = current_source$source_label,
            source_path = export_source_path,
            source_fingerprint = current_source$source_fingerprint
          ),
          data_selection = list(
            analyte_column = current_result$selected_column,
            sex_column = first_scalar_or_default(current_result$selected_sex_column, ""),
            age_column = first_scalar_or_default(current_result$selected_age_column, "")
          ),
          grouping = current_grouping,
          execution = current_preflight$config,
          display = current_display_config_input(),
          metadata = list(
            export_warnings = unique(warnings)
          )
        ),
        error = function(error) error
      )

      if (inherits(settings_object, "error")) {
        return(create_preflight_result(config = NULL, errors = conditionMessage(settings_object), warnings = unique(warnings)))
      }

      create_preflight_result(config = settings_object, warnings = unique(warnings))
    })

    output$download_experiment_settings <- shiny::downloadHandler(
      filename = function() {
        sprintf("experiment_settings_%s.json", format(Sys.time(), "%Y%m%d_%H%M%S"))
      },
      content = function(file) {
        export_preflight <- current_experiment_settings_export()

        if (!isTRUE(export_preflight$ok) || is.null(export_preflight$config)) {
          stop("Current experiment settings are not ready for export.", call. = FALSE)
        }

        write_experiment_settings(export_preflight$config, file)
        experiment_settings_feedback(list(type = "success", message = sprintf("Exported experiment settings for %s.", export_preflight$config$source_reference$source_label)))
      }
    )

    output$csv_reference_status <- shiny::renderUI({
      current_source <- current_source_metadata()
      reference_resolution <- csv_reference_resolution()

      if (!identical(input$data_source, "csv")) {
        return(NULL)
      }

      if (inherits(imported_source_override(), "refiner_source_data") && identical(current_source$source_type, "csv") && !nzchar(csv_reference_path())) {
        return(shiny::tags$div(
          style = "padding: 12px; border-radius: 8px; margin-bottom: 12px; background: #eef4fb; border: 1px solid #7aa6d8; color: #163a59;",
          shiny::tags$strong("CSV reference path restored"),
          shiny::tags$p("Imported settings supplied a reusable local CSV reference path for future exports.")
        ))
      }

      if (is.null(reference_resolution)) {
        return(shiny::tags$div(
          style = "padding: 12px; border-radius: 8px; margin-bottom: 12px; background: #f6f7f8; border: 1px solid #d7dbdf; color: #1f2a30;",
          shiny::tags$strong("CSV reference path"),
          shiny::tags$p("Enter the stable local path to this CSV so exported settings can be imported into a clean session.")
        ))
      }

      if (isTRUE(reference_resolution$ok)) {
        return(shiny::tags$div(
          style = "padding: 12px; border-radius: 8px; margin-bottom: 12px; background: #e8f6ec; border: 1px solid #6db37c; color: #14361d;",
          shiny::tags$strong("CSV reference path verified"),
          shiny::tags$p(sprintf("Path: %s", reference_resolution$source_reference$source_path))
        ))
      }

      shiny::tags$div(
        style = "padding: 12px; border-radius: 8px; margin-bottom: 12px; background: #fff4df; border: 1px solid #d8a64b; color: #5d3b09;",
        shiny::tags$strong("CSV reference path not ready"),
        shiny::tags$p(paste(reference_resolution$errors, collapse = " "))
      )
    })

    display_preflight <- shiny::reactive({
      result <- latest_success_result()
      group_result <- selected_group_result()

      if (is.null(result)) {
        return(create_preflight_result(
          config = current_display_config_input(),
          errors = "A successful execution is required before summary, table, and plot outputs can be rendered."
        ))
      }

      if (is_grouped_execution_result(result)) {
        if (is.null(group_result)) {
          return(create_preflight_result(
            config = current_display_config_input(),
            errors = "Select a grouped result before summary, table, and plot outputs can be rendered."
          ))
        }

        if (!isTRUE(group_result$execution_result$success)) {
          return(create_preflight_result(
            config = current_display_config_input(),
            errors = sprintf("Selected group '%s' does not have a successful refineR fit to render.", group_result$group_label)
          ))
        }

        return(validate_display_config(current_display_config_input(), group_result$execution_result$config))
      }

      validate_display_config(current_display_config_input(), result$config)
    })

    display_surface <- shiny::reactive({
      result <- latest_success_result()
      group_result <- selected_group_result()
      preflight <- display_preflight()

      if (is.null(result) || !isTRUE(preflight$ok)) {
        return(list(summary = character(), interval_table = NULL))
      }

      if (is_grouped_execution_result(result)) {
        return(list(
          summary = capture_refiner_summary(group_result$execution_result$fit, preflight$config, group_result$execution_result$config),
          interval_table = extract_reference_interval(group_result$execution_result$fit, preflight$config)
        ))
      }

      list(
        summary = capture_refiner_summary(result$fit, preflight$config, result$config),
        interval_table = extract_reference_interval(result$fit, preflight$config)
      )
    })

    shiny::observe({
      current_result <- ingest_result()
      choices <- current_result$numeric_candidate_columns

      if (length(choices) == 0) {
        shiny::updateSelectInput(session, "analyte_column", choices = character(), selected = character())
        return()
      }

      selected <- current_result$selected_column

      if (is.null(selected) || !selected %in% choices) {
        selected <- choices[[1]]
      }

      shiny::updateSelectInput(session, "analyte_column", choices = stats::setNames(choices, choices), selected = selected)
    })

    shiny::observe({
      current_result <- ingest_result()
      choices <- setdiff(current_result$sex_candidate_columns, current_result$selected_column)
      selected <- first_scalar_or_default(current_result$selected_sex_column, "")

      if (!selected %in% choices) {
        selected <- ""
      }

      shiny::updateSelectInput(
        session,
        "sex_column",
        choices = c("None" = "", stats::setNames(choices, choices)),
        selected = selected
      )
    })

    shiny::observe({
      current_result <- ingest_result()
      choices <- setdiff(current_result$age_candidate_columns, current_result$selected_column)
      selected <- first_scalar_or_default(current_result$selected_age_column, "")

      if (!selected %in% choices) {
        selected <- ""
      }

      shiny::updateSelectInput(
        session,
        "age_column",
        choices = c("None" = "", stats::setNames(choices, choices)),
        selected = selected
      )
    })

    start_grouped_execution <- function(
      current_result,
      current_preflight,
      current_grouping,
      current_group_plan,
      current_source,
      run_event_id,
      run_signature,
      checkpoint_root_value = "",
      resume_manifest = NULL
    ) {
      config_signature <- build_config_signature(current_preflight$config)
      grouping_signature <- build_grouping_signature(current_group_plan, current_grouping)
      status_frame <- build_group_status_frame(current_group_plan)
      group_results <- vector("list", length = nrow(status_frame))
      restored_group_count <- 0L
      checkpoint_manifest <- NULL

      if (!is.null(resume_manifest)) {
        checkpoint_manifest <- resume_manifest
      } else if (identical(current_source$source_type, "csv") && nzchar(checkpoint_root_value)) {
        checkpoint_manifest <- initialize_group_checkpoint(
          checkpoint_root = checkpoint_root_value,
          run_signature = run_signature,
          config_signature = config_signature,
          grouping_signature = grouping_signature,
          source_metadata = current_source,
          planned_groups = as.character(current_group_plan$groups$group_label)
        )
      }

      if (!is.null(checkpoint_manifest)) {
        active_checkpoint_manifest(checkpoint_manifest)
      } else {
        active_checkpoint_manifest(NULL)
      }

      if (!is.null(resume_manifest)) {
        restored_results <- load_checkpoint_group_results(resume_manifest)

        for (group_label in names(restored_results)) {
          restored_result <- restored_results[[group_label]]

          if (is.null(restored_result)) {
            next
          }

          group_index <- match(group_label, status_frame$group_label)

          if (is.na(group_index)) {
            next
          }

          group_results[[group_index]] <- restored_result
          status_frame$status[[group_index]] <- restored_result$status
          status_frame$warnings[[group_index]] <- paste(restored_result$warning_messages, collapse = " ")
          status_frame$error[[group_index]] <- first_scalar_or_default(restored_result$error_message, "")
          restored_group_count <- restored_group_count + 1L
        }
      }

      execution_state("running")
      execution_result(NULL)
      wall_elapsed_seconds(NULL)
      group_execution_status(status_frame)
      selected_group_label("")
      message(sprintf("run_estimation executing [event=%s]", run_event_id))

      t_run_start <- proc.time()
      run_queue <- which(status_frame$status != "completed")

      finalize_grouped_execution <- function() {
        t_wall <- unname((proc.time() - t_run_start)["elapsed"])
        grouped_result <- assemble_grouped_execution_result(
          group_results = group_results,
          group_plan = current_group_plan,
          config = current_preflight$config,
          metadata = list(
            total_elapsed_seconds = t_wall,
            completed_at = as.character(Sys.time()),
            run_signature = run_signature,
            config_signature = config_signature,
            grouping_signature = grouping_signature,
            source_label = current_result$source_label,
            selected_column = current_result$selected_column,
            run_event_id = run_event_id,
            checkpoint_dir = if (!is.null(checkpoint_manifest)) checkpoint_manifest$checkpoint_dir else NULL,
            resumed_from_checkpoint = !is.null(resume_manifest),
            restored_groups = restored_group_count
          )
        )

        execution_result(grouped_result)
        execution_state(if (isTRUE(grouped_result$success_overall)) "success" else "error")
        wall_elapsed_seconds(t_wall)

        if (!is.null(checkpoint_manifest)) {
          active_checkpoint_manifest(checkpoint_manifest)
        }

        message(
          sprintf(
            "run_estimation grouped completed [event=%s, attempted=%s, completed=%s, failed=%s, wall_elapsed=%.2f]",
            run_event_id,
            grouped_result$metadata$groups_attempted,
            grouped_result$metadata$groups_completed,
            grouped_result$metadata$groups_failed,
            t_wall
          )
        )
      }

      if (length(run_queue) == 0) {
        finalize_grouped_execution()
        return(invisible(NULL))
      }

      run_next_group <- function(queue_position) {
        if (queue_position > length(run_queue)) {
          finalize_grouped_execution()
          return(invisible(NULL))
        }

        group_index <- run_queue[[queue_position]]
        status_frame$status[[group_index]] <<- "running"
        group_execution_status(status_frame)

        later::later(function() {
          group_label <- status_frame$group_label[[group_index]]
          row_indices <- extract_group_row_indices(current_result, current_group_plan, group_label)

          group_result <- tryCatch(
            run_group_refiner_estimation(
              ingest_result = current_result,
              group_plan = current_group_plan,
              config = current_preflight$config,
              group_index = group_index
            ),
            error = function(error) {
              create_group_execution_result(
                group_label = group_label,
                group_size = length(row_indices),
                row_indices = row_indices,
                status = "failed",
                execution_result = create_execution_result(
                  success = FALSE,
                  config = current_preflight$config,
                  metadata = list(group_label = group_label, row_indices = row_indices),
                  error_message = conditionMessage(error)
                ),
                error_message = conditionMessage(error)
              )
            }
          )

          group_results[[group_index]] <<- group_result

          if (!is.null(checkpoint_manifest)) {
            artifact_entry <- write_group_checkpoint_artifact(checkpoint_manifest$checkpoint_dir, group_result)
            checkpoint_manifest <<- update_checkpoint_manifest_group_result(checkpoint_manifest, group_result, artifact_entry)
            write_checkpoint_manifest(checkpoint_manifest, checkpoint_manifest$checkpoint_dir)
            write_checkpoint_summary(checkpoint_manifest, checkpoint_manifest$checkpoint_dir)
            active_checkpoint_manifest(checkpoint_manifest)
          }

          status_frame$status[[group_index]] <<- group_result$status
          status_frame$warnings[[group_index]] <<- paste(group_result$warning_messages, collapse = " ")
          status_frame$error[[group_index]] <<- first_scalar_or_default(group_result$error_message, "")
          group_execution_status(status_frame)

          message(
            sprintf(
              "run_estimation grouped progress [event=%s, group=%s, status=%s]",
              run_event_id,
              group_result$group_label,
              group_result$status
            )
          )

          run_next_group(queue_position + 1L)
        }, delay = 0.01)
      }

      run_next_group(1L)
      invisible(NULL)
    }

    shiny::observeEvent(input$run_estimation, {
      current_result <- ingest_result()
      current_preflight <- execution_preflight()
      current_grouping <- current_grouping_settings()
      current_group_plan <- grouping_preflight()
      current_source <- current_source_metadata()
      run_event_id <- first_scalar_or_default(input$run_estimation, "unknown")
      run_signature <- build_ingest_signature(
        current_result,
        grouping_mode = current_grouping$mode,
        sex_mapping_text = current_grouping$sex_mapping_text,
        age_band_text = current_grouping$age_band_text
      )

      message(
        sprintf(
          "run_estimation triggered [event=%s, source=%s, column=%s, ready=%s, preflight_ok=%s]",
          run_event_id,
          first_scalar_or_default(current_result$source_label, "not-selected"),
          first_scalar_or_default(current_result$selected_column, "none"),
          isTRUE(current_result$ready),
          isTRUE(current_preflight$ok)
        )
      )

      if (!current_result$ready) {
        message(sprintf("run_estimation blocked [event=%s, reason=data-not-ready]", run_event_id))
        execution_state("error")
        active_checkpoint_manifest(NULL)
        group_execution_status(NULL)
        execution_result(create_execution_result(
          success = FALSE,
          config = current_execution_config_input(),
          metadata = list(run_signature = run_signature),
          error_message = "Validated analyte data is required before execution can start."
        ))
        return()
      }

      if (!isTRUE(current_preflight$ok)) {
        message(sprintf("run_estimation blocked [event=%s, reason=preflight, errors=%s]", run_event_id, paste(current_preflight$errors, collapse = " | ")))
        execution_state("error")
        active_checkpoint_manifest(NULL)
        group_execution_status(NULL)
        execution_result(create_execution_result(
          success = FALSE,
          config = current_execution_config_input(),
          metadata = list(run_signature = run_signature),
          error_message = paste(current_preflight$errors, collapse = " ")
        ))
        return()
      }

      if (!identical(current_grouping$mode, "overall") && !isTRUE(current_group_plan$ok)) {
        message(sprintf("run_estimation blocked [event=%s, reason=group-plan, errors=%s]", run_event_id, paste(current_group_plan$errors, collapse = " | ")))
        execution_state("error")
        active_checkpoint_manifest(NULL)
        group_execution_status(build_group_status_frame(current_group_plan))
        execution_result(create_execution_result(
          success = FALSE,
          config = current_execution_config_input(),
          metadata = list(run_signature = run_signature),
          error_message = paste(current_group_plan$errors, collapse = " ")
        ))
        return()
      }

      if (!identical(current_grouping$mode, "overall")) {
        start_grouped_execution(
          current_result = current_result,
          current_preflight = current_preflight,
          current_grouping = current_grouping,
          current_group_plan = current_group_plan,
          current_source = current_source,
          run_event_id = run_event_id,
          run_signature = run_signature,
          checkpoint_root_value = checkpoint_root()
        )
        return()
      }

      active_checkpoint_manifest(NULL)
      group_execution_status(NULL)

      execution_state("running")
      execution_result(NULL)
      wall_elapsed_seconds(NULL)
      message(sprintf("run_estimation executing [event=%s]", run_event_id))

      t_run_start <- proc.time()

      # Use later::later() to defer execution and allow UI to update first
      # This ensures the "running" state is flushed to the browser before the long computation
      later::later(function() {
        tryCatch({
          result <- run_refiner_estimation(
            analyte_values = current_result$normalized_values,
            config = current_preflight$config
          )

          result$metadata$run_signature <- run_signature
          result$metadata$config_signature <- build_config_signature(current_preflight$config)
          result$metadata$source_label <- current_result$source_label
          result$metadata$selected_column <- current_result$selected_column
          result$metadata$run_event_id <- run_event_id

          execution_result(result)
          execution_state(if (isTRUE(result$success)) "success" else "error")

          t_wall <- unname((proc.time() - t_run_start)["elapsed"])
          wall_elapsed_seconds(t_wall)

          message(
            sprintf(
              "run_estimation completed [event=%s, success=%s, elapsed=%.2f]",
              run_event_id,
              isTRUE(result$success),
              if (!is.null(result$metadata$elapsed_seconds)) result$metadata$elapsed_seconds else NA_real_
            )
          )

          message(
            sprintf(
              "run_estimation wall-clock [event=%s, wrapper_elapsed=%.2f, wall_elapsed=%.2f]",
              run_event_id,
              if (!is.null(result$metadata$elapsed_seconds)) result$metadata$elapsed_seconds else NA_real_,
              t_wall
            )
          )
        }, error = function(error) {
          message(sprintf("run_estimation error [event=%s, error=%s]", run_event_id, conditionMessage(error)))
          execution_result(create_execution_result(
            success = FALSE,
            config = current_preflight$config,
            metadata = list(
              run_signature = run_signature,
              run_event_id = run_event_id
            ),
            error_message = conditionMessage(error)
          ))
          execution_state("error")

          t_wall <- unname((proc.time() - t_run_start)["elapsed"])
          wall_elapsed_seconds(t_wall)
        })
      }, delay = 0.01)
    }, ignoreInit = TRUE)

    shiny::observeEvent(input$resume_grouped_run, {
      if (identical(execution_state(), "running")) {
        return()
      }

      current_result <- ingest_result()
      current_preflight <- execution_preflight()
      current_grouping <- current_grouping_settings()
      current_group_plan <- grouping_preflight()
      current_source <- current_source_metadata()
      checkpoint_entry <- compatible_checkpoint_entry()
      run_event_id <- sprintf("resume-%s", first_scalar_or_default(input$resume_grouped_run, "unknown"))
      run_signature <- build_ingest_signature(
        current_result,
        grouping_mode = current_grouping$mode,
        sex_mapping_text = current_grouping$sex_mapping_text,
        age_band_text = current_grouping$age_band_text
      )

      if (
        is.null(checkpoint_entry) ||
        is.null(checkpoint_entry$manifest) ||
        !isTRUE(checkpoint_entry$compatibility$ok) ||
        length(checkpoint_entry$compatibility$config$rerunnable_groups) == 0
      ) {
        execution_state("error")
        execution_result(create_execution_result(
          success = FALSE,
          config = current_execution_config_input(),
          metadata = list(run_signature = run_signature),
          error_message = "No compatible checkpoint with remaining groups is available to resume."
        ))
        return()
      }

      start_grouped_execution(
        current_result = current_result,
        current_preflight = current_preflight,
        current_grouping = current_grouping,
        current_group_plan = current_group_plan,
        current_source = current_source,
        run_event_id = run_event_id,
        run_signature = run_signature,
        checkpoint_root_value = checkpoint_root(),
        resume_manifest = checkpoint_entry$manifest
      )
    }, ignoreInit = TRUE)

    shiny::observeEvent(input$discard_checkpoint, {
      checkpoint_entry <- compatible_checkpoint_entry()
      candidate_entries <- checkpoint_candidate_entries()
      target_entry <- if (!is.null(checkpoint_entry)) checkpoint_entry else if (length(candidate_entries) > 0) candidate_entries[[1]] else NULL

      if (is.null(target_entry) || is.null(target_entry$checkpoint_dir)) {
        return()
      }

      discard_checkpoint_run(target_entry$checkpoint_dir)

      if (!is.null(active_checkpoint_manifest()) && identical(active_checkpoint_manifest()$checkpoint_dir, target_entry$checkpoint_dir)) {
        active_checkpoint_manifest(NULL)
      }
    }, ignoreInit = TRUE)

    shiny::observeEvent(input$import_settings_file, {
      staged_experiment_settings(NULL)
      staged_experiment_settings_error("")
      experiment_settings_feedback(NULL)

      if (is.null(input$import_settings_file)) {
        return()
      }

      imported_settings <- tryCatch(
        read_experiment_settings(input$import_settings_file$datapath),
        error = function(error) error
      )

      if (inherits(imported_settings, "error")) {
        staged_experiment_settings_error(conditionMessage(imported_settings))
        return()
      }

      staged_experiment_settings(imported_settings)
    }, ignoreInit = TRUE)

    shiny::observeEvent(input$clear_imported_settings, {
      staged_experiment_settings(NULL)
      staged_experiment_settings_error("")
      experiment_settings_feedback(NULL)
    }, ignoreInit = TRUE)

    shiny::observeEvent(input$apply_imported_settings, {
      if (identical(execution_state(), "running")) {
        return()
      }

      import_preflight <- staged_experiment_settings_preflight()

      if (is.null(import_preflight) || !isTRUE(import_preflight$ok)) {
        return()
      }

      staged_settings <- import_preflight$config$settings
      source_resolution <- import_preflight$config$source_resolution
      override_source <- NULL

      if (
        isTRUE(source_resolution$ok) &&
        inherits(source_resolution$source_data, "refiner_source_data") &&
        identical(source_resolution$metadata$source_origin %||% "stored_reference", "stored_reference")
      ) {
        override_source <- source_resolution$source_data
      }

      applying_imported_settings(TRUE)
      imported_source_override(override_source)
      saved_display_config(import_preflight$config$display)
      active_checkpoint_manifest(NULL)
      group_execution_status(NULL)
      selected_group_label("")
      execution_result(NULL)
      execution_state("idle")
      wall_elapsed_seconds(NULL)
      experiment_settings_feedback(list(type = "success", message = sprintf("Imported experiment settings for %s.", staged_settings$source_reference$source_label)))

      shiny::updateRadioButtons(session, "data_source", selected = staged_settings$source_reference$source_type)

      if (identical(staged_settings$source_reference$source_type, "sample")) {
        shiny::updateSelectInput(session, "sample_dataset", selected = staged_settings$source_reference$source_label)
        shiny::updateTextInput(session, "csv_reference_path", value = "")
      } else {
        shiny::updateTextInput(session, "csv_reference_path", value = staged_settings$source_reference$source_path)
      }

      shiny::updateSelectInput(session, "param_model", selected = staged_settings$execution$model)
      shiny::updateNumericInput(session, "param_nbootstrap", value = staged_settings$execution$NBootstrap)
      shiny::updateNumericInput(session, "param_seed", value = staged_settings$execution$seed)
      shiny::updateTextInput(session, "param_riperc", value = paste(staged_settings$execution$RIperc, collapse = ", "))
      shiny::updateNumericInput(session, "param_ciprop", value = staged_settings$execution$CIprop)
      shiny::updateNumericInput(session, "param_umprop", value = staged_settings$execution$UMprop)
      shiny::updateSelectInput(session, "param_point_est", selected = staged_settings$execution$pointEst)
      shiny::updateSelectInput(session, "param_scale", selected = staged_settings$execution$Scale)

      staged_experiment_settings(NULL)
      staged_experiment_settings_error("")

      later::later(function() {
        shiny::updateSelectInput(session, "analyte_column", selected = staged_settings$data_selection$analyte_column)

        later::later(function() {
          shiny::updateSelectInput(session, "sex_column", selected = staged_settings$data_selection$sex_column)
          shiny::updateSelectInput(session, "age_column", selected = staged_settings$data_selection$age_column)
          shiny::updateSelectInput(session, "grouping_mode", selected = staged_settings$grouping$mode)
          shiny::updateTextAreaInput(session, "sex_mapping_text", value = staged_settings$grouping$sex_mapping_text)
          shiny::updateTextAreaInput(session, "age_band_text", value = staged_settings$grouping$age_band_text)

          later::later(function() {
            shiny::updateSelectInput(session, "display_point_est", selected = staged_settings$display$pointEst)
            shiny::updateSelectInput(session, "display_uncertainty_region", selected = staged_settings$display$uncertaintyRegion)
            shiny::updateNumericInput(session, "display_nhist", value = staged_settings$display$Nhist)
            shiny::updateCheckboxInput(session, "display_show_margin", value = staged_settings$display$showMargin)
            shiny::updateCheckboxInput(session, "display_show_pathol", value = staged_settings$display$showPathol)
            shiny::updateCheckboxInput(session, "display_show_value", value = staged_settings$display$showValue)
            shiny::updateSelectInput(session, "display_col_scheme", selected = staged_settings$display$colScheme)
            applying_imported_settings(FALSE)
          }, delay = 0.05)
        }, delay = 0.05)
      }, delay = 0.05)
    }, ignoreInit = TRUE)

    output$health_status <- shiny::renderUI({
      container_style <- paste(
        "padding: 16px; border-radius: 10px; margin-bottom: 16px;",
        if (startup_status$ok) {
          "background: #e8f6ec; border: 1px solid #6db37c; color: #14361d;"
        } else {
          "background: #fdeeee; border: 1px solid #d37b7b; color: #5a1717;"
        }
      )

      package_lines <- vapply(
        names(startup_status$package_checks),
        FUN = function(package_name) {
          state <- if (isTRUE(startup_status$package_checks[[package_name]])) {
            "available"
          } else {
            "missing"
          }

          sprintf("%s: %s", package_name, state)
        },
        FUN.VALUE = character(1)
      )

      shiny::tags$div(
        style = container_style,
        shiny::tags$h3(if (startup_status$ok) "Startup Health: OK" else "Startup Health: Attention Required"),
        shiny::tags$p(startup_status$message),
        shiny::tags$ul(
          shiny::tags$li(sprintf("renv activation: %s", if (startup_status$renv_activated) "detected" else "not detected")),
          lapply(package_lines, shiny::tags$li)
        )
      )
    })

    output$phase_scope <- shiny::renderText({
      paste(
        "Phase 5 scope:",
        "results visualization and interpretation are active.",
        "execution still remains button-driven from the existing Phase 4 flow."
      )
    })

    output$validation_status <- shiny::renderUI({
      current_result <- ingest_result()
      source_label <- first_scalar_or_default(current_result$source_label, "")
      selected_column <- if (!is.null(current_result$selected_column) && length(current_result$selected_column) > 0) {
        current_result$selected_column[[1]]
      } else {
        "none"
      }
      selected_sex_column <- first_scalar_or_default(current_result$selected_sex_column, "none")
      selected_age_column <- first_scalar_or_default(current_result$selected_age_column, "none")

      build_compact_status(
        ok = current_result$ready,
        ready_title = "Data validation ready",
        blocked_title = "Data validation blocked",
        summary = sprintf(
          "Source: %s | Analyte: %s | Sex: %s | Age: %s",
          if (nzchar(source_label)) source_label else "not selected",
          selected_column,
          selected_sex_column,
          selected_age_column
        ),
        warnings = current_result$warnings,
        errors = current_result$errors
      )
    })

    output$data_quality_snapshot <- shiny::renderUI({
      current_result <- ingest_result()
      plan <- grouping_preflight()
      settings <- current_grouping_settings()
      groups_planned <- if (!isTRUE(current_result$ready)) {
        0L
      } else if (!identical(settings$mode, "overall") && is.data.frame(plan$groups)) {
        nrow(plan$groups)
      } else {
        1L
      }

      shiny::div(
        class = "quality-metrics",
        build_metric_tile("Rows loaded", current_result$row_count),
        build_metric_tile("Usable analyte", length(current_result$normalized_values)),
        build_metric_tile("Missing analyte", current_result$missing_count),
        build_metric_tile("Metadata excluded", if (!is.null(plan$excluded_rows)) plan$excluded_rows else 0L),
        build_metric_tile("Groups planned", groups_planned)
      )
    })

    output$grouping_status <- shiny::renderUI({
      current_result <- ingest_result()
      plan <- grouping_preflight()
      settings <- current_grouping_settings()

      note <- if (identical(settings$mode, "overall")) {
        "Overall-only planning preserves the current single-fit execution path."
      } else if (plan$ok) {
        sprintf(
          "Grouped planning is ready: %s row(s) included, %s row(s) excluded.",
          plan$included_rows,
          plan$excluded_rows
        )
      } else {
        "Grouped planning must be valid before grouped refineR execution can start."
      }

      build_compact_status(
        ok = plan$ok,
        ready_title = "Grouping plan ready",
        blocked_title = "Grouping plan blocked",
        summary = sprintf("Mode: %s | %s", format_grouping_mode(settings$mode), note),
        warnings = plan$warnings,
        errors = c(
          plan$errors,
          if (!current_result$metadata_ready && identical(settings$mode, "overall")) {
            sprintf("Unused metadata issues: %s", format_validation_messages(current_result$metadata_errors))
          }
        )
      )
    })

    output$grouping_preview_ui <- shiny::renderUI({
      settings <- current_grouping_settings()
      plan <- grouping_preflight()

      if (identical(settings$mode, "overall") || !isTRUE(plan$ok) || !is.data.frame(plan$groups) || nrow(plan$groups) == 0) {
        return(NULL)
      }

      shiny::div(
        class = "grouping-preview",
        shiny::tags$h4("Grouping Preview"),
        shiny::tableOutput("grouping_preview_table")
      )
    })

    output$grouping_preview_table <- shiny::renderTable({
      settings <- current_grouping_settings()
      plan <- grouping_preflight()

      if (identical(settings$mode, "overall") || !isTRUE(plan$ok)) {
        return(NULL)
      }

      build_grouping_preview_frame(plan)
    }, striped = TRUE, bordered = TRUE, spacing = "s")

    output$workflow_readiness <- shiny::renderUI({
      readiness <- build_readiness_state(
        current_result = ingest_result(),
        preflight = execution_preflight(),
        settings = current_grouping_settings(),
        group_plan = grouping_preflight()
      )

      shiny::tags$div(
        class = paste("readiness-banner", if (readiness$ready) "ready" else "blocked"),
        shiny::tags$strong(readiness$title),
        shiny::tags$p(readiness$summary),
        if (!readiness$ready) compact_message_list(readiness$blockers)
      )
    })

    output$preview_table <- shiny::renderTable({
      build_preview_table(ingest_result())
    }, striped = TRUE, bordered = TRUE, spacing = "s")

    output$parameter_preflight_status <- shiny::renderUI({
      preflight <- execution_preflight()

      build_compact_status(
        ok = preflight$ok,
        ready_title = "Parameter preflight passed",
        blocked_title = "Parameter preflight blocked",
        summary = "Core settings are validated before refineR execution.",
        warnings = preflight$warnings,
        errors = preflight$errors
      )
    })

    output$run_button_ui <- shiny::renderUI({
      current_result <- ingest_result()
      preflight <- execution_preflight()
      settings <- current_grouping_settings()
      group_plan <- grouping_preflight()
      current_source <- current_source_metadata()

      button_label <- if (!identical(settings$mode, "overall") && identical(current_source$source_type, "csv")) {
        "Start New Grouped Run"
      } else {
        "Run refineR Estimation"
      }

      if (current_result$ready && isTRUE(preflight$ok) && (identical(settings$mode, "overall") || isTRUE(group_plan$ok))) {
        return(shiny::actionButton(
          inputId = "run_estimation",
          label = button_label,
          class = "btn-primary"
        ))
      }

      if (!identical(settings$mode, "overall")) {
        return(shiny::tagList(
          shiny::tags$button(
            type = "button",
            class = "btn btn-primary",
            disabled = "disabled",
            button_label
          )
        ))
      }

      shiny::tags$button(
        type = "button",
        class = "btn btn-primary",
        disabled = "disabled",
        button_label
      )
    })

    output$experiment_settings_export_ui <- shiny::renderUI({
      export_preflight <- current_experiment_settings_export()

      if (isTRUE(export_preflight$ok)) {
        return(shiny::downloadButton(
          outputId = "download_experiment_settings",
          label = "Export Settings",
          class = "btn-default"
        ))
      }

      shiny::tagList(
        shiny::tags$button(
          type = "button",
          class = "btn btn-default",
          disabled = "disabled",
          "Export Settings"
        ),
        shiny::tags$p("Settings export becomes available once the current source, grouping, and execution settings are valid.")
      )
    })

    output$experiment_settings_status <- shiny::renderUI({
      import_preflight <- staged_experiment_settings_preflight()
      feedback <- experiment_settings_feedback()
      export_preflight <- current_experiment_settings_export()

      if (!is.null(import_preflight)) {
        if (!isTRUE(import_preflight$ok)) {
          return(shiny::tags$div(
            style = "padding: 12px; border-radius: 8px; margin-bottom: 12px; background: #fdeeee; border: 1px solid #d37b7b; color: #5a1717;",
            shiny::tags$strong("Imported settings need attention"),
            shiny::tags$p(paste(import_preflight$errors, collapse = " ")),
            if (length(import_preflight$warnings) > 0) shiny::tags$p(sprintf("Warnings: %s", paste(import_preflight$warnings, collapse = " ")))
          ))
        }

        return(shiny::tags$div(
          style = "padding: 12px; border-radius: 8px; margin-bottom: 12px; background: #e8f6ec; border: 1px solid #6db37c; color: #14361d;",
          shiny::tags$strong("Imported settings are ready to apply"),
          shiny::tags$p(import_preflight$config$settings$summary),
          if (length(import_preflight$warnings) > 0) shiny::tags$p(sprintf("Warnings: %s", paste(import_preflight$warnings, collapse = " ")))
        ))
      }

      if (!is.null(feedback)) {
        container_style <- if (identical(feedback$type, "success")) {
          "padding: 12px; border-radius: 8px; margin-bottom: 12px; background: #e8f6ec; border: 1px solid #6db37c; color: #14361d;"
        } else {
          "padding: 12px; border-radius: 8px; margin-bottom: 12px; background: #f6f7f8; border: 1px solid #d7dbdf; color: #1f2a30;"
        }

        return(shiny::tags$div(
          style = container_style,
          shiny::tags$strong("Experiment settings"),
          shiny::tags$p(feedback$message)
        ))
      }

      default_lines <- c("Export the current validated configuration or import a saved settings JSON file.")

      if (!isTRUE(export_preflight$ok) && length(export_preflight$errors) > 0) {
        default_lines <- c(default_lines, paste(export_preflight$errors, collapse = " "))
      }

      if (length(export_preflight$warnings) > 0) {
        default_lines <- c(default_lines, sprintf("Warnings: %s", paste(export_preflight$warnings, collapse = " ")))
      }

      shiny::tags$div(
        style = "padding: 12px; border-radius: 8px; margin-bottom: 12px; background: #f6f7f8; border: 1px solid #d7dbdf; color: #1f2a30;",
        shiny::tags$strong("Experiment settings"),
        lapply(default_lines, shiny::tags$p)
      )
    })

    output$experiment_settings_controls_ui <- shiny::renderUI({
      import_preflight <- staged_experiment_settings_preflight()
      can_apply <- !is.null(import_preflight) && isTRUE(import_preflight$ok) && !identical(execution_state(), "running")
      has_staged_settings <- !is.null(import_preflight) || nzchar(staged_experiment_settings_error())

      shiny::tagList(
        shiny::actionButton(
          inputId = "apply_imported_settings",
          label = "Apply Imported Settings",
          class = "btn-default",
          disabled = if (!can_apply) "disabled" else NULL
        ),
        shiny::actionButton(
          inputId = "clear_imported_settings",
          label = "Clear Imported Settings",
          class = "btn-default",
          disabled = if (!has_staged_settings) "disabled" else NULL
        )
      )
    })

    output$checkpoint_status <- shiny::renderUI({
      settings <- current_grouping_settings()
      current_source <- current_source_metadata()
      selected_root <- checkpoint_root()
      candidate_entries <- checkpoint_candidate_entries()
      checkpoint_entry <- compatible_checkpoint_entry()
      active_manifest <- active_checkpoint_manifest()
      state <- execution_state()

      if (identical(settings$mode, "overall") || !identical(current_source$source_type, "csv")) {
        return(NULL)
      }

      if (!nzchar(selected_root)) {
        return(shiny::tags$div(
          style = "padding: 12px; border-radius: 8px; margin-bottom: 12px; background: #f6f7f8; border: 1px solid #d7dbdf; color: #1f2a30;",
          shiny::tags$strong("Checkpointing is optional"),
          shiny::tags$p("Enter a checkpoint folder to persist subgroup results and enable resume for grouped CSV runs.")
        ))
      }

      if (!is.null(active_manifest) && (identical(state, "running") || identical(state, "success") || identical(state, "error"))) {
        return(shiny::tags$div(
          style = "padding: 12px; border-radius: 8px; margin-bottom: 12px; background: #eef4fb; border: 1px solid #7aa6d8; color: #163a59;",
          shiny::tags$strong("Active checkpoint"),
          shiny::tags$p(sprintf("Folder: %s", active_manifest$checkpoint_dir)),
          shiny::tags$p(sprintf(
            "Completed: %s | Failed: %s | Pending: %s",
            length(active_manifest$completed_groups),
            length(active_manifest$failed_groups),
            length(active_manifest$pending_groups)
          ))
        ))
      }

      if (!is.null(checkpoint_entry)) {
        rerunnable_groups <- checkpoint_entry$compatibility$config$rerunnable_groups

        return(shiny::tags$div(
          style = "padding: 12px; border-radius: 8px; margin-bottom: 12px; background: #e8f6ec; border: 1px solid #6db37c; color: #14361d;",
          shiny::tags$strong(if (length(rerunnable_groups) > 0) "Compatible checkpoint ready" else "Compatible checkpoint already complete"),
          shiny::tags$p(sprintf("Folder: %s", checkpoint_entry$manifest$checkpoint_dir)),
          shiny::tags$p(sprintf(
            "Completed: %s | Failed: %s | Pending: %s",
            length(checkpoint_entry$manifest$completed_groups),
            length(checkpoint_entry$manifest$failed_groups),
            length(checkpoint_entry$manifest$pending_groups)
          )),
          if (length(rerunnable_groups) > 0) shiny::tags$p(sprintf("Resume will execute %s remaining or previously failed group(s).", length(rerunnable_groups)))
        ))
      }

      if (length(candidate_entries) > 0) {
        latest_entry <- candidate_entries[[1]]

        if (!is.null(latest_entry$read_error)) {
          return(shiny::tags$div(
            style = "padding: 12px; border-radius: 8px; margin-bottom: 12px; background: #fff4df; border: 1px solid #d8a64b; color: #5d3b09;",
            shiny::tags$strong("Checkpoint found but unreadable"),
            shiny::tags$p(latest_entry$read_error)
          ))
        }

        return(shiny::tags$div(
          style = "padding: 12px; border-radius: 8px; margin-bottom: 12px; background: #fff4df; border: 1px solid #d8a64b; color: #5d3b09;",
          shiny::tags$strong("Checkpoint found but not compatible"),
          shiny::tags$p(sprintf("Folder: %s", latest_entry$manifest$checkpoint_dir)),
          shiny::tags$p(sprintf("Mismatch: %s", paste(latest_entry$compatibility$errors, collapse = " ")))
        ))
      }

      shiny::tags$div(
        style = "padding: 12px; border-radius: 8px; margin-bottom: 12px; background: #f6f7f8; border: 1px solid #d7dbdf; color: #1f2a30;",
        shiny::tags$strong("No checkpoint found"),
        shiny::tags$p("Starting a new grouped run will create a checkpoint under the selected folder.")
      )
    })

    output$checkpoint_controls_ui <- shiny::renderUI({
      settings <- current_grouping_settings()
      current_source <- current_source_metadata()
      checkpoint_entry <- compatible_checkpoint_entry()
      candidate_entries <- checkpoint_candidate_entries()
      state <- execution_state()

      if (identical(settings$mode, "overall") || !identical(current_source$source_type, "csv")) {
        return(NULL)
      }

      rerunnable_groups <- if (!is.null(checkpoint_entry)) checkpoint_entry$compatibility$config$rerunnable_groups else character()
      can_resume_checkpoint <- !identical(state, "running") && !is.null(checkpoint_entry) && length(rerunnable_groups) > 0

      shiny::tagList(
        if (can_resume_checkpoint) {
          shiny::actionButton(
            inputId = "resume_grouped_run",
            label = "Resume Grouped Run",
            class = "btn-default"
          )
        },
        if (length(candidate_entries) > 0) {
          shiny::actionButton(
            inputId = "discard_checkpoint",
            label = "Discard Checkpoint",
            class = "btn-default",
            disabled = if (identical(state, "running")) "disabled" else NULL
          )
        }
      )
    })

    output$execution_defaults <- shiny::renderUI({
      preflight <- execution_preflight()
      config_to_display <- if (isTRUE(preflight$ok)) preflight$config else current_execution_config_input()

      shiny::tags$p(
        shiny::tags$strong("Selected configuration: "),
        format_execution_defaults(config_to_display)
      )
    })

    output$execution_status <- shiny::renderUI({
      state <- execution_state()
      current_result <- execution_result()
      preflight <- execution_preflight()
      settings <- current_grouping_settings()
      group_plan <- grouping_preflight()
      group_status <- group_execution_status()
      latest_signature <- if (!is.null(current_result)) current_result$metadata$run_signature else NULL
      current_signature <- build_ingest_signature(
        ingest_result(),
        grouping_mode = settings$mode,
        sex_mapping_text = settings$sex_mapping_text,
        age_band_text = settings$age_band_text
      )
      latest_config_signature <- if (!is.null(current_result)) current_result$metadata$config_signature else NULL
      current_config_signature <- if (isTRUE(preflight$ok)) build_config_signature(preflight$config) else "invalid"

      status_style <- switch(
        state,
        running = "background: #eef4fb; border: 1px solid #7aa6d8; color: #163a59;",
        success = "background: #e8f6ec; border: 1px solid #6db37c; color: #14361d;",
        error = "background: #fdeeee; border: 1px solid #d37b7b; color: #5a1717;",
        "background: #f6f7f8; border: 1px solid #d7dbdf; color: #1f2a30;"
      )

      body <- switch(
        state,
        running = if (!identical(settings$mode, "overall") && is.data.frame(group_status) && nrow(group_status) > 0) {
          shiny::tagList(
            sprintf(
              "Grouped refineR execution is running. Completed %s/%s group(s); failed %s. ",
              sum(group_status$status == "completed"),
              nrow(group_status),
              sum(group_status$status == "failed")
            ),
            shiny::tags$span(class = "spinner")
          )
        } else {
          shiny::tagList(
            "refineR execution is running. ",
            shiny::tags$span(class = "spinner")
          )
        },
        success = if (is_grouped_execution_result(current_result)) {
          sprintf(
            "Grouped run completed for %s / %s. Successful groups: %s. Failed groups: %s.",
            current_result$metadata$source_label,
            current_result$metadata$selected_column,
            current_result$metadata$groups_completed,
            current_result$metadata$groups_failed
          )
        } else {
          sprintf(
            "Run completed successfully for %s / %s.",
            current_result$metadata$source_label,
            current_result$metadata$selected_column
          )
        },
        error = if (is_grouped_execution_result(current_result)) {
          paste(
            c(
              sprintf(
                "Grouped run finished with no successful groups across %s planned group(s).",
                current_result$metadata$groups_attempted
              ),
              format_validation_messages(current_result$errors)
            ),
            collapse = " "
          )
        } else {
          current_result$error_message
        },
        if (!ingest_result()$ready) {
          "Execution is blocked until the data panel reaches a valid ready state."
        } else if (!identical(settings$mode, "overall") && !group_plan$ok) {
          "Execution is blocked until the grouping plan is valid."
        } else if (!identical(settings$mode, "overall")) {
          "Validated grouped planning is ready. Click Run refineR Estimation to execute the planned subgroup fits."
        } else if (!preflight$ok) {
          "Execution is blocked until parameter preflight validation succeeds."
        } else {
          "Validated data and parameters are ready. Click Run refineR Estimation to execute with the selected configuration."
        }
      )

      stale_note <- if (
        !is.null(current_result) &&
          identical(state, "success") &&
          (!identical(latest_signature, current_signature) || !identical(latest_config_signature, current_config_signature))
      ) {
        shiny::tags$p("Current data selection or parameter configuration differs from the last executed run. Click Run refineR Estimation to refresh the results.")
      }

      metadata_line <- if (!is.null(current_result) && identical(state, "success")) {
        wall <- wall_elapsed_seconds()
        shiny::tagList(
          if (is_grouped_execution_result(current_result)) {
            shiny::tags$p(sprintf(
              "Grouped wall elapsed: %.2f s | Attempted: %s | Completed: %s | Failed: %s",
              if (!is.null(current_result$metadata$total_elapsed_seconds)) current_result$metadata$total_elapsed_seconds else NA_real_,
              current_result$metadata$groups_attempted,
              current_result$metadata$groups_completed,
              current_result$metadata$groups_failed
            ))
          } else {
            shiny::tags$p(sprintf(
              "Wrapper elapsed: %.2f s",
              if (!is.null(current_result$metadata$elapsed_seconds)) current_result$metadata$elapsed_seconds else NA_real_
            ))
          },
          if (!is.null(wall)) {
            shiny::tags$p(sprintf("Wall elapsed (server total): %.2f s", wall))
          }
        )
      }

      shiny::tags$div(
        style = paste("padding: 12px; border-radius: 8px; margin-top: 12px;", status_style),
        shiny::tags$strong(sprintf("Execution state: %s", state)),
        shiny::tags$p(body),
        metadata_line,
        stale_note
      )
    })

    output$results_status <- shiny::renderUI({
      state <- execution_state()
      result <- latest_success_result()
      preflight <- display_preflight()
      settings <- current_grouping_settings()
      group_result <- selected_group_result()

      if (identical(state, "running") && !identical(settings$mode, "overall")) {
        return(shiny::tags$p("Grouped execution is in progress. The group summary table updates as each subgroup finishes."))
      }

      if (is.null(result)) {
        return(if (identical(settings$mode, "overall")) {
          shiny::tags$p("Run refineR Estimation to populate the summary, interval table, and plot views.")
        } else {
          shiny::tags$p("Run refineR Estimation to populate the grouped summary table and selected-group detail views.")
        })
      }

      if (!isTRUE(preflight$ok)) {
        return(shiny::tags$p("Display controls are blocked until the current results-display configuration is valid for the selected result."))
      }

      if (identical(state, "error") && is.null(result)) {
        return(shiny::tags$p("The last run failed, so no interpretation surface is available yet."))
      }

       if (is_grouped_execution_result(result)) {
        return(shiny::tags$p(sprintf(
          "Grouped summary shows all planned strata. Detail tabs are currently displaying '%s'.",
          if (!is.null(group_result)) group_result$group_label else "no selected group"
        )))
      }

      shiny::tags$p("Summary, interval table, and plot are being regenerated from the last successful fit using the current display controls.")
    })

    output$results_display_preflight <- shiny::renderUI({
      result <- latest_success_result()
      preflight <- display_preflight()
      settings <- current_grouping_settings()

      if (is.null(result)) {
        return(shiny::tags$div(
          style = "padding: 12px; border-radius: 8px; margin-bottom: 12px; background: #f6f7f8; border: 1px solid #d7dbdf; color: #1f2a30;",
          shiny::tags$strong("Results display is waiting for a successful run"),
          shiny::tags$p(if (identical(settings$mode, "overall")) {
            "Execute refineR once to enable the summary, interval table, and plot controls."
          } else {
            "Execute grouped refineR once to enable the grouped summary table and selected-group interpretation controls."
          })
        ))
      }

      style <- paste(
        "padding: 12px; border-radius: 8px; margin-bottom: 12px;",
        if (preflight$ok) {
          "background: #e8f6ec; border: 1px solid #6db37c; color: #14361d;"
        } else {
          "background: #fdeeee; border: 1px solid #d37b7b; color: #5a1717;"
        }
      )

      shiny::tags$div(
        style = style,
        shiny::tags$strong(if (preflight$ok) "Results display is ready" else "Results display is blocked"),
        shiny::tags$p(sprintf("Warnings: %s", format_validation_messages(preflight$warnings))),
        shiny::tags$p(sprintf("Errors: %s", format_validation_messages(preflight$errors)))
      )
    })

    output$group_results_summary_ui <- shiny::renderUI({
      settings <- current_grouping_settings()
      result <- latest_success_result()
      status_frame <- group_execution_status()

      if (identical(settings$mode, "overall") && !is_grouped_execution_result(result)) {
        return(NULL)
      }

      if (is.null(result) && (!is.data.frame(status_frame) || nrow(status_frame) == 0)) {
        return(NULL)
      }

      shiny::tags$div(
        style = "padding: 4px 0 12px 0;",
        shiny::tags$h4("Group Summary"),
        shiny::tableOutput("group_results_summary_table")
      )
    })

    output$group_results_summary_table <- shiny::renderTable({
      result <- latest_success_result()
      status_frame <- group_execution_status()

      if (is_grouped_execution_result(result) && is.data.frame(result$summary_table)) {
        return(result$summary_table)
      }

      if (is.data.frame(status_frame) && nrow(status_frame) > 0) {
        return(status_frame)
      }

      NULL
    }, striped = TRUE, bordered = TRUE, spacing = "s")

    output$selected_group_ui <- shiny::renderUI({
      result <- latest_success_result()

      if (!is_grouped_execution_result(result)) {
        return(NULL)
      }

      summary_table <- result$summary_table
      selector_frame <- if (is.data.frame(summary_table) && nrow(summary_table) > 0) {
        data.frame(
          group_label = as.character(summary_table$group_label),
          row_count = as.integer(summary_table$row_count),
          status = as.character(summary_table$status),
          stringsAsFactors = FALSE
        )
      } else {
        build_group_status_frame(result$grouping_metadata$group_plan)
      }

      shiny::tags$div(
        style = "padding-bottom: 12px;",
        shiny::tags$h4("Selected Group Detail"),
        shiny::selectInput(
          inputId = "group_detail_label",
          label = "Group",
          choices = format_group_choice_labels(selector_frame),
          selected = selected_group_label(),
          selectize = FALSE
        )
      )
    })

    shiny::observeEvent(input$group_detail_label, {
      selected_group_label(first_scalar_or_default(input$group_detail_label, ""))
    }, ignoreInit = TRUE)

    output$results_summary <- shiny::renderText({
      surface <- display_surface()
      result <- latest_success_result()
      group_result <- selected_group_result()

      if (is_grouped_execution_result(result) && !is.null(group_result) && !isTRUE(group_result$execution_result$success)) {
        return(sprintf("Selected group '%s' did not finish with a successful refineR fit.", group_result$group_label))
      }

      if (length(surface$summary) == 0) {
        return("No summary is available yet.")
      }

      paste(surface$summary, collapse = "\n")
    })

    output$results_table <- shiny::renderTable({
      surface <- display_surface()

      if (!is.data.frame(surface$interval_table)) {
        return(NULL)
      }

      surface$interval_table
    }, striped = TRUE, bordered = TRUE, spacing = "s")

    results_plot_width <- function() {
      width <- suppressWarnings(as.numeric(input$results_plot_width_px))

      if (length(width) != 1 || is.na(width) || !is.finite(width) || width <= 0) {
        return(900)
      }

      max(320, min(1600, as.integer(width)))
    }

    results_plot_height <- function() {
      height <- suppressWarnings(as.numeric(input$results_plot_height_px))

      if (length(height) != 1 || is.na(height) || !is.finite(height) || height <= 0) {
        return(460)
      }

      max(300, min(760, as.integer(height)))
    }

    output$results_plot <- shiny::renderPlot({
      result <- latest_success_result()
      group_result <- selected_group_result()
      preflight <- display_preflight()

      if (is.null(result) || !isTRUE(preflight$ok)) {
        return(invisible(NULL))
      }

      if (is_grouped_execution_result(result)) {
        if (is.null(group_result) || !isTRUE(group_result$execution_result$success)) {
          return(invisible(NULL))
        }

        return(build_refiner_plot(group_result$execution_result$fit, preflight$config, group_result$execution_result$config))
      }

      build_refiner_plot(result$fit, preflight$config, result$config)
    }, width = results_plot_width, height = results_plot_height, res = 96, execOnResize = TRUE)
  }
}
