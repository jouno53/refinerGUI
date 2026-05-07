make_checkpoint_root <- function() {
  checkpoint_root <- tempfile("checkpoints_")
  dir.create(checkpoint_root)
  checkpoint_root
}

make_checkpoint_source_metadata <- function(fingerprint = "fingerprint-a") {
  list(
    source_type = "csv",
    source_label = "grouped_execution_valid.csv",
    source_fingerprint = fingerprint
  )
}

make_checkpoint_manifest <- function(
  checkpoint_root = make_checkpoint_root(),
  checkpoint_id = "checkpoint-test",
  planned_groups = c("female", "male"),
  source_fingerprint = "fingerprint-a"
) {
  initialize_group_checkpoint(
    checkpoint_root = checkpoint_root,
    run_signature = "run-signature",
    config_signature = "config-signature",
    grouping_signature = "grouping-signature",
    source_metadata = make_checkpoint_source_metadata(source_fingerprint),
    planned_groups = planned_groups,
    checkpoint_id = checkpoint_id
  )
}

expect_checkpoint_core_fields_equal <- function(actual, expected) {
  expect_identical(actual$schema_version, expected$schema_version)
  expect_identical(actual$checkpoint_id, expected$checkpoint_id)
  expect_identical(actual$run_signature, expected$run_signature)
  expect_identical(actual$config_signature, expected$config_signature)
  expect_identical(actual$grouping_signature, expected$grouping_signature)
  expect_identical(actual$source_metadata, expected$source_metadata)
  expect_identical(actual$planned_groups, expected$planned_groups)
  expect_identical(actual$completed_groups, expected$completed_groups)
  expect_identical(actual$failed_groups, expected$failed_groups)
  expect_identical(actual$pending_groups, expected$pending_groups)
  expect_identical(actual$status, expected$status)
}

expect_checkpoint_interval_table_equal <- function(actual, expected, tolerance = 1e-8) {
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

make_real_group_results <- function(group_count = 2L) {
  source_data <- load_local_csv(test_path("../../fixtures/grouped_execution_valid.csv"))
  ingest <- prepare_ingest_result(
    source_data,
    column_name = "analyte",
    sex_column = "sex_code",
    age_column = "age_years"
  )
  group_plan <- plan_grouped_analysis(
    analyte_values = ingest$normalized_values,
    grouping_mode = "sex",
    sex_values = ingest$sex_values,
    sex_mapping_text = "female = F\nmale = M"
  )

  expect_true(isTRUE(ingest$ready))
  expect_true(isTRUE(group_plan$ok))

  lapply(seq_len(group_count), function(group_index) {
    run_group_refiner_estimation(
      ingest_result = ingest,
      group_plan = group_plan,
      config = default_execution_config(),
      group_index = group_index
    )
  })
}

make_failed_group_result <- function(group_label = "male") {
  create_group_execution_result(
    group_label = group_label,
    group_size = 2L,
    row_indices = c(1L, 2L),
    status = "failed",
    execution_result = create_execution_result(
      success = FALSE,
      config = default_execution_config(),
      metadata = list(group_label = group_label),
      error_message = "simulated failure"
    ),
    error_message = "simulated failure",
    metadata = list(reason = "test")
  )
}

test_that("checkpoint manifest initialization writes manifest and summary files", {
  checkpoint_root <- make_checkpoint_root()
  on.exit(unlink(checkpoint_root, recursive = TRUE, force = TRUE), add = TRUE)

  manifest <- make_checkpoint_manifest(checkpoint_root)
  paths <- build_checkpoint_paths(manifest$checkpoint_dir)

  expect_s3_class(manifest, "refiner_checkpoint_manifest")
  expect_true(dir.exists(manifest$checkpoint_dir))
  expect_true(file.exists(paths$manifest_path))
  expect_true(file.exists(paths$summary_path))
  expect_identical(manifest$planned_groups, c("female", "male"))
  expect_identical(manifest$pending_groups, c("female", "male"))
  expect_identical(manifest$completed_groups, character())
  expect_identical(manifest$failed_groups, character())
  expect_identical(manifest$status, "in_progress")
})

test_that("checkpoint manifest read write round trip preserves core fields", {
  checkpoint_root <- make_checkpoint_root()
  on.exit(unlink(checkpoint_root, recursive = TRUE, force = TRUE), add = TRUE)

  manifest <- make_checkpoint_manifest(checkpoint_root)
  write_checkpoint_manifest(manifest)
  read_manifest <- read_checkpoint_manifest(manifest$checkpoint_dir)

  expect_checkpoint_core_fields_equal(read_manifest, manifest)
})

test_that("group artifact write read round trip preserves completed execution result", {
  checkpoint_root <- make_checkpoint_root()
  on.exit(unlink(checkpoint_root, recursive = TRUE, force = TRUE), add = TRUE)
  group_result <- make_real_group_results(1L)[[1]]
  manifest <- make_checkpoint_manifest(checkpoint_root, planned_groups = group_result$group_label)

  artifact_entry <- write_group_checkpoint_artifact(manifest$checkpoint_dir, group_result)
  restored <- read_group_checkpoint_artifact(
    checkpoint_dir = manifest$checkpoint_dir,
    group_label = group_result$group_label,
    artifact_filename = artifact_entry$artifact_filename
  )

  expect_s3_class(restored, "refiner_group_execution_result")
  expect_identical(restored$group_label, group_result$group_label)
  expect_identical(restored$group_size, group_result$group_size)
  expect_identical(restored$row_indices, group_result$row_indices)
  expect_identical(restored$status, group_result$status)
  expect_identical(restored$execution_result$success, group_result$execution_result$success)
  expect_checkpoint_interval_table_equal(restored$execution_result$interval_table, group_result$execution_result$interval_table)
  expect_identical(restored$execution_result$config, group_result$execution_result$config)
  expect_identical(restored$metadata, group_result$metadata)
})

test_that("manifest state update moves groups through pending completed and completed status", {
  checkpoint_root <- make_checkpoint_root()
  on.exit(unlink(checkpoint_root, recursive = TRUE, force = TRUE), add = TRUE)
  group_results <- make_real_group_results(2L)
  planned_groups <- vapply(group_results, function(group_result) group_result$group_label, character(1))
  manifest <- make_checkpoint_manifest(checkpoint_root, planned_groups = planned_groups)

  first_artifact <- write_group_checkpoint_artifact(manifest$checkpoint_dir, group_results[[1]])
  manifest <- update_checkpoint_manifest_group_result(manifest, group_results[[1]], first_artifact)

  expect_identical(manifest$completed_groups, group_results[[1]]$group_label)
  expect_identical(manifest$failed_groups, character())
  expect_false(group_results[[1]]$group_label %in% manifest$pending_groups)
  expect_true(group_results[[2]]$group_label %in% manifest$pending_groups)
  expect_true(group_results[[1]]$group_label %in% names(manifest$per_group_artifacts))
  expect_identical(manifest$status, "in_progress")

  second_artifact <- write_group_checkpoint_artifact(manifest$checkpoint_dir, group_results[[2]])
  manifest <- update_checkpoint_manifest_group_result(manifest, group_results[[2]], second_artifact)

  expect_identical(sort(manifest$completed_groups), sort(planned_groups))
  expect_identical(manifest$pending_groups, character())
  expect_identical(manifest$failed_groups, character())
  expect_identical(manifest$status, "completed")
})

test_that("failed group update records failed state without completing manifest", {
  checkpoint_root <- make_checkpoint_root()
  on.exit(unlink(checkpoint_root, recursive = TRUE, force = TRUE), add = TRUE)
  manifest <- make_checkpoint_manifest(checkpoint_root, planned_groups = c("female", "male"))
  failed_result <- make_failed_group_result("male")

  manifest <- update_checkpoint_manifest_group_result(manifest, failed_result)

  expect_identical(manifest$failed_groups, "male")
  expect_false("male" %in% manifest$pending_groups)
  expect_false("male" %in% manifest$completed_groups)
  expect_identical(manifest$status, "in_progress")
  expect_true("male" %in% names(manifest$per_group_artifacts))
})

test_that("checkpoint compatibility succeeds for matching manifest and current run state", {
  checkpoint_root <- make_checkpoint_root()
  on.exit(unlink(checkpoint_root, recursive = TRUE, force = TRUE), add = TRUE)
  manifest <- make_checkpoint_manifest(checkpoint_root)

  compatibility <- validate_checkpoint_compatibility(
    manifest,
    run_signature = "run-signature",
    config_signature = "config-signature",
    grouping_signature = "grouping-signature",
    source_type = "csv",
    planned_groups = c("female", "male"),
    source_fingerprint = "fingerprint-a"
  )

  expect_s3_class(compatibility, "refiner_preflight_result")
  expect_true(isTRUE(compatibility$ok))
})

test_that("checkpoint compatibility fails clearly for independent mismatches", {
  checkpoint_root <- make_checkpoint_root()
  on.exit(unlink(checkpoint_root, recursive = TRUE, force = TRUE), add = TRUE)
  manifest <- make_checkpoint_manifest(checkpoint_root)

  expect_incompatible <- function(overrides, pattern, manifest_override = manifest) {
    args <- utils::modifyList(list(
      manifest = manifest_override,
      run_signature = "run-signature",
      config_signature = "config-signature",
      grouping_signature = "grouping-signature",
      source_type = "csv",
      planned_groups = c("female", "male"),
      source_fingerprint = "fingerprint-a"
    ), overrides)
    compatibility <- do.call(validate_checkpoint_compatibility, args)

    expect_false(isTRUE(compatibility$ok))
    expect_match(paste(compatibility$errors, collapse = " "), pattern)
  }

  expect_incompatible(list(source_type = "sample"), "only supported for grouped CSV")
  expect_incompatible(list(run_signature = "changed-run"), "data selection")
  expect_incompatible(list(config_signature = "changed-config"), "execution settings")
  expect_incompatible(list(grouping_signature = "changed-grouping"), "grouping settings")
  expect_incompatible(list(planned_groups = c("female")), "planned groups")
  expect_incompatible(list(source_fingerprint = "fingerprint-b"), "fingerprint")

  invalid_manifest <- manifest
  invalid_manifest$status <- "invalid"
  expect_incompatible(list(), "marked invalid", invalid_manifest)
})

test_that("compatible checkpoint discovery returns the matching pure manifest", {
  checkpoint_root <- make_checkpoint_root()
  on.exit(unlink(checkpoint_root, recursive = TRUE, force = TRUE), add = TRUE)
  make_checkpoint_manifest(
    checkpoint_root = checkpoint_root,
    checkpoint_id = "001-incompatible",
    planned_groups = c("female", "male"),
    source_fingerprint = "stale-fingerprint"
  )
  compatible_manifest <- make_checkpoint_manifest(
    checkpoint_root = checkpoint_root,
    checkpoint_id = "002-compatible",
    planned_groups = c("female", "male"),
    source_fingerprint = "fingerprint-a"
  )

  discovered <- find_compatible_checkpoint_manifest(
    checkpoint_root = checkpoint_root,
    run_signature = "run-signature",
    config_signature = "config-signature",
    grouping_signature = "grouping-signature",
    source_type = "csv",
    planned_groups = c("female", "male"),
    source_fingerprint = "fingerprint-a"
  )

  expect_type(discovered, "list")
  expect_identical(discovered$checkpoint_id, compatible_manifest$checkpoint_id)
  expect_identical(discovered$source_metadata$source_fingerprint, "fingerprint-a")
})

test_that("completed checkpoint group results restore all completed artifacts", {
  checkpoint_root <- make_checkpoint_root()
  on.exit(unlink(checkpoint_root, recursive = TRUE, force = TRUE), add = TRUE)
  group_results <- make_real_group_results(2L)
  planned_groups <- vapply(group_results, function(group_result) group_result$group_label, character(1))
  manifest <- make_checkpoint_manifest(checkpoint_root, planned_groups = planned_groups)

  for (group_result in group_results) {
    artifact_entry <- write_group_checkpoint_artifact(manifest$checkpoint_dir, group_result)
    manifest <- update_checkpoint_manifest_group_result(manifest, group_result, artifact_entry)
  }

  write_checkpoint_manifest(manifest)
  restored_manifest <- read_checkpoint_manifest(manifest$checkpoint_dir)
  restored_groups <- load_checkpoint_group_results(restored_manifest)

  expect_identical(names(restored_groups), planned_groups)
  for (index in seq_along(group_results)) {
    original <- group_results[[index]]
    restored <- restored_groups[[original$group_label]]

    expect_s3_class(restored, "refiner_group_execution_result")
    expect_identical(restored$group_label, original$group_label)
    expect_identical(restored$row_indices, original$row_indices)
    expect_identical(restored$status, "completed")
    expect_checkpoint_interval_table_equal(restored$execution_result$interval_table, original$execution_result$interval_table)
    expect_identical(restored$execution_result$config, original$execution_result$config)
  }
})
