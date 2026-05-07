test_that("default execution config is explicit and stable", {
  expect_identical(
    default_execution_config(),
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
  )
})

test_that("execution config normalization accepts user-facing scalar strings", {
  normalized <- normalize_execution_config(list(
    NBootstrap = "0",
    seed = "123",
    RIperc = "0.025, 0.975",
    CIprop = "0.95",
    UMprop = "0.90"
  ))

  expect_identical(normalized, default_execution_config())
})

test_that("execution config preflight and assertion return normalized config", {
  config <- default_execution_config()
  preflight <- validate_execution_config(config)

  expect_s3_class(preflight, "refiner_preflight_result")
  expect_true(isTRUE(preflight$ok))
  expect_identical(preflight$config, normalize_execution_config(config))
  expect_identical(assert_valid_execution_config(config), normalize_execution_config(config))
})

test_that("invalid execution configs fail clearly", {
  expect_invalid_config <- function(overrides, pattern) {
    preflight <- validate_execution_config(utils::modifyList(default_execution_config(), overrides))

    expect_false(isTRUE(preflight$ok))
    expect_match(paste(preflight$errors, collapse = " "), pattern, fixed = FALSE)
  }

  expect_invalid_config(list(model = "badModel"), "model must be one of")
  expect_invalid_config(list(NBootstrap = -1L), "NBootstrap must be zero or greater")
  expect_invalid_config(list(NBootstrap = 1.5), "NBootstrap must be an integer value")
  expect_invalid_config(list(seed = 1.5), "seed must be an integer value")
  expect_invalid_config(list(RIperc = c(0, 0.975)), "strictly between 0 and 1")
  expect_invalid_config(list(RIperc = c(0.975, 0.025)), "sorted in ascending order")
  expect_invalid_config(list(RIperc = c(0.025, 0.025)), "sorted in ascending order")
  expect_invalid_config(list(pointEst = "badPoint"), "pointEst must be either")
  expect_invalid_config(list(pointEst = "medianBS", NBootstrap = 0L), "medianBS requires NBootstrap")
  expect_invalid_config(list(Scale = "badScale"), "Scale must be one of")
  expect_invalid_config(list(CIprop = 1), "CIprop must be strictly between 0 and 1")
  expect_invalid_config(list(UMprop = "bad"), "UMprop must be numeric")
})

test_that("app-supported refineR argument names exist and defaults are documented", {
  findri_formals <- formals(refineR::findRI)
  getri_formals <- formals(refineR::getRI)
  app_config <- default_execution_config()

  expect_true(all(c("Data", "model", "NBootstrap", "seed") %in% names(findri_formals)))
  expect_true(all(c("x", "RIperc", "CIprop", "UMprop", "pointEst", "Scale") %in% names(getri_formals)))

  expect_identical(app_config$model, eval(findri_formals$model)[[1]])
  expect_identical(app_config$NBootstrap, as.integer(findri_formals$NBootstrap))
  expect_identical(app_config$seed, as.integer(findri_formals$seed))

  expect_identical(app_config$RIperc, eval(getri_formals$RIperc))
  expect_identical(app_config$CIprop, as.numeric(getri_formals$CIprop))
  expect_identical(app_config$UMprop, as.numeric(getri_formals$UMprop))
  expect_identical(app_config$pointEst, eval(getri_formals$pointEst)[[1]])
  expect_identical(app_config$Scale, eval(getri_formals$Scale)[[1]])

  expect_null(findri_formals$Data)
})

test_that("refineR call summary reports explicit app values", {
  summary <- format_refiner_call_summary(default_execution_config())

  expect_match(
    summary,
    'refineR::findRI\\(Data = <normalized_values>, model = "BoxCox", NBootstrap = 0, seed = 123\\)'
  )
  expect_match(
    summary,
    'refineR::getRI\\(<fit>, RIperc = c\\(0.025, 0.975\\), CIprop = 0.95, UMprop = 0.9, pointEst = "fullDataEst", Scale = "original"\\)'
  )
})
