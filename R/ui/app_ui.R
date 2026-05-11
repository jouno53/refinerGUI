build_placeholder_panel <- function(title, output_id) {
  shiny::wellPanel(
    style = "min-height: 180px; background: #f6f7f8; border: 1px solid #d7dbdf;",
    shiny::tags$h3(title),
    shiny::textOutput(output_id)
  )
}

build_compact_field_grid <- function(...) {
  shiny::div(
    class = "compact-field-grid",
    ...
  )
}

build_workflow_steps <- function() {
  labels <- c("Data + Grouping", "Parameters", "Run", "Results")

  shiny::div(
    class = "workflow-steps",
    lapply(seq_along(labels), function(index) {
      shiny::div(
        class = "workflow-step",
        shiny::span(class = "workflow-step-number", index),
        shiny::span(class = "workflow-step-label", labels[[index]])
      )
    })
  )
}

build_data_panel <- function() {
  build_panel(
    "Data & Mapping",
    step_number = 1,
    shiny::div(
      class = "stack-tight",
      shiny::radioButtons(
        inputId = "data_source",
        label = "Data source",
        choices = c("Packaged sample dataset" = "sample", "Upload CSV" = "csv"),
        selected = "sample",
        inline = TRUE
      ),
      shiny::conditionalPanel(
        condition = "input.data_source === 'sample'",
        shiny::selectInput(
          inputId = "sample_dataset",
          label = "Sample dataset",
          choices = character(),
          selectize = FALSE
        )
      ),
      shiny::conditionalPanel(
        condition = "input.data_source === 'csv'",
        shiny::fileInput(
          inputId = "csv_file",
          label = "CSV file",
          accept = c(".csv", "text/csv")
        ),
        shiny::textInput(
          inputId = "csv_reference_path",
          label = "CSV reference path for settings import/export",
          value = "",
          placeholder = "C:/path/to/source.csv"
        )
      ),
      build_compact_field_grid(
        shiny::selectInput(
          inputId = "analyte_column",
          label = "Analyte column",
          choices = character(),
          selectize = FALSE
        ),
        shiny::selectInput(
          inputId = "sex_column",
          label = "Sex column (optional)",
          choices = c("None" = ""),
          selectize = FALSE
        ),
        shiny::selectInput(
          inputId = "age_column",
          label = "Age column (optional)",
          choices = c("None" = ""),
          selectize = FALSE
        ),
        shiny::selectInput(
          inputId = "grouping_mode",
          label = "Grouping mode",
          choices = c(
            "Overall only" = "overall",
            "Sex only" = "sex",
            "Age only" = "age",
            "Sex by age" = "sex_age"
          ),
          selected = "overall",
          selectize = FALSE
        )
      ),
      shiny::conditionalPanel(
        condition = "input.grouping_mode === 'sex' || input.grouping_mode === 'sex_age'",
        shiny::textAreaInput(
          inputId = "sex_mapping_text",
          label = "Sex mapping",
          placeholder = "female = F, Female\nmale = M, Male",
          rows = 3,
          resize = "vertical"
        )
      ),
      shiny::conditionalPanel(
        condition = "input.grouping_mode === 'age' || input.grouping_mode === 'sex_age'",
        shiny::textAreaInput(
          inputId = "age_band_text",
          label = "Age bands",
          placeholder = "0-18\n18-30\n30+",
          rows = 4,
          resize = "vertical"
        ),
        shiny::tags$p(class = "section-note", "Age-band boundaries are assigned to the earlier band.")
      ),
      shiny::uiOutput("csv_reference_status"),
      shiny::uiOutput("data_quality_snapshot"),
      shiny::uiOutput("validation_status"),
      shiny::uiOutput("grouping_status"),
      shiny::uiOutput("grouping_preview_ui")
    ),
    build_collapsible_section(
      "Preview Data",
      shiny::tableOutput("preview_table")
    )
  )
}

build_parameters_panel <- function() {
  build_panel(
    "Parameters",
    step_number = 2,
    build_compact_field_grid(
      shiny::selectInput(
        inputId = "param_model",
        label = shiny::tags$span(
          "Model",
          title = paste(
            "Transformation model for findRI().",
            "BoxCox: standard one-parameter Box-Cox (fastest).",
            "modBoxCoxFast: adds a shift search with one iteration (moderate speed).",
            "modBoxCox: full shift search (slowest, most flexible)."
          )
        ),
        choices = character(),
        selectize = FALSE
      ),
      shiny::numericInput(
        inputId = "param_nbootstrap",
        label = shiny::tags$span(
          "Bootstrap iterations",
          title = paste(
            "Number of bootstrap repetitions passed to findRI().",
            "0 disables bootstrapping (faster; no confidence intervals).",
            "Set 200 or more to enable CIs and the medianBS point estimate."
          )
        ),
        value = 0,
        min = 0,
        step = 1
      ),
      shiny::numericInput(
        inputId = "param_seed",
        label = shiny::tags$span(
          "Seed",
          title = "Integer seed for reproducible bootstrap sampling. Only used when Bootstrap iterations > 0."
        ),
        value = 123,
        step = 1
      ),
      shiny::textInput(
        inputId = "param_riperc",
        label = shiny::tags$span(
          "RI percentiles",
          title = paste(
            "Comma-separated probabilities defining the reference interval bounds.",
            "Must be strictly between 0 and 1, sorted ascending.",
            "Clinical default 0.025, 0.975 gives the central 95% reference interval."
          )
        ),
        value = "0.025, 0.975"
      )
    ),
    build_collapsible_section(
      "Fine Tuning",
      build_compact_field_grid(
        shiny::numericInput(
          inputId = "param_ciprop",
          label = shiny::tags$span(
            "CI proportion",
            title = paste(
              "Coverage probability for the bootstrap confidence interval (0 < value < 1).",
              "Requires Bootstrap iterations > 0. Default: 0.95."
            )
          ),
          value = 0.95,
          min = 0.01,
          max = 0.99,
          step = 0.01
        ),
        shiny::numericInput(
          inputId = "param_umprop",
          label = shiny::tags$span(
            "UM proportion",
            title = paste(
              "Coverage probability for the uncertainty margin region (0 < value < 1).",
              "Uncertainty margins do not require bootstrap. Default: 0.90."
            )
          ),
          value = 0.90,
          min = 0.01,
          max = 0.99,
          step = 0.01
        ),
        shiny::selectInput(
          inputId = "param_point_est",
          label = shiny::tags$span(
            "Point estimate",
            title = paste(
              "How the point estimate is derived.",
              "fullDataEst: model fit from the full dataset (always available).",
              "medianBS: median model across bootstrap samples (requires Bootstrap iterations > 0)."
            )
          ),
          choices = character(),
          selectize = FALSE
        ),
        shiny::selectInput(
          inputId = "param_scale",
          label = shiny::tags$span(
            "Scale",
            title = paste(
              "Unit scale for reported percentiles.",
              "original: measurement units.",
              "transformed: Box-Cox-transformed units.",
              "zScore: z-score units."
            )
          ),
          choices = character(),
          selectize = FALSE
        )
      )
    ),
    shiny::uiOutput("parameter_preflight_status")
  )
}

build_execution_panel <- function() {
  build_panel(
    "Run",
    step_number = 3,
    shiny::div(
      class = "execution-runway",
      shiny::div(
        class = "sticky-run-shell",
        shiny::div(
          class = "run-header-row",
          shiny::tags$h4("Run Control"),
          shiny::uiOutput("run_button_ui")
        ),
        shiny::uiOutput("workflow_readiness"),
        shiny::uiOutput("execution_status")
      ),
      build_collapsible_section(
        "Current Settings",
        shiny::uiOutput("execution_defaults")
      ),
      build_collapsible_section(
        "Settings Import / Export",
        shiny::uiOutput("experiment_settings_export_ui"),
        shiny::fileInput(
          inputId = "import_settings_file",
          label = "Import settings file",
          accept = c(".json", "application/json")
        ),
        shiny::uiOutput("experiment_settings_status"),
        shiny::uiOutput("experiment_settings_controls_ui")
      ),
      shiny::conditionalPanel(
        condition = "input.data_source === 'csv' && input.grouping_mode !== 'overall'",
        build_grouped_section(
          "Grouped Checkpointing",
          shiny::textInput(
            inputId = "checkpoint_root",
            label = "Checkpoint folder",
            value = "",
            placeholder = "C:/path/to/checkpoints"
          ),
          shiny::uiOutput("checkpoint_status"),
          shiny::uiOutput("checkpoint_controls_ui")
        )
      )
    )
  )
}

build_results_display_controls <- function() {
  shiny::tags$div(
    class = "plot-controls",
    shiny::tags$h4("Plot Controls"),
    shiny::fluidRow(
      shiny::column(
        6,
        shiny::selectInput(
          inputId = "display_point_est",
          label = shiny::tags$span(
            "Point estimate view",
            title = paste(
              "Which point estimate to show in the plot and table.",
              "medianBS requires a fit run with Bootstrap iterations > 0."
            )
          ),
          choices = character(),
          selectize = FALSE
        ),
        shiny::numericInput(
          inputId = "display_nhist",
          label = shiny::tags$span(
            "Histogram bins",
            title = "Number of bins in the background histogram (Nhist in plot.RWDRI). Default: 60."
          ),
          value = 60,
          min = 1,
          step = 1
        ),
        shiny::selectInput(
          inputId = "display_uncertainty_region",
          label = shiny::tags$span(
            "Uncertainty region",
            title = paste(
              "Region drawn around point estimates.",
              "bootstrapCI: bootstrap confidence bands (requires bootstrap fit).",
              "uncertaintyMargin: analytical uncertainty margins (no bootstrap needed)."
            )
          ),
          choices = character(),
          selectize = FALSE
        ),
        shiny::selectInput(
          inputId = "display_col_scheme",
          label = shiny::tags$span(
            "Color scheme",
            title = "Color scheme for the non-pathological distribution and RI shading. green or blue."
          ),
          choices = character(),
          selectize = FALSE
        )
      ),
      shiny::column(
        6,
        shiny::checkboxInput(
          inputId = "display_show_margin",
          label = shiny::tags$span(
            "Show margins",
            title = "Show or hide the confidence/uncertainty margin region on the plot (showMargin in plot.RWDRI)."
          ),
          value = TRUE
        ),
        shiny::checkboxInput(
          inputId = "display_show_pathol",
          label = shiny::tags$span(
            "Show pathological distribution",
            title = "Overlay the estimated pathological component on the plot (showPathol in plot.RWDRI)."
          ),
          value = FALSE
        ),
        shiny::checkboxInput(
          inputId = "display_show_value",
          label = shiny::tags$span(
            "Show interval values",
            title = "Display the numeric reference interval values above the plot (showValue in plot.RWDRI)."
          ),
          value = TRUE
        )
      )
    )
  )
}

build_plot_resize_observer_script <- function() {
  shiny::HTML("
(function() {
  var plotSizeTimer = null;

  function sendPlotSize(options) {
    options = options || {};

    if (!window.Shiny || !Shiny.setInputValue) {
      return;
    }

    var frame = document.getElementById('results_plot_frame');
    if (!frame) {
      return;
    }

    var rect = frame.getBoundingClientRect();
    var width = Math.round(rect.width);
    var height = Math.round(rect.height);

    if (width > 0 && height > 0) {
      Shiny.setInputValue('results_plot_width_px', width, {priority: 'event'});
      Shiny.setInputValue('results_plot_height_px', height, {priority: 'event'});
    }
  }

  function schedulePlotSizeUpdate() {
    if (plotSizeTimer !== null) {
      clearTimeout(plotSizeTimer);
    }

    plotSizeTimer = setTimeout(function() {
      plotSizeTimer = null;
      sendPlotSize();
    }, 350);
  }

  function installPlotResizeObserver() {
    var frame = document.getElementById('results_plot_frame');
    if (!frame || frame.dataset.resizeObserverAttached === 'true') {
      sendPlotSize();
      return;
    }

    frame.dataset.resizeObserverAttached = 'true';

    if (window.ResizeObserver) {
      var observer = new ResizeObserver(schedulePlotSizeUpdate);
      observer.observe(frame);
    }

    sendPlotSize();
  }

  function installPlotResizeGrip() {
    var frame = document.getElementById('results_plot_frame');
    var grip = document.getElementById('results_plot_resize_grip');

    if (!frame || !grip || grip.dataset.resizeGripAttached === 'true') {
      return;
    }

    grip.dataset.resizeGripAttached = 'true';

    function setPlotFrameHeight(height) {
      var nextHeight = Math.max(300, Math.min(760, Math.round(height)));
      frame.style.height = nextHeight + 'px';
      schedulePlotSizeUpdate();
    }

    function startResize(event) {
      event.preventDefault();
      var startY = event.clientY;
      var startHeight = frame.getBoundingClientRect().height;

      function moveResize(moveEvent) {
        setPlotFrameHeight(startHeight + moveEvent.clientY - startY);
      }

      function endResize() {
        document.removeEventListener('mousemove', moveResize);
        document.removeEventListener('mouseup', endResize);
        sendPlotSize();
      }

      document.addEventListener('mousemove', moveResize);
      document.addEventListener('mouseup', endResize);
    }

    grip.addEventListener('mousedown', startResize);
    grip.addEventListener('keydown', function(event) {
      if (event.key === 'ArrowUp' || event.key === 'ArrowDown') {
        event.preventDefault();
        var currentHeight = frame.getBoundingClientRect().height;
        setPlotFrameHeight(currentHeight + (event.key === 'ArrowDown' ? 20 : -20));
      }
    });
  }

  document.addEventListener('DOMContentLoaded', installPlotResizeObserver);
  document.addEventListener('DOMContentLoaded', installPlotResizeGrip);
  document.addEventListener('shiny:connected', installPlotResizeObserver);
  document.addEventListener('shiny:connected', installPlotResizeGrip);
  document.addEventListener('shown.bs.tab', sendPlotSize);
  window.addEventListener('resize', schedulePlotSizeUpdate);
  setTimeout(installPlotResizeObserver, 500);
  setTimeout(installPlotResizeGrip, 500);
})();
")
}

build_results_panel <- function() {
  build_panel(
    "Results",
    step_number = 4,
    shiny::uiOutput("results_status"),
    shiny::uiOutput("group_results_summary_ui"),
    shiny::uiOutput("selected_group_ui"),
    shiny::uiOutput("results_display_preflight"),
    shiny::div(
      class = "results-workspace",
      shiny::div(
        class = "results-balance-grid",
        shiny::div(
          class = "result-stack",
          shiny::div(
            class = "result-block",
            shiny::tags$h4("Summary"),
            shiny::verbatimTextOutput("results_summary", placeholder = TRUE)
          ),
          shiny::div(
            class = "result-block",
            shiny::tags$h4("Interval Table"),
            shiny::tableOutput("results_table")
          )
        ),
        shiny::div(
          class = "result-block",
          shiny::tags$h4("Plot"),
          build_results_display_controls(),
          shiny::tags$div(
            id = "results_plot_frame",
            class = "resizable-plot-frame",
            shiny::plotOutput("results_plot", width = "100%", height = "100%"),
            shiny::tags$div(
              id = "results_plot_resize_grip",
              class = "plot-resize-grip",
              title = "Resize plot",
              `aria-label` = "Resize plot",
              tabindex = "0",
              role = "separator"
            )
          )
        )
      )
    )
  )
}

build_app_ui <- function() {
  shiny::fluidPage(
    shiny::tags$head(
      shiny::tags$title("refineR GUI"),
      shiny::tags$meta(name = "viewport", content = "width=device-width, initial-scale=1.0"),
      shiny::tags$style(shiny::HTML(inject_theme_css_variables())),
      shiny::tags$style(shiny::HTML(get_global_styles())),
      shiny::tags$script(build_plot_resize_observer_script())
    ),
    shiny::div(
      class = "app-shell",
      build_branded_header(
        title = "refinerGUI",
        lede = "Run local refineR reference interval workflows with validated data mapping, reproducible settings, and balanced result review.",
        icon_path = "refinerGUI.png"
      ),
      shiny::div(class = "health-strip", shiny::uiOutput("health_status")),
      build_workflow_steps(),
      shiny::div(
        class = "bench-workspace",
        shiny::div(
          class = "bench-config-stack",
          build_data_panel(),
          build_parameters_panel()
        ),
        shiny::div(
          class = "bench-run-rail",
          shiny::div(class = "run-panel", build_execution_panel())
        )
      ),
      shiny::div(class = "bench-results-row", build_results_panel())
    )
  )
}
