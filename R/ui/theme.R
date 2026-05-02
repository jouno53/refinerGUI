# Theme and Layout Helpers for Phase 11 UI Refresh
# Purpose: Extract styling and component logic from app_ui.R for reuse and centralization

# Theme tokens — semantic color and spacing constants
theme_tokens <- function() {
  list(
    # Color palette
    color = list(
      background = "#f3efe7",
      text_primary = "#1f2a30",
      border_light = "#d7dbdf",
      panel_bg = "#f6f7f8",
      success = "#e8f6ec",
      success_border = "#6db37c",
      success_text = "#14361d",
      error = "#fdeeee",
      error_border = "#d37b7b",
      error_text = "#5a1717",
      info = "#f6f7f8",
      info_border = "#d7dbdf",
      info_text = "#1f2a30",
      spinner = "#163a59"
    ),
    # Spacing (in pixels)
    spacing = list(
      xs = "4px",
      sm = "8px",
      md = "12px",
      lg = "16px",
      xl = "24px",
      xxl = "40px"
    ),
    # Typography
    typography = list(
      font_family = "'Segoe UI', Tahoma, Geneva, Verdana, sans-serif",
      font_size_base = "14px",
      font_size_h1 = "24px",
      font_size_h3 = "18px",
      font_size_h4 = "16px",
      line_height = "1.5"
    ),
    # Borders and shadows
    border = list(
      radius = "8px",
      width = "1px",
      color = "#d7dbdf"
    ),
    # Layout
    layout = list(
      max_width = "1480px",
      content_padding = "20px 18px 40px"
    )
  )
}

# CSS variables injected into the document head
inject_theme_css_variables <- function() {
  tokens <- theme_tokens()
  css_vars <- paste(
    ":root {",
    sprintf("  --color-background: %s;", tokens$color$background),
    sprintf("  --color-text-primary: %s;", tokens$color$text_primary),
    sprintf("  --color-border-light: %s;", tokens$color$border_light),
    sprintf("  --color-panel-bg: %s;", tokens$color$panel_bg),
    sprintf("  --color-success: %s;", tokens$color$success),
    sprintf("  --color-error: %s;", tokens$color$error),
    sprintf("  --spacing-xs: %s;", tokens$spacing$xs),
    sprintf("  --spacing-sm: %s;", tokens$spacing$sm),
    sprintf("  --spacing-md: %s;", tokens$spacing$md),
    sprintf("  --spacing-lg: %s;", tokens$spacing$lg),
    sprintf("  --spacing-xl: %s;", tokens$spacing$xl),
    sprintf("  --spacing-xxl: %s;", tokens$spacing$xxl),
    sprintf("  --layout-max-width: %s;", tokens$layout$max_width),
    sprintf("  --border-radius: %s;", tokens$border$radius),
    "}",
    sep = "\n"
  )
  shiny::HTML(css_vars)
}

# Global app styles
get_global_styles <- function() {
  paste(
    "body {",
    "  background: var(--color-background);",
    "  color: var(--color-text-primary);",
    "  font-family: 'Segoe UI', Tahoma, Geneva, Verdana, sans-serif;",
    "  font-size: 14px;",
    "  line-height: 1.5;",
    "}",
    "",
    ".app-shell {",
    "  max-width: var(--layout-max-width);",
    "  margin: 0 auto;",
    "  padding: 20px 18px var(--spacing-xxl);",
    "}",
    "",
    ".app-header {",
    "  display: flex;",
    "  align-items: center;",
    "  gap: var(--spacing-lg);",
    "  margin-bottom: 18px;",
    "  padding-bottom: 14px;",
    "  border-bottom: 1px solid var(--color-border-light);",
    "}",
    "",
    ".app-header-icon {",
    "  width: 64px;",
    "  height: 64px;",
    "  flex-shrink: 0;",
    "}",
    "",
    ".app-header-text h1 {",
    "  margin: 0;",
    "  font-size: 28px;",
    "  font-weight: 600;",
    "}",
    "",
    ".app-lede {",
    "  margin: 0;",
    "  font-size: 15px;",
    "  color: var(--color-text-primary);",
    "  opacity: 0.8;",
    "}",
    "",
    ".panel {",
    "  background: var(--color-panel-bg);",
    "  border: 1px solid var(--color-border-light);",
    "  border-radius: var(--border-radius);",
    "  padding: 14px 16px;",
    "  margin-bottom: 14px;",
    "}",
    "",
    ".panel h3 {",
    "  margin: 0 0 10px 0;",
    "  font-size: 18px;",
    "  font-weight: 600;",
    "}",
    "",
    ".panel h4 {",
    "  margin: 10px 0 6px 0;",
    "  font-size: 14px;",
    "  font-weight: 600;",
    "  text-transform: uppercase;",
    "  letter-spacing: 0.5px;",
    "  color: var(--color-text-primary);",
    "  opacity: 0.7;",
    "}",
    "",
    ".panel-intro {",
    "  margin: 0 0 12px 0;",
    "  font-size: 13px;",
    "  line-height: 1.45;",
    "  opacity: 0.82;",
    "}",
    "",
    ".section-note {",
    "  margin: 0 0 10px 0;",
    "  font-size: 12px;",
    "  opacity: 0.78;",
    "}",
    "",
    ".status-success {",
    "  padding: 10px 12px;",
    "  border-radius: var(--border-radius);",
    "  margin-bottom: 10px;",
    "  background: var(--color-success);",
    "  border: 1px solid var(--color-success-border);",
    "  color: var(--color-success-text);",
    "}",
    "",
    ".status-error {",
    "  padding: 10px 12px;",
    "  border-radius: var(--border-radius);",
    "  margin-bottom: 10px;",
    "  background: var(--color-error);",
    "  border: 1px solid var(--color-error-border);",
    "  color: var(--color-error-text);",
    "}",
    "",
    ".status-info {",
    "  padding: 10px 12px;",
    "  border-radius: var(--border-radius);",
    "  margin-bottom: 10px;",
    "  background: var(--color-info);",
    "  border: 1px solid var(--color-info-border);",
    "  color: var(--color-info-text);",
    "}",
    "",
    ".stack-tight > * + * {",
    "  margin-top: 10px;",
    "}",
    "",
    ".compact-field-grid {",
    "  display: grid;",
    "  grid-template-columns: repeat(2, minmax(0, 1fr));",
    "  gap: 10px 14px;",
    "  align-items: start;",
    "}",
    "",
    ".compact-field-grid > .form-group {",
    "  margin-bottom: 0;",
    "}",
    "",
    ".compact-field-grid .shiny-input-container {",
    "  width: 100%;",
    "  margin-bottom: 0;",
    "}",
    "",
    ".workspace-top-row,",
    ".workspace-results-row {",
    "  margin-left: -7px;",
    "  margin-right: -7px;",
    "}",
    "",
    ".workspace-top-row > [class*='col-'],",
    ".workspace-results-row > [class*='col-'] {",
    "  padding-left: 7px;",
    "  padding-right: 7px;",
    "}",
    "",
    ".workspace-side-column .panel {",
    "  margin-bottom: 12px;",
    "}",
    "",
    ".sticky-run-shell {",
    "  position: sticky;",
    "  top: 12px;",
    "  z-index: 10;",
    "  margin-bottom: 12px;",
    "  padding: 12px;",
    "  border: 1px solid rgba(22, 58, 89, 0.14);",
    "  border-radius: var(--border-radius);",
    "  background: linear-gradient(180deg, rgba(246, 247, 248, 0.98), rgba(243, 239, 231, 0.96));",
    "  box-shadow: 0 8px 20px rgba(31, 42, 48, 0.08);",
    "}",
    "",
    ".run-header-row {",
    "  display: flex;",
    "  align-items: center;",
    "  justify-content: space-between;",
    "  gap: 12px;",
    "  margin-bottom: 10px;",
    "}",
    "",
    ".run-header-row h4 {",
    "  margin: 0;",
    "}",
    "",
    ".run-header-row .shiny-html-output,",
    ".run-header-row .btn {",
    "  margin-bottom: 0;",
    "}",
    "",
    ".results-workspace {",
    "  margin-top: 12px;",
    "  padding: 12px;",
    "  border: 1px solid rgba(31, 42, 48, 0.08);",
    "  border-radius: var(--border-radius);",
    "  background: rgba(255, 255, 255, 0.35);",
    "}",
    "",
    ".results-workspace .nav-tabs {",
    "  margin-bottom: 8px;",
    "}",
    "",
    ".results-workspace .tab-content {",
    "  min-height: 220px;",
    "}",
    "",
    ".panel table {",
    "  margin-bottom: 0;",
    "}",
    "",
    ".panel .form-group {",
    "  margin-bottom: 10px;",
    "}",
    "",
    ".panel .radio {",
    "  margin-top: 4px;",
    "  margin-bottom: 4px;",
    "}",
    "",
    ".panel .help-block {",
    "  margin-bottom: 0;",
    "}",
    "",
    ".spinner {",
    "  display: inline-block;",
    "  width: 16px;",
    "  height: 16px;",
    "  border: 2px solid rgba(22, 58, 89, 0.2);",
    "  border-top-color: var(--color-spinner);",
    "  border-radius: 50%;",
    "  animation: spin 1.2s linear infinite;",
    "  vertical-align: middle;",
    "  margin-left: var(--spacing-sm);",
    "}",
    "",
    "@keyframes spin {",
    "  to { transform: rotate(360deg); }",
    "}",
    "",
    ".section-advanced {",
    "  margin-top: 12px;",
    "  padding-top: 12px;",
    "  border-top: 1px solid var(--color-border-light);",
    "}",
    "",
    ".section-advanced summary {",
    "  cursor: pointer;",
    "  font-weight: 600;",
    "  color: var(--color-text-primary);",
    "  padding: 6px 0;",
    "  user-select: none;",
    "}",
    "",
    ".section-advanced summary:hover {",
    "  opacity: 0.8;",
    "}",
    "",
    ".section-advanced-content {",
    "  margin-top: 8px;",
    "  padding: 10px 12px;",
    "  background: rgba(31, 42, 48, 0.02);",
    "  border-radius: var(--border-radius);",
    "  border-left: 3px solid var(--color-border-light);",
    "}",
    "",
    ".section-grouped {",
    "  margin: 12px 0;",
    "  padding: 10px 12px;",
    "  background: rgba(109, 179, 124, 0.05);",
    "  border: 1px solid rgba(109, 179, 124, 0.3);",
    "  border-radius: var(--border-radius);",
    "}",
    "",
    ".section-grouped h5 {",
    "  margin: 0 0 var(--spacing-sm) 0;",
    "  font-size: 14px;",
    "  font-weight: 600;",
    "  color: #14361d;",
    "}",
    "",
    "@media (max-width: 1199px) {",
    "  .compact-field-grid {",
    "    grid-template-columns: 1fr;",
    "  }",
    "  .sticky-run-shell {",
    "    position: static;",
    "  }",
    "}",
    "",
    "@media (max-width: 768px) {",
    "  .app-shell {",
    "    padding: var(--spacing-lg) var(--spacing-sm) var(--spacing-lg);",
    "  }",
    "  .app-header {",
    "    flex-direction: column;",
    "    align-items: flex-start;",
    "  }",
    "  .app-header-icon {",
    "    width: 48px;",
    "    height: 48px;",
    "  }",
    "  .compact-field-grid {",
    "    grid-template-columns: 1fr;",
    "  }",
    "  .run-header-row {",
    "    flex-direction: column;",
    "    align-items: stretch;",
    "  }",
    "}",
    sep = "\n"
  )
}

# Panel builder with consistent styling
build_panel <- function(title, ..., id = NULL) {
  shiny::wellPanel(
    class = "panel",
    style = "background: var(--color-panel-bg); border: 1px solid var(--color-border-light); border-radius: var(--border-radius);",
    if (!is.null(title)) shiny::tags$h3(title),
    ...
  )
}

# Status card builders
build_status_card <- function(type = "info", title = NULL, message = NULL, details = NULL) {
  class_name <- sprintf("status-%s", type)
  
  shiny::tags$div(
    class = class_name,
    if (!is.null(title)) shiny::tags$strong(title),
    if (!is.null(message)) shiny::tags$p(message),
    if (!is.null(details)) shiny::tags$p(sprintf("Details: %s", details))
  )
}

# Responsive grid helpers
grid_row <- function(...) {
  shiny::fluidRow(...)
}

grid_col_6 <- function(...) {
  shiny::column(6, ...)
}

# Branding header with icon
build_branded_header <- function(title, lede, icon_path = NULL) {
  header_content <- shiny::tags$div(
    class = "app-header",
    if (!is.null(icon_path)) {
      shiny::tags$img(
        class = "app-header-icon",
        src = icon_path,
        alt = "refineR Logo"
      )
    },
    shiny::tags$div(
      class = "app-header-text",
      shiny::tags$h1(title),
      shiny::tags$p(class = "app-lede", lede)
    )
  )
  
  header_content
}

# Collapsible section for advanced or secondary controls
build_collapsible_section <- function(title, ..., open = FALSE) {
  shiny::tags$details(
    class = "section-advanced",
    open = if (open) "open" else NULL,
    shiny::tags$summary(paste("▼", title)),
    shiny::tags$div(
      class = "section-advanced-content",
      ...
    )
  )
}

# Grouped workflow section (highlighted background)
build_grouped_section <- function(title, ...) {
  shiny::tags$fieldset(
    class = "section-grouped",
    shiny::tags$h5(title),
    ...
  )
}

# Export all helpers for use in app_ui.R
export_theme_helpers <- function() {
  list(
    theme_tokens = theme_tokens,
    inject_theme_css_variables = inject_theme_css_variables,
    get_global_styles = get_global_styles,
    build_panel = build_panel,
    build_status_card = build_status_card,
    grid_row = grid_row,
    grid_col_6 = grid_col_6,
    build_branded_header = build_branded_header,
    build_collapsible_section = build_collapsible_section,
    build_grouped_section = build_grouped_section
  )
}
