source("R/ingest/contracts.R", local = TRUE)
source("R/wrapper/contracts.R", local = TRUE)
source("R/ui/theme.R", local = TRUE)
source("R/reactive/app_server.R", local = TRUE)
source("R/ui/app_ui.R", local = TRUE)

app <- shiny::shinyApp(
  ui = build_app_ui(),
  server = build_app_server()
)

app