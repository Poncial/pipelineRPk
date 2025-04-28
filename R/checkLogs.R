#' Generate a Markdown and HTML report from pipeline logs of the last 24 hours
#'
#' This function connects to the database, retrieves the latest 100 pipeline logs,
#' filters the ones from the last 24 hours, and creates both a Markdown (.md) and HTML (.html) report with summary statistics and a detailed table.
#'
#' @param outputFile A character string. Path to the output Markdown file. Default is `"output.md"`.
#'
#' @return Invisibly returns the filtered `data.table` with the logs from the last 24 hours.
#'
#' @details
#' The report includes:
#' - Total number of executions
#' - Number of successful executions (status == "ok")
#' - Number of failed executions (any status different from "ok")
#' - List of error messages
#' - Full detailed table of logs
#'
#' @import data.table
#' @importFrom DBI dbGetQuery dbDisconnect
#' @importFrom rmarkdown render
#' @export
extractLast24hPipelineLogsMd <- function(outputFile = "output.md") {
  # Connect to the database
  con <- connect_db()
  
  # Ensure disconnection even in case of error
  on.exit(DBI::dbDisconnect(con), add = TRUE)
  
  # Read the table
  db <- DBI::dbGetQuery(
    conn = con,
    statement = "SELECT * FROM student_alexandre.pipeline_logs ORDER BY id ASC LIMIT 100"
  ) |> data.table::as.data.table()
  
  # Define limit time
  limitTime <- Sys.time() - 24 * 3600
  
  # Filter logs from the last 24h
  db[, timestamp := as.POSIXct(timestamp)]
  db24h <- db[timestamp >= limitTime]
  
  # Keep only relevant columns
  db24h <- db24h[, .(id, symbol, status, message, timestamp)]
  
  # Compute statistics (camelCase)
  totalExecutions <- nrow(db24h)
  successfulExecutions <- db24h[status == "ok", .N]
  failedExecutions <- db24h[status != "ok", .N]
  errorMessages <- db24h[status != "ok" & !is.na(message), unique(message)]
  
  # Build Markdown report
  mdContent <- paste0(
    "---\ntitle: \"Pipeline Logs Report\"\ndate: \"", format(Sys.time(), "%Y-%m-%d %H:%M:%S"), "\"\noutput: html_document\n---\n\n",
    
    "# Pipeline Logs Report (Last 24 Hours)\n\n",
    
    "## Summary\n\n",
    "- **Total number of executions:** ", totalExecutions, "\n",
    "- **Number of successful executions:** ", successfulExecutions, "\n",
    "- **Number of failed executions:** ", failedExecutions, "\n\n",
    
    "## Error Messages\n\n",
    if (length(errorMessages) == 0) {
      "- No errors recorded.\n"
    } else {
      paste0("- ", paste(errorMessages, collapse = "\n- "), "\n")
    },
    
    "\n## Detailed Logs\n\n"
  )
  
  # Generate Markdown table
  if (totalExecutions > 0) {
    header <- paste0("| ", paste(names(db24h), collapse = " | "), " |\n")
    separator <- paste0("| ", paste(rep("---", ncol(db24h)), collapse = " | "), " |\n")
    rows <- apply(db24h, 1, function(row) {
      paste0("| ", paste(row, collapse = " | "), " |")
    })
    
    tableMd <- paste0(header, separator, paste(rows, collapse = "\n"), "\n")
    mdContent <- paste0(mdContent, tableMd)
  } else {
    mdContent <- paste0(mdContent, "_No executions in the last 24 hours._\n")
  }
  
  # Write Markdown file
  writeLines(mdContent, outputFile)
  
  # Render Markdown to HTML
  htmlOutput <- sub("\\.md$", ".html", outputFile)
  
  rmarkdown::render(
    input = outputFile,
    output_format = "html_document",
    output_file = htmlOutput,
    quiet = TRUE
  )
  
  # Return the filtered data invisibly
  invisible(db24h)
}
