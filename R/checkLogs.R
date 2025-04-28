#' Extract and analyze pipeline logs from the last 24 hours
#'
#' This function connects to the database, retrieves the `pipeline_logs` table,
#' filters the entries from the last 24 hours, keeps only relevant columns, adds
#' the number of executions, and generates an HTML report.
#'
#' @param output_file A character string. Path where the HTML output will be saved. Default is `"output.html"`.
#'
#' @return Invisibly returns the filtered `data.table` containing the last 24 hours of logs.
#'
#' @details
#' - Connects to the database using `connect_db()`.
#' - Retrieves the 100 most recent entries from `student_alexandre.pipeline_logs`.
#' - Filters logs to include only those from the past 24 hours based on the `timestamp` field.
#' - Keeps only the following columns: `id`, `symbol`, `status`, `message`, `timestamp`.
#' - Adds a column `numberOfExecution` with the total number of rows.
#' - Generates an HTML report using `{htmlTable}` and saves it to the specified file.
#'
#' @import data.table
#' @import htmlTable
#' @importFrom DBI dbGetQuery dbDisconnect
#' @export
extract_last24h_pipeline_logs <- function(output_file = "output.html") {
  # Connect to the database
  con <- connect_db()
  
  # Read the table
  db <- DBI::dbGetQuery(
    conn = con,
    statement = "SELECT * FROM student_alexandre.pipeline_logs ORDER BY id ASC LIMIT 100"
  ) |> data.table::as.data.table()
  
  # Close the connection
  DBI::dbDisconnect(con)
  
  # Define the limit time
  limit_time <- Sys.time() - 24 * 3600
  
  # Filter the logs from the last 24 hours
  db[, timestamp := as.POSIXct(timestamp)]
  db24h <- db[timestamp >= limit_time]
  
  # Keep useful columns and add the number of executions
  db24h <- db24h[, .(id, symbol, status, message, timestamp)]
  db24h[, numberOfExecution := nrow(db24h)]
  
  # Generate HTML report
  html_output <- htmlTable::htmlTable(db24h)
  writeLines(html_output, output_file)
  
  # Return the table (invisible to avoid printing large outputs)
  invisible(db24h)
}
