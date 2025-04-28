# Connect to your database.
con <- connect_db()
# Read the pipeline_logs table.(select * student_alexandre.pipeline_logs)
db <- DBI::dbGetQuery(con, "select * student_alexandre.pipeline_logs")
# Analyze the logs from the last 24 hours.
# Generate a report file with the following content:
#   The total number of executions.
# The number of successful executions.
# The number of failed executions.
# A list of any errors if available.

