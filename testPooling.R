library(DBI)
library(RPostgres)
library(pool)
library(furrr)
library(future)

# Set up the connection pool
pg_pool <- dbPool(
  drv = RPostgres::Postgres(),
  dbname = "postgres",
  host = "aws-0-eu-west-3.pooler.supabase.com",
  port = 6543,
  user = "postgres.zctxxrgkafjrieubmcuz",
  password = "leACMVQ7MFH9TtFf",
  minSize = 1,  # Minimum number of connections in the pool
  maxSize = 15,  # Maximum number of connections in the pool
  idleTimeout = 600  # Time in seconds to keep an idle connection before closing it
)

# Function to get a connection from the pool
get_pg_connection <- function() {
  con <- poolCheckout(pg_pool)
  return(con)
}

# Function to return the connection to the pool
return_pg_connection <- function(con) {
  poolReturn(con)
}

# Properly close all connections in the pool when done
close_pg_pool <- function() {
  poolClose(pg_pool)
}

# Function to perform a simple query using a connection from the pool
perform_db_query <- function() {
  con <- NULL
  
  tryCatch({
    # Get a connection from the pool
    con <- get_pg_connection()
    
    # Perform a simple query
    result <- dbGetQuery(con, "SELECT current_timestamp AS time")
    
    # Return the result
    return(result)
    
  }, error = function(e) {
    # Handle any errors
    cat("Error: ", e$message, "\n")
    return(NULL)
    
  }, finally = {
    # Return the connection to the pool
    if (!is.null(con)) {
      return_pg_connection(con)
    }
  })
}

# Set up parallel processing with the number of cores available
plan(multisession, workers = parallel::detectCores() - 1)

# Perform multiple database queries in parallel
results <- future_map(1:10, function(i) {
  perform_db_query()
})

# Print the results
print(results)

# Properly close the pool when done
close_pg_pool()
