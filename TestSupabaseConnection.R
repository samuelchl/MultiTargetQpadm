# Install the necessary package if not already installed
#install.packages("RPostgres")

# Load the RPostgres package
library(RPostgres)

# Define the connection details
host <- "aws-0-eu-west-3.pooler.supabase.com"
port <- 6543
dbname <- "postgres"
user <- "postgres.zctxxrgkafjrieubmcuz"
password <- "leACMVQ7MFH9TtFf"  # Replace with your actual password

# Establish a connection to the PostgreSQL database
con <- dbConnect(RPostgres::Postgres(),
                 dbname = dbname,
                 host = host,
                 port = port,
                 user = user,
                 password = password)

# Test the connection by running a simple query
result <- dbGetQuery(con, "SELECT tablename FROM pg_tables WHERE schemaname = 'public';")

# Print the result to verify the connection
print(result)

# Disconnect from the database
dbDisconnect(con)

# Confirm the disconnection
cat("Disconnected from the database.\n")
