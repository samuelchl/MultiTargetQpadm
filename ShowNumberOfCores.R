# Load necessary library
library(parallel)

# Get the number of logical cores
logical_cores <- detectCores(logical = TRUE)

# Get the number of physical cores
physical_cores <- detectCores(logical = FALSE)

# Print the number of logical and physical cores
cat("Number of logical cores:", logical_cores, "\n")
cat("Number of physical cores:", physical_cores, "\n")
