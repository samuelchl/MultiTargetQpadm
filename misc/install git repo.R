# Install necessary packages if not already installed
if (!requireNamespace("git2r", quietly = TRUE)) {
  install.packages("git2r")
}
if (!requireNamespace("stringr", quietly = TRUE)) {
  install.packages("stringr")
}

# Load the packages
library(git2r)
library(stringr)

# Clone the repository
repo_url <- "https://github.com/uqrmaie1/admixtools.git"
repo_dir <- "admixtools"
clone(repo_url, repo_dir)

# Navigate into the repository directory
setwd(repo_dir)

# Define the path to the file to be modified
file_path <- file.path("R", "qpdstat.R")

# Read the file
file_content <- readLines(file_path)

# Replace maxcomb value
file_content <- str_replace(file_content, "maxcomb = 1e6", "maxcomb = 2e6")

# Write the modified content back to the file
writeLines(file_content, file_path)

# Verify the change
modified_content <- readLines(file_path)
grep("maxcomb =", modified_content, value = TRUE)
