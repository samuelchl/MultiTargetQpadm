# Load necessary libraries
library("admixtools")
library("tidyverse")
library("ggplot2")
library("dplyr")
library("grid")
library("cowplot")
library("future")
library("furrr")
library("parallel")
library("RSQLite")

# Define paths and configurations
prefix_ho <- "c:/datasets/v54.1.p1_HO_public"
progress_log_path <- "c:/qpadmdata/progress.txt"
ongoingCombinationProgress_log_path <- "c:/qpadmdata/ongoing_combination_progress.txt"
all_weights_data_path <- "c:/qpadmdata/all_weights_data.csv"
p_values_path <- "c:/qpadmdata/p_values.csv"
avg_se_path <- "c:/qpadmdata/avg_se.csv"
right_populations_path <- "c:/qpadmdata/right_populations.csv"
rejection_log_path <- "c:/qpadmdata/rejection_log.csv"
db_path <- "c:/qpadmdata/qpadmdata2.db"

# Input strings for right and left populations
input_string_left_static <- "Lebanon_MBA.SG,CanaryIslands_Guanche.SG,Italy_PianSultano_BA.SG"
input_string_left_dynamic <- "Tajikistan_Ksirov_Kushan"
input_string_right_static <- "Mbuti.DG,Israel_PPNB,Russia_MA1_HG.SG,Turkey_Boncuklu_N,Turkey_Epipaleolithic,Morocco_Iberomaurusian,Serbia_IronGates_Mesolithic,Luxembourg_Loschbour.DG,Russia_Karelia_HG,Georgia_Kotias.SG,Iran_GanjDareh_N,China_Tianyuan,Indian_GreatAndaman_100BP.SG,Mongolia_North_N"
input_string_right_dynamic <- "Jordan_PPNB,Russia_Tyumen_HG,Israel_Natufian"

left_static <- unlist(strsplit(input_string_left_static, split = ","))
left_dynamic <- unlist(strsplit(input_string_left_dynamic, split = ","))

right_static <- unlist(strsplit(input_string_right_static, split = ","))
right_dynamic <- unlist(strsplit(input_string_right_dynamic, split = ","))

# Check if both left_static and left_dynamic are empty
if (length(left_static) == 0 && length(left_dynamic) == 0) {
  stop("Both left_static and left_dynamic are empty. At least one must be non-empty.")
}

# Check if both right_static and right_dynamic are empty
if (length(right_static) == 0 && length(right_dynamic) == 0) {
  stop("Both right_static and right_dynamic are empty. At least one must be non-empty.")
}

# List of target populations
targets <- c("Jew_Ashkenazi.HO", "Jew_Moroccan.HO")

# Function to clear a log file
clear_log_file <- function(file_path) {
  write("", file = file_path)
}

# Clear log files
clear_log_file(progress_log_path)
clear_log_file(ongoingCombinationProgress_log_path)
clear_log_file(rejection_log_path)
clear_log_file(right_populations_path)

# Function to determine if in main thread
is_main_thread <- function() {
  Sys.getenv("RSTUDIO_SESSION_PORT") == ""
}

# Custom logging function
log_message <- function(message) {
  main_thread <- if (is_main_thread()) "MainThread" else "WorkerThread"
  workers_count <- future::nbrOfWorkers()
  timestamp <- Sys.time()
  if (is.vector(message) && length(message) > 1) {
    message <- paste(message, collapse = ", ")
  }
  message <- paste("][Time:", timestamp, "]", as.character(message))
  print(paste(message, "\n"))
  write(message, file = progress_log_path, append = TRUE, sep = "\n")
}

log_message2 <- function(prefix_ho, left, right, target) {
  main_thread <- if (is_main_thread()) "MainThread" else "WorkerThread"
  workers_count <- future::nbrOfWorkers()
  timestamp <- Sys.time()
  if (is.vector(left) && length(left) > 1) {
    left_str <- paste(left, collapse = ", ")
  } else {
    left_str <- left
  }
  if (is.vector(right) && length(right) > 1) {
    right_str <- paste(right, collapse = ", ")
  } else {
    right_str <- right
  }
  message <- paste("][Time:", timestamp, "] process_combination", prefix_ho, left_str, right_str, target, sep = " ")
  print(paste(message, "\n"))
  write(message, file = ongoingCombinationProgress_log_path, append = TRUE, sep = "\n")
}

# Initialize list to store rejection reasons
rejection_reasons <- list()

log_rejection <- function(prefix_ho, left, right, target, reason) {
  main_thread <- if (is_main_thread()) "MainThread" else "WorkerThread"
  workers_count <- future::nbrOfWorkers()
  timestamp <- Sys.time()
  if (is.vector(left) && length(left) > 1) {
    left_str <- paste(left, collapse = ", ")
  } else {
    left_str <- left
  }
  if (is.vector(right) && length(right) > 1) {
    right_str <- paste(right, collapse = ", ")
  } else {
    right_str <- right
  }
  message <- paste("][Time:", timestamp, "]", 
                   "Rejected combination:", prefix_ho, left_str, right_str, target, "Reason:", reason)
  print(paste(message, "\n"))
  write(message, file = rejection_log_path, append = TRUE, sep = "\n")
  
  # Store rejection reason in the list
  rejection_reasons <<- append(rejection_reasons, list(data.frame(prefix_ho = prefix_ho, left = left_str, right = right_str, target = target, reason = reason, stringsAsFactors = FALSE)))
}

# Function to check if a combination has been processed before
check_existing_request <- function(con, left, right, target) {
  left_str <- paste(left, collapse = ", ")
  right_str <- paste(right, collapse = ", ")
  query <- paste("SELECT * FROM request WHERE left_id = (SELECT id FROM left_populations WHERE left_combination = '", left_str, "') AND right_id = (SELECT id FROM right_populations WHERE right_combination = '", right_str, "') AND target = '", target, "'", sep = "")
  dbGetQuery(con, query)
}

# Function to fetch result for an existing request
fetch_existing_result <- function(con, left, right, target) {
  left_str <- paste(left, collapse = ", ")
  right_str <- paste(right, collapse = ", ")
  query <- paste("SELECT * FROM result WHERE request_id IN (SELECT id FROM request WHERE left_id = (SELECT id FROM left_populations WHERE left_combination = '", left_str, "') AND right_id = (SELECT id FROM right_populations WHERE right_combination = '", right_str, "') AND target = '", target, "')", sep = "")
  dbGetQuery(con, query)
}

# Function to insert and get left ID
insert_and_get_left_id <- function(con, left_combination) {
  left_str <- paste(left_combination, collapse = ", ")
  query <- paste("SELECT id FROM left_populations WHERE left_combination = '", left_str, "'", sep = "")
  result <- dbGetQuery(con, query)
  
  if (nrow(result) == 0) {
    dbExecute(con, paste("INSERT INTO left_populations (left_combination) VALUES ('", left_str, "')", sep = ""))
    result <- dbGetQuery(con, "SELECT last_insert_rowid() AS id")
  }
  
  return(result$id)
}

# Function to insert and get right ID
insert_and_get_right_id <- function(con, right_combination) {
  right_str <- paste(right_combination, collapse = ", ")
  query <- paste("SELECT id FROM right_populations WHERE right_combination = '", right_str, "'", sep = "")
  result <- dbGetQuery(con, query)
  
  if (nrow(result) == 0) {
    dbExecute(con, paste("INSERT INTO right_populations (right_combination) VALUES ('", right_str, "')", sep = ""))
    result <- dbGetQuery(con, "SELECT last_insert_rowid() AS id")
  }
  
  return(result$id)
}

# Function to convert existing results
convert_existing_results <- function(existing_result, model_id, right_id) {
  weights_data <- existing_result %>%
    select(population, weight, se, target) %>%
    mutate(target = paste0(target, "_mdl", sprintf("%03d", model_id), "_Rght", sprintf("%03d", right_id)))
  
  result_list <- list(
    weights = weights_data,
    rankdrop = data.frame(),  # This can be populated if required
    popdrop = data.frame(
      pat = sapply(1:nrow(weights_data), function(x) paste0(rep("0", x), collapse = "")),
      p = existing_result$p_value[1]
    )
  )
  
  list(
    result = result_list,
    weights_data = weights_data,
    p_value = existing_result$p_value[1],
    avg_se = existing_result$avg_se[1],
    right_combination = strsplit(existing_result$right_combination[1], ", ")[[1]],
    original_target = existing_result$target[1],
    left_combination = strsplit(existing_result$left_combination[1], ", ")[[1]]
  )
}

# Function to process a single combination and save to the database
process_combination <- function(prefix_ho, left, right, target, model_id, right_id) {
  con <- dbConnect(SQLite(), dbname = db_path)
  dbExecute(con, "PRAGMA journal_mode = WAL;")
  # Check if the request already exists
  existing_request <- check_existing_request(con, left, right, target)
  if (nrow(existing_request) > 0) {
    existing_result <- fetch_existing_result(con, left, right, target)
    if (nrow(existing_result) > 0) {
      dbDisconnect(con)
      return(convert_existing_results(existing_result, model_id, right_id))
    }
  }
  
  log_message2(prefix_ho, left, right, target)
  res <- qpadm(prefix_ho, left, right, target, allsnps = TRUE)
  result <- res
  if (is.null(result)) {
    log_rejection(prefix_ho, left, right, target, "qpadm returned NULL")
    dbDisconnect(con)
    return(NULL)
  }
  
  weights_data <- result$weights %>%
    select(left, weight, se) %>%
    rename(population = left) %>%
    mutate(target = paste0(target, "_mdl", sprintf("%03d", model_id), "_Rght", sprintf("%03d", right_id)))
  
  num_left <- length(left)
  current_p_value <- result$popdrop %>%
    filter(pat %in% sapply(1:num_left, function(x) paste0(rep("0", x), collapse = ""))) %>%
    pull(p)
  
  current_avg_se <- mean(weights_data$se)
  
  # Save result to the database
  left_id <- insert_and_get_left_id(con, left)
  right_id <- insert_and_get_right_id(con, right)
  
  dbExecute(con, paste("INSERT INTO request (left_id, right_id, target) VALUES (", left_id, ",", right_id, ",'", target, "')", sep = ""))
  request_id <- dbGetQuery(con, "SELECT last_insert_rowid() AS id")$id[1]
  
  for (i in 1:nrow(weights_data)) {
    row <- weights_data[i, ]
    dbExecute(con, paste("INSERT INTO result (request_id, population, weight, se, target, p_value, avg_se, right_combination, left_combination) VALUES (", 
                         request_id, ",'", row$population, "',", row$weight, ",", row$se, ",'", target, "',", current_p_value, ",", current_avg_se, ",'", 
                         paste(right, collapse = ", "), "', '", paste(left, collapse = ", "), "')", sep = ""))
  }
  dbDisconnect(con)
  
  list(
    result = result,
    weights_data = weights_data,
    p_value = current_p_value,
    avg_se = current_avg_se,
    right_combination = right,  # Include right combination in the result for logging
    original_target = target,  # Include the original target
    left_combination = left  # Include the left combination
  )
}

# Function to prepare all combinations for all targets
prepare_combinations <- function(targets, left_static, left_dynamic, right_static, right_dynamic) {
  left_combinations <- unlist(lapply(0:length(left_dynamic), function(n) combn(left_dynamic, n, simplify = FALSE)), recursive = FALSE)
  right_combinations <- unlist(lapply(0:length(right_dynamic), function(n) combn(right_dynamic, n, simplify = FALSE)), recursive = FALSE)
  
  combinations <- expand.grid(target = targets, left_combination = left_combinations, right_combination = right_combinations, stringsAsFactors = FALSE)
  combinations <- combinations %>%
    mutate(left_combination = lapply(left_combination, function(x) c(left_static, x)),
           right_combination = lapply(right_combination, function(x) c(right_static, x)))
  
  # Remove combinations where left_combination or right_combination is empty
  combinations <- combinations %>%
    filter(lengths(left_combination) > 0 & lengths(right_combination) > 0)
  
  combinations
}

# Function to calculate the number of combinations
calculate_combinations_count <- function(dynamic_population) {
  sum(sapply(0:length(dynamic_population), function(k) choose(length(dynamic_population), k)))
}

# Create SQLite database and tables if they don't exist
con <- dbConnect(SQLite(), dbname = db_path)
dbExecute(con, "PRAGMA journal_mode = WAL;")
dbExecute(con, "CREATE TABLE IF NOT EXISTS left_populations (id INTEGER PRIMARY KEY, left_combination TEXT)")
dbExecute(con, "CREATE TABLE IF NOT EXISTS right_populations (id INTEGER PRIMARY KEY, right_combination TEXT)")
dbExecute(con, "CREATE TABLE IF NOT EXISTS request (id INTEGER PRIMARY KEY, left_id INTEGER, right_id INTEGER, target TEXT, FOREIGN KEY(left_id) REFERENCES left_populations(id), FOREIGN KEY(right_id) REFERENCES right_populations(id))")
dbExecute(con, "CREATE TABLE IF NOT EXISTS result (request_id INTEGER, population TEXT, weight REAL, se REAL, target TEXT, p_value REAL, avg_se REAL, right_combination TEXT, left_combination TEXT, FOREIGN KEY(request_id) REFERENCES request(id))")
dbDisconnect(con)

# Record start time
start_time <- Sys.time()
log_message(paste("Start time:", start_time))

# Setup parallel backend
plan(multisession, workers = parallel::detectCores() - 1)

# Calculate the number of combinations for one target
num_combinations_per_target <- calculate_combinations_count(left_dynamic) * calculate_combinations_count(right_dynamic)
log_message(paste("There will be", num_combinations_per_target, "combinations for one target."))

# Calculate the total number of combinations for all targets
total_combinations <- num_combinations_per_target * length(targets)
log_message(paste("There will be a total of", total_combinations, "combinations for all targets."))

# Prepare all combinations
combinations <- prepare_combinations(targets, left_static, left_dynamic, right_static, right_dynamic)

# Generate right combination IDs
right_combination_ids <- combinations %>%
  select(right_combination) %>%
  distinct() %>%
  mutate(right_id = row_number())

# Join right combination IDs back to combinations
combinations <- combinations %>%
  left_join(right_combination_ids, by = "right_combination") %>%
  mutate(model_id = row_number())

# Save all right combinations with IDs
right_combinations_df <- right_combination_ids %>%
  mutate(right_combination = sapply(right_combination, function(x) paste(x, collapse = ", "))) %>%
  rename(RightId = right_id, Right = right_combination)

# Check for existing results and fetch them from the database if they exist
con <- dbConnect(SQLite(), dbname = db_path)
dbExecute(con, "PRAGMA journal_mode = WAL;")
existing_results <- list()
new_combinations <- list()
for (i in 1:nrow(combinations)) {
  row <- combinations[i, ]
  target <- row$target
  left <- row$left_combination[[1]]
  right <- row$right_combination[[1]]
  existing_request <- check_existing_request(con, left, right, target)
  if (nrow(existing_request) > 0) {
    existing_result <- fetch_existing_result(con, left, right, target)
    if (nrow(existing_result) > 0) {
      existing_results <- append(existing_results, list(list(result = existing_result, model_id = row$model_id, right_id = row$right_id)))
    }
  } else {
    new_combinations <- append(new_combinations, list(row))
  }
}
dbDisconnect(con)

# Process new combinations in parallel
results <- future_map(new_combinations, function(row) {
  target <- row$target
  left <- row$left_combination[[1]]
  right <- row$right_combination[[1]]
  model_id <- row$model_id
  right_id <- row$right_id
  res <- process_combination(prefix_ho, left, right, target, model_id, right_id)
  if (is.null(res)) {
    log_message(paste("Combination for target", target, "with left", paste(left, collapse = ", "), "and right", paste(right, collapse = ", "), "returned NULL"))
  } else {
    log_message(paste("Combination for target", target, "with left", paste(left, collapse = ", "), "and right", paste(right, collapse = ", "), "returned a result"))
  }
  res
}, .options = furrr_options(seed = TRUE, scheduling = 1))

# Convert existing results to the same format as new results
existing_results_converted <- lapply(existing_results, function(x) convert_existing_results(x$result, x$model_id, x$right_id))

# Combine results and existing results
results <- c(results, existing_results_converted)

# Filter out NULL results
valid_results <- Filter(Negate(is.null), results)

# Separate valid and invalid results
valid_results_filtered <- list()
best_invalid_results <- list()

for (target in targets) {
  target_results <- Filter(function(res) grepl(target, res$weights_data$target[1]), valid_results)
  
  if (length(target_results) > 0) {
    valid_target_results <- Filter(function(res) {
      if (all(res$weights_data$weight >= 0) && res$p_value >= 0.05 && res$avg_se <= 0.05) {
        TRUE
      } else {
        log_rejection(prefix_ho, res$weights_data$population, res$right_combination, target, paste("Invalid model - P : ",res$p_value," SE : ",res$avg_se))
        FALSE
      }
    }, target_results)
    
    if (length(valid_target_results) > 0) {
      valid_results_filtered[[target]] <- valid_target_results
    } else {
      sorted_results <- target_results[order(sapply(target_results, function(res) -res$p_value),
                                             sapply(target_results, function(res) res$avg_se))]
      
      best_invalid_result <- Filter(function(res) all(res$weights_data$weight >= 0), sorted_results)
      if (length(best_invalid_result) > 0) {
        best_invalid_results[[target]] <- best_invalid_result[[1]]
      } else {
        best_invalid_results[[target]] <- sorted_results[[1]]
      }
    }
  } else {
    log_message(paste("No results found for target:", target))
  }
}

# Ensure each target has at least one result
final_results <- list()
for (target in targets) {
  if (!is.null(valid_results_filtered[[target]]) && length(valid_results_filtered[[target]]) > 0) {
    final_results <- append(final_results, valid_results_filtered[[target]])
  } else if (!is.null(best_invalid_results[[target]])) {
    final_results <- append(final_results, list(best_invalid_results[[target]]))
  }
}

# Combine results for each target
all_weights_data <- bind_rows(lapply(final_results, `[[`, "weights_data"))

# Flatten the p_value and avg_se dictionaries to single values
p_values_dict <- setNames(sapply(final_results, function(res) res$p_value), sapply(final_results, function(res) res$weights_data$target[1]))
avg_se_dict <- setNames(sapply(final_results, function(res) res$avg_se), sapply(final_results, function(res) res$weights_data$target[1]))

# Convert dictionaries to dataframes
p_values_df <- enframe(p_values_dict, name = "target", value = "p_value")
avg_se_df <- enframe(avg_se_dict, name = "target", value = "avg_se")

check_and_rename_file <- function(file_path) {
  if (file.exists(file_path)) {
    timestamp <- format(Sys.time(), "%d%m%y%H%M")
    file_ext <- tools::file_ext(file_path)
    file_name <- sub(paste0("\\.", file_ext, "$"), "", basename(file_path))
    new_file_path <- file.path(dirname(file_path), paste0(file_name, "_", timestamp, ".", file_ext))
    file.rename(file_path, new_file_path)
  }
}

# Check and rename existing files
check_and_rename_file(all_weights_data_path)
check_and_rename_file(p_values_path)
check_and_rename_file(avg_se_path)
check_and_rename_file(right_populations_path)

# Save the data to a file
write_csv(all_weights_data, all_weights_data_path)
write_csv(p_values_df, p_values_path)
write_csv(avg_se_df, avg_se_path)
write_csv(right_combinations_df, right_populations_path)

# Save the rejection reasons to a CSV file
if (length(rejection_reasons) > 0) {
  rejection_df <- bind_rows(rejection_reasons)
  write_csv(rejection_df, rejection_log_path)
}

# Verify saved data
all_weights_data_check <- read_csv(all_weights_data_path)
p_values_check <- read_csv(p_values_path)
avg_se_check <- read_csv(avg_se_path)
right_populations_check <- read_csv(right_populations_path)

# Debugging: Print to verify saved data
log_message("all_weights_data_check:")
log_message(capture.output(print(all_weights_data_check)))
log_message("p_values_check:")
log_message(capture.output(print(p_values_check)))
log_message("avg_se_check:")
log_message(capture.output(print(avg_se_check)))
log_message("right_populations_check:")
log_message(capture.output(print(right_populations_check)))

# Record end time
end_time <- Sys.time()
log_message(paste("End time:", end_time, "\n"))
script_duration_seconds <- as.numeric(difftime(end_time, start_time, units = "secs"))
script_duration_formatted <- sprintf("%02d:%02d:%02d", as.integer(script_duration_seconds %/% 3600), as.integer((script_duration_seconds %% 3600) %/% 60), as.integer(script_duration_seconds %% 60))
log_message(paste("Script duration:", script_duration_formatted))
