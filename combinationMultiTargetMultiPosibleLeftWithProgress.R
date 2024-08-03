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

# Define paths and configurations
prefix_ho <- "c:/datasets/v54.1.p1_HO_public"
progress_log_path <- "c:/qpadmdata/progress.txt"
ongoingCombinationProgress_log_path <- "c:/qpadmdata/ongoing_combination_progress.txt"
all_weights_data_path <- "c:/qpadmdata/all_weights_data.csv"
p_values_path <- "c:/qpadmdata/p_values.csv"
avg_se_path <- "c:/qpadmdata/avg_se.csv"
right_populations_path <- "c:/qpadmdata/right_populations.csv"

# Input strings for right and left populations
input_string <- "Cameroon_SMA.DG,Russia_MA1_HG.SG,Morocco_Iberomaurusian,Turkey_Epipaleolithic,Turkey_Boncuklu_N"
right <- unlist(strsplit(input_string, split = ","))

input_string_left <- "Lebanon_MBA.SG,CanaryIslands_Guanche.SG,Italy_PianSultano_BA.SG"
left_initial <- unlist(strsplit(input_string_left, split = ","))

# List of target populations
targets <- c("Jew_Ashkenazi.HO","Jew_Moroccan.HO")

# Function to clear a log file
clear_log_file <- function(file_path) {
  write("", file = file_path)
}

# Clear log files
clear_log_file(progress_log_path)
clear_log_file(ongoingCombinationProgress_log_path)

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
  message <- paste("[Thread:", main_thread, "][Workers:", workers_count, "][Time:", timestamp, "]", as.character(message))
  print(paste(message, "\n"))
  write(message, file = progress_log_path, append = TRUE, sep = "\n")
}

log_message2 <- function(prefix_ho, left, target) {
  main_thread <- if (is_main_thread()) "MainThread" else "WorkerThread"
  workers_count <- future::nbrOfWorkers()
  timestamp <- Sys.time()
  if (is.vector(left) && length(left) > 1) {
    left_str <- paste(left, collapse = ", ")
  } else {
    left_str <- left
  }
  message <- paste("[Thread:", main_thread, "][Workers:", workers_count, "][Time:", timestamp, "] process_combination", prefix_ho, left_str, target, sep = " ")
  print(paste(message, "\n"))
  write(message, file = ongoingCombinationProgress_log_path, append = TRUE, sep = "\n")
}

# Function to process a single combination
process_combination <- function(prefix_ho, left, right, target, model_id) {
  log_message2(prefix_ho, left, target)
  res <- qpadm(prefix_ho, left, right, target, allsnps = TRUE)
  result <- res
  weights_data <- result$weights %>%
    select(left, weight, se) %>%
    rename(population = left) %>%
    mutate(target = paste0(target, ifelse(model_id == 1, "", paste0("_model", model_id))))
  
  if (all(weights_data$weight >= 0)) {
    num_left <- length(left)
    current_p_value <- result$popdrop %>%
      filter(pat %in% sapply(1:num_left, function(x) paste0(rep("0", x), collapse = ""))) %>%
      pull(p)
    
    current_avg_se <- mean(weights_data$se)
    
    list(
      result = result,
      weights_data = weights_data,
      p_value = current_p_value,
      avg_se = current_avg_se
    )
  } else {
    NULL
  }
}

# Function to prepare all combinations for all targets
prepare_combinations <- function(targets, left_initial) {
  combinations <- expand.grid(target = targets, left_combination = unlist(lapply(1:length(left_initial), function(n) combn(left_initial, n, simplify = FALSE)), recursive = FALSE), stringsAsFactors = FALSE)
  combinations
}

# Function to calculate the number of combinations
calculate_combinations_count <- function(left_initial) {
  sum(sapply(1:length(left_initial), function(k) choose(length(left_initial), k)))
}

# Record start time
start_time <- Sys.time()
log_message(paste("Start time:", start_time))

# Setup parallel backend
plan(multisession, workers = parallel::detectCores() - 1)

# Calculate the number of combinations for one target
num_combinations_per_target <- calculate_combinations_count(left_initial)
log_message(paste("There will be", num_combinations_per_target, "combinations for one target."))

# Calculate the total number of combinations for all targets
total_combinations <- num_combinations_per_target * length(targets)
log_message(paste("There will be a total of", total_combinations, "combinations for all targets."))

# Prepare all combinations
combinations <- prepare_combinations(targets, left_initial)

# Add a model ID to each combination
combinations <- combinations %>%
  mutate(model_id = row_number())

# Process all combinations in parallel
results <- future_map(1:nrow(combinations), function(i) {
  row <- combinations[i, ]
  target <- row$target
  left <- row$left_combination[[1]]
  model_id <- row$model_id
  process_combination(prefix_ho, left, right, target, model_id)
}, .options = furrr_options(seed = TRUE, scheduling = 1))

# Filter out NULL results
valid_results <- Filter(Negate(is.null), results)

# Combine results for each target
all_weights_data <- bind_rows(lapply(valid_results, `[[`, "weights_data"))

# Flatten the p_value and avg_se dictionaries to single values
p_values_dict <- setNames(sapply(valid_results, function(res) res$p_value), sapply(valid_results, function(res) res$weights_data$target[1]))
avg_se_dict <- setNames(sapply(valid_results, function(res) res$avg_se), sapply(valid_results, function(res) res$weights_data$target[1]))

# Debugging: Print dictionaries to ensure all targets are included
log_message("p_values_dict:")
log_message(paste(p_values_dict, collapse = "\n"))
log_message("avg_se_dict:")
log_message(paste(avg_se_dict, collapse = "\n"))

# Convert dictionaries to dataframes
p_values_df <- enframe(p_values_dict, name = "target", value = "p_value")
avg_se_df <- enframe(avg_se_dict, name = "target", value = "avg_se")

# Debugging: Print dataframes to ensure they are correct before saving
log_message("p_values_df:")
log_message(capture.output(print(p_values_df)))
log_message("avg_se_df:")
log_message(capture.output(print(avg_se_df)))

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
write_csv(data.frame(right = right), right_populations_path)

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
