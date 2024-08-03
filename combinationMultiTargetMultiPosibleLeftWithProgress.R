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
rejection_log_path <- "c:/qpadmdata/rejection_log.csv"

# Input strings for right and left populations
input_string <- "Mbuti.DG,Israel_PPNB,Russia_MA1_HG.SG,Turkey_Boncuklu_N,Turkey_Epipaleolithic,Morocco_Iberomaurusian,Serbia_IronGates_Mesolithic,Luxembourg_Loschbour.DG,Russia_Karelia_HG,Georgia_Kotias.SG,Iran_GanjDareh_N,China_Tianyuan,Indian_GreatAndaman_100BP.SG,Mongolia_North_N,Russia_Tyumen_HG,Israel_Natufian,Jordan_PPNB"
right <- unlist(strsplit(input_string, split = ","))

# Define static and dynamic left populations
input_string_left_static <- ""
input_string_left_dynamic <- "Lebanon_MBA.SG,CanaryIslands_Guanche.SG,Italy_PianSultano_BA.SG,Tajikistan_Ksirov_Kushan,Syria_TellQarassa_Umayyad,France_Occitanie_EBA.SG,Syria_TellMasaikh_Medieval.SG"
left_static <- unlist(strsplit(input_string_left_static, split = ","))
left_dynamic <- unlist(strsplit(input_string_left_dynamic, split = ","))

# Check if both left_static and left_dynamic are empty
if (length(left_static) == 0 && length(left_dynamic) == 0) {
  stop("Both left_static and left_dynamic are empty. At least one must be non-empty.")
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

log_message2 <- function(prefix_ho, left, target) {
  main_thread <- if (is_main_thread()) "MainThread" else "WorkerThread"
  workers_count <- future::nbrOfWorkers()
  timestamp <- Sys.time()
  if (is.vector(left) && length(left) > 1) {
    left_str <- paste(left, collapse = ", ")
  } else {
    left_str <- left
  }
  message <- paste("][Time:", timestamp, "] process_combination", prefix_ho, left_str, target, sep = " ")
  print(paste(message, "\n"))
  write(message, file = ongoingCombinationProgress_log_path, append = TRUE, sep = "\n")
}

# Initialize list to store rejection reasons
rejection_reasons <- list()

log_rejection <- function(prefix_ho, left, target, reason) {
  main_thread <- if (is_main_thread()) "MainThread" else "WorkerThread"
  workers_count <- future::nbrOfWorkers()
  timestamp <- Sys.time()
  if (is.vector(left) && length(left) > 1) {
    left_str <- paste(left, collapse = ", ")
  } else {
    left_str <- left
  }
  message <- paste("][Time:", timestamp, "]", 
                   "Rejected combination:", prefix_ho, left_str, target, "Reason:", reason)
  print(paste(message, "\n"))
  write(message, file = rejection_log_path, append = TRUE, sep = "\n")
  
  # Store rejection reason in the list
  rejection_reasons <<- append(rejection_reasons, list(data.frame(prefix_ho = prefix_ho, left = left_str, target = target, reason = reason, stringsAsFactors = FALSE)))
}

# Function to process a single combination
process_combination <- function(prefix_ho, left, right, target, model_id) {
  log_message2(prefix_ho, left, target)
  res <- qpadm(prefix_ho, left, right, target, allsnps = TRUE)
  result <- res
  if (is.null(result)) {
    log_rejection(prefix_ho, left, target, "qpadm returned NULL")
    return(NULL)
  }
  weights_data <- result$weights %>%
    select(left, weight, se) %>%
    rename(population = left) %>%
    mutate(target = paste0(target, ifelse(model_id == 1, "", paste0("_model", model_id))))
  
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
}

# Function to prepare all combinations for all targets
prepare_combinations <- function(targets, left_static, left_dynamic) {
  dynamic_combinations <- expand.grid(target = targets, left_combination = unlist(lapply(0:length(left_dynamic), function(n) combn(left_dynamic, n, simplify = FALSE)), recursive = FALSE), stringsAsFactors = FALSE)
  static_dynamic_combinations <- dynamic_combinations %>%
    mutate(left_combination = lapply(left_combination, function(x) c(left_static, x)))
  
  # Remove combinations where left_combination is empty (i.e., both left_static and left_dynamic are empty)
  static_dynamic_combinations <- static_dynamic_combinations %>%
    filter(lengths(left_combination) > 0)
  
  static_dynamic_combinations
}

# Function to calculate the number of combinations
calculate_combinations_count <- function(left_dynamic) {
  sum(sapply(0:length(left_dynamic), function(k) choose(length(left_dynamic), k)))
}

# Record start time
start_time <- Sys.time()
log_message(paste("Start time:", start_time))

# Setup parallel backend
plan(multisession, workers = parallel::detectCores() - 1)

# Calculate the number of combinations for one target
num_combinations_per_target <- calculate_combinations_count(left_dynamic)
log_message(paste("There will be", num_combinations_per_target, "combinations for one target."))

# Calculate the total number of combinations for all targets
total_combinations <- num_combinations_per_target * length(targets)
log_message(paste("There will be a total of", total_combinations, "combinations for all targets."))

# Prepare all combinations
combinations <- prepare_combinations(targets, left_static, left_dynamic)

# Add a model ID to each combination
combinations <- combinations %>%
  mutate(model_id = row_number())

# Process all combinations in parallel
results <- future_map(1:nrow(combinations), function(i) {
  row <- combinations[i, ]
  target <- row$target
  left <- row$left_combination[[1]]
  model_id <- row$model_id
  res <- process_combination(prefix_ho, left, right, target, model_id)
  if (is.null(res)) {
    log_message(paste("Combination for target", target, "with left", paste(left, collapse = ", "), "returned NULL"))
  } else {
    log_message(paste("Combination for target", target, "with left", paste(left, collapse = ", "), "returned a result"))
  }
  res
}, .options = furrr_options(seed = TRUE, scheduling = 1))

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
        log_rejection(prefix_ho, res$weights_data$population, target, paste("Invalid model - P : ",res$p_value," SE : ",res$avg_se))
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
write_csv(data.frame(right = right), right_populations_path)

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
