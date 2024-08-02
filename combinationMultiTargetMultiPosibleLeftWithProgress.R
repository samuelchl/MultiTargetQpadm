# Install cowplot package if not already installed
# install.packages("cowplot")

library("admixtools")
library("tidyverse")
library("ggplot2")
library("dplyr")
library("grid")
library("cowplot")
library("future")
library("furrr")

# Define the dataset prefix
prefix_ho <- "c:/datasets/v54.1.p1_HO_public"

# Input strings for right and left populations
input_string <- "Mbuti.DG,Ami.DG,Basque.DG,Biaka.DG,Bougainville.DG,Chukchi.DG,Eskimo_Naukan.DG,Han.DG,Iran_GanjDareh_N,Ju_hoan_North.DG,Karitiana.DG,Papuan.DG,Sardinian.DG,She.DG,Ulchi.DG,Yoruba.DG"
input_string <- "Cameroon_SMA.DG,Russia_MA1_HG.SG,Morocco_Iberomaurusian,Turkey_Epipaleolithic,Turkey_Boncuklu_N,Georgia_Satsurblia.SG,Iran_TepeAbdulHosein_N.SG,Russia_Karelia_HG,Luxembourg_Loschbour,Jordan_PPNB,Mongolia_North_N,Turkey_Alalakh_MLBA,Russia_Samara_EBA_Yamnaya,Turkey_EBA_II.SG,Greece_BA_Mycenaean"
#input_string <- "Mbuti.DG,Israel_PPNB,Russia_MA1_HG.SG,Turkey_Boncuklu_N,Turkey_Epipaleolithic,Morocco_Iberomaurusian,Serbia_IronGates_Mesolithic,Luxembourg_Loschbour.DG,Russia_Karelia_HG,Georgia_Kotias.SG,Iran_GanjDareh_N,China_Tianyuan,Indian_GreatAndaman_100BP.SG,Mongolia_North_N,Russia_Tyumen_HG,Israel_Natufian,Jordan_PPNB"

right <- unlist(strsplit(input_string, split = ","))

input_string_left <- "Lebanon_MBA.SG,CanaryIslands_Guanche.SG,Italy_PianSultano_BA.SG,Tajikistan_Ksirov_Kushan,Syria_TellQarassa_Umayyad,France_Occitanie_EBA.SG,Syria_TellMasaikh_Medieval.SG"
left_initial <- unlist(strsplit(input_string_left, split = ","))

# List of target populations
targets <- c("Jew_Ashkenazi.HO", "Jew_Moroccan.HO", "Jew_Tunisian.HO", "Jew_Libyan.HO", "Jew_Yemenite.HO", "Jew_Iranian.HO", "Jew_Iraqi.HO", "Jew_Georgian.HO", "Jew_Turkish.HO","Lebanese.HO","Cypriot.HO","Palestinian.HO","Syrian.HO","Italian_South.HO")

# Function to run qpadm for all combinations and return the valid results or the best found
run_qpadm_all_combinations <- function(prefix_ho, left_initial, right, target) {
  combinations <- unlist(lapply(1:length(left_initial), function(n) combn(left_initial, n, simplify = FALSE)), recursive = FALSE)
  valid_results <- list()
  best_result <- NULL
  best_p_value <- -Inf
  best_avg_se <- Inf
  
  cat("Processing target:", target, "\n")
  
  for (left in combinations) {
    result <- qpadm(prefix_ho, left, right, target, allsnps = TRUE)
    weights_data <- result$weights %>%
      select(left, weight, se) %>%
      rename(population = left) %>%
      mutate(target = target)
    
    if (all(weights_data$weight >= 0)) {
      num_left <- length(left)
      current_p_value <- result$popdrop %>%
        filter(pat %in% sapply(1:num_left, function(x) paste0(rep("0", x), collapse = ""))) %>%
        pull(p)
      
      current_avg_se <- mean(weights_data$se)
      
      if (current_p_value > 0.05 && current_avg_se < 0.05) {
        valid_results <- append(valid_results, list(list(
          result = result,
          weights_data = weights_data,
          p_value = current_p_value,
          avg_se = current_avg_se
        )))
      }
      
      if (current_p_value > best_p_value || (current_p_value == best_p_value && current_avg_se < best_avg_se)) {
        best_result <- list(
          result = result,
          weights_data = weights_data,
          p_value = current_p_value,
          avg_se = current_avg_se
        )
        best_p_value <- current_p_value
        best_avg_se <- current_avg_se
      }
    }
  }
  
  if (length(valid_results) == 0) {
    valid_results <- list(best_result)
  }
  
  cat("Completed processing target:", target, "\n")
  
  valid_results
}

# Record start time
start_time <- Sys.time()
cat("Start time:", start_time, "\n")

# Setup parallel backend
plan(multisession, workers = parallel::detectCores() - 1)

# Generate results for all targets in parallel
results_list <- future_map(targets, function(target) {
  cat("Starting target:", target, "\n")
  qpadm_results <- run_qpadm_all_combinations(prefix_ho, left_initial, right, target)
  lapply(seq_along(qpadm_results), function(i) {
    list(
      weights_data = qpadm_results[[i]]$weights_data %>% mutate(target = paste0(target, ifelse(i == 1, "", paste0("_model", i)))),
      p_value = qpadm_results[[i]]$p_value,
      avg_se = qpadm_results[[i]]$avg_se,
      target = paste0(target, ifelse(i == 1, "", paste0("_model", i)))
    )
  })
}, .options = furrr_options(seed = TRUE))

# Combine all results into a single dataframe
all_weights_data <- bind_rows(lapply(unlist(results_list, recursive = FALSE), `[[`, "weights_data"))

# Flatten the p_value and avg_se dictionaries to single values
p_values_dict <- setNames(sapply(unlist(results_list, recursive = FALSE), function(res) res$p_value), unlist(lapply(unlist(results_list, recursive = FALSE), `[[`, "target")))
avg_se_dict <- setNames(sapply(unlist(results_list, recursive = FALSE), function(res) res$avg_se), unlist(lapply(unlist(results_list, recursive = FALSE), `[[`, "target")))

# Debugging: Print dictionaries to ensure all targets are included
print("p_values_dict:")
print(p_values_dict)
print("avg_se_dict:")
print(avg_se_dict)

# Convert dictionaries to dataframes
p_values_df <- enframe(p_values_dict, name = "target", value = "p_value")
avg_se_df <- enframe(avg_se_dict, name = "target", value = "avg_se")

# Debugging: Print dataframes to ensure they are correct before saving
print("p_values_df:")
print(p_values_df)
print("avg_se_df:")
print(avg_se_df)

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
check_and_rename_file("c:/qpadmdata/all_weights_data.csv")
check_and_rename_file("c:/qpadmdata/p_values.csv")
check_and_rename_file("c:/qpadmdata/avg_se.csv")
check_and_rename_file("c:/qpadmdata/right_populations.csv")

# Save the data to a file
write_csv(all_weights_data, "c:/qpadmdata/all_weights_data.csv")
write_csv(p_values_df, "c:/qpadmdata/p_values.csv")
write_csv(avg_se_df, "c:/qpadmdata/avg_se.csv")
write_csv(data.frame(right = right), "c:/qpadmdata/right_populations.csv")

# Verify saved data
all_weights_data_check <- read_csv("c:/qpadmdata/all_weights_data.csv")
p_values_check <- read_csv("c:/qpadmdata/p_values.csv")
avg_se_check <- read_csv("c:/qpadmdata/avg_se.csv")
right_populations_check <- read_csv("c:/qpadmdata/right_populations.csv")

# Debugging: Print to verify saved data
print("all_weights_data_check:")
print(all_weights_data_check)
print("p_values_check:")
print(p_values_check)
print("avg_se_check:")
print(avg_se_check)
print("right_populations_check:")
print(right_populations_check)

# Record end time
end_time <- Sys.time()
cat("End time:", end_time, "\n")
