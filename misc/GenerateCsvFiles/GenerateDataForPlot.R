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
#input_string <- "Cameroon_SMA.DG,Russia_MA1_HG.SG,Morocco_Iberomaurusian,Turkey_Epipaleolithic,Turkey_Boncuklu_N,Georgia_Satsurblia.SG,Iran_TepeAbdulHosein_N.SG,Russia_Karelia_HG,Luxembourg_Loschbour,Jordan_PPNB,Mongolia_North_N,Turkey_Alalakh_MLBA,Russia_Samara_EBA_Yamnaya,Turkey_EBA_II.SG,Greece_BA_Mycenaean"
right <- unlist(strsplit(input_string, split = ","))

#write_csv(data.frame(right = right), "c:/qpadmdata/right_populations.csv")

input_string_left <- "Lebanon_MBA.SG,CanaryIslands_Guanche.SG,Italy_PianSultano_BA.SG,Tajikistan_Ksirov_Kushan,Syria_TellQarassa_Umayyad"
left_initial <- unlist(strsplit(input_string_left, split = ","))

# List of target populations
targets <- c("Jew_Ashkenazi.HO", "Jew_Moroccan.HO", "Jew_Tunisian.HO", "Jew_Libyan.HO", "Jew_Yemenite.HO", "Jew_Iranian.HO", "Jew_Iraqi.HO", "Jew_Georgian.HO", "Jew_Turkish.HO","Lebanese.HO","Cypriot.HO","Palestinian.HO","Syrian.HO","Italian_South.HO")

# Function to run qpadm and handle negative weights iteratively
run_qpadm_until_positive_weights <- function(prefix_ho, left, right, target) {
  best_result <- NULL
  best_weights_data <- NULL
  best_p_value <- -Inf
  best_avg_se <- NULL
  
  while (TRUE) {
    result <- qpadm(prefix_ho, left, right, target, allsnps = TRUE)
    weights_data <- result$weights %>%
      select(left, weight, se) %>%
      rename(population = left) %>%
      mutate(target = target)  # Add target to weights_data
    
    negative_populations <- weights_data$population[weights_data$weight < 0]
    
    if (length(negative_populations) == 0) {
      current_p_value <- result$popdrop %>%
        filter(pat %in% c("000000000", "00000000", "0000000", "000000", "00000", "0000", "000", "00", "0")) %>%
        pull(p)
      
      current_avg_se <- mean(weights_data$se)
      
      if (current_p_value > best_p_value) {
        best_result <- result
        best_weights_data <- weights_data
        best_p_value <- current_p_value
        best_avg_se <- current_avg_se
      }
      
      break
    } else {
      for (pop in negative_populations) {
        current_left <- left[!left %in% pop]
        current_result <- qpadm(prefix_ho, current_left, right, target, allsnps = TRUE)
        current_weights_data <- current_result$weights %>%
          select(left, weight, se) %>%
          rename(population = left) %>%
          mutate(target = target)  # Add target to weights_data
        
        current_p_value <- current_result$popdrop %>%
          filter(pat %in% c("000000000", "00000000", "0000000", "000000", "00000", "0000", "000", "00", "0")) %>%
          pull(p)
        
        current_avg_se <- mean(current_weights_data$se)
        
        if (all(current_weights_data$weight >= 0) && current_p_value > best_p_value) {
          best_result <- current_result
          best_weights_data <- current_weights_data
          best_p_value <- current_p_value
          best_avg_se <- current_avg_se
        }
      }
      
      # Remove one negative population
      left <- left[!left %in% negative_populations[1]]
    }
  }
  
  list(result = best_result, weights_data = best_weights_data, p_value = current_p_value, avg_se = current_avg_se)
}

# Setup parallel backend
plan(multisession, workers = parallel::detectCores() - 1)

# Generate results for all targets in parallel
results_list <- future_map(targets, function(target) {
  left <- left_initial  # Reset the left populations for each target
  qpadm_result <- run_qpadm_until_positive_weights(prefix_ho, left, right, target)
  list(
    weights_data = qpadm_result$weights_data,
    p_value = qpadm_result$p_value,
    avg_se = qpadm_result$avg_se,
    target = target
  )
}, .options = furrr_options(seed = TRUE))

# Combine all results into a single dataframe
all_weights_data <- bind_rows(lapply(results_list, `[[`, "weights_data"))

# Flatten the p_value and avg_se dictionaries to single values
p_values_dict <- setNames(sapply(results_list, function(res) res$p_value), targets)
avg_se_dict <- setNames(sapply(results_list, function(res) res$avg_se), targets)

# Debugging: Print dictionaries to ensure all targets are included
print("p_values_dict:")
print(p_values_dict)
print("avg_se_dict:")
print(avg_se_dict)

# Verify that all targets have corresponding p_values and avg_se
for (target in targets) {
  if (is.null(p_values_dict[[target]])) stop(paste("Missing p_value for target:", target))
  if (is.null(avg_se_dict[[target]])) stop(paste("Missing avg_se for target:", target))
}

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
