# Install cowplot package if not already installed
# install.packages("cowplot")

library("admixtools")
library("tidyverse")
library("ggplot2")
library("dplyr")
library("grid")  # Load the grid package for textGrob
library("cowplot")  # Load cowplot package for combining plots

# Define the dataset prefix
prefix_ho = "c:/datasets/v54.1.p1_HO_public"

# Input strings for right and left populations
input_string <- "Mbuti.DG,Ami.DG,Basque.DG,Biaka.DG,Bougainville.DG,Chukchi.DG,Eskimo_Naukan.DG,Han.DG,Iran_GanjDareh_N,Ju_hoan_North.DG,Karitiana.DG,Papuan.DG,Sardinian.DG,She.DG,Ulchi.DG,Yoruba.DG"
input_string <- "Cameroon_SMA.DG,Russia_MA1_HG.SG,Morocco_Iberomaurusian,Turkey_Epipaleolithic,Turkey_Boncuklu_N,Georgia_Satsurblia.SG,Iran_TepeAbdulHosein_N.SG,Russia_Karelia_HG,Luxembourg_Loschbour,Jordan_PPNB,Mongolia_North_N,Turkey_Alalakh_MLBA,Russia_Samara_EBA_Yamnaya,Turkey_EBA_II.SG,Greece_BA_Mycenaean"
right <- unlist(strsplit(input_string, split = ","))

input_string_left <- "Lebanon_MBA.SG,CanaryIslands_Guanche.SG,Italy_PianSultano_BA.SG,Tajikistan_Ksirov_Kushan"
left_initial <- unlist(strsplit(input_string_left, split = ","))

# List of target populations
targets <- c("Jew_Ashkenazi.HO", "Jew_Moroccan.HO","Jew_Turkish.HO", "Jew_Tunisian.HO", "Jew_Libyan.HO","Jew_Yemenite.HO","Jew_Iranian.HO", "Jew_Iraqi.HO","Jew_Georgian.HO","Jew_Cochin.HO")

# Function to run qpadm and handle negative weights
run_qpadm_until_positive_weights <- function(prefix_ho, left, right, target) {
  while (TRUE) {
    result <- qpadm(prefix_ho, left, right, target, allsnps = TRUE)
    weights_data <- result$weights %>%
      select(left, weight, se) %>%
      rename(population = left)
    
    if (all(weights_data$weight >= 0)) {
      break
    }
    
    # Remove populations with negative weights
    left <- left[!left %in% weights_data$population[weights_data$weight < 0]]
  }
  
  list(result = result, weights_data = weights_data)
}

# Function to generate plot for a target population
generate_plot_for_target <- function(target, prefix_ho, left_initial, right) {
  left <- left_initial  # Reset the left populations for each target
  qpadm_result <- run_qpadm_until_positive_weights(prefix_ho, left, right, target)
  result <- qpadm_result$result
  weights_data <- qpadm_result$weights_data
  
  # Extracting the p-value for rank 000 from result$popdrop
  p_value_rank_000 <- result$popdrop %>%
    filter(pat == "0000" | pat == "000"| pat == "00"| pat == "0") %>%
    pull(p)
  
  # Calculate the average SE
  average_se <- mean(weights_data$se)
  
  # Create a single horizontal bar chart from 0 to 100%
  p <- ggplot(weights_data, aes(x = weight, y = target, fill = population)) +
    geom_bar(stat = "identity") +  # Use a small height to make the bar thin
    geom_text(aes(label = paste0(scales::percent(weight), "\n(SE: ", round(se, 4), ")")), 
              position = position_fill(vjust = 0.5), size = 3, color = "black") +
    labs(title = paste("Population Weights for Target:", target, 
                       "\nP-value:", format(p_value_rank_000, scientific = TRUE), 
                       "\nAverage SE:", round(average_se, 4)), 
         x = "Proportion", y = NULL) +
    theme_classic() +
    theme(legend.position = "bottom",
          axis.text.y = element_blank(), 
          axis.ticks.y = element_blank())
  
  return(p)
}

# Generate plots for all targets
plots <- lapply(targets, generate_plot_for_target, prefix_ho = prefix_ho, left_initial = left_initial, right = right)

# Generate the right label text
right_label_text <- paste("Right Populations:\n", paste(strwrap(paste(right, collapse = ", "), width = 60), collapse = "\n"))

# Create a text plot for the right label text with white background
text_plot <- ggplot() + 
  annotate("text", x = 0.5, y = 0.5, label = right_label_text, size = 4, hjust = 0.5, vjust = 0.5) +
  theme_void() +
  theme(plot.background = element_rect(fill = "white", color = NA))

# Combine all the plots and the text plot into a grid with n+1 rows (n = number of targets)
combined_plot <- plot_grid(plotlist = c(plots, list(text_plot)), ncol = 1, rel_heights = c(rep(1, length(targets)), 1))

# Generate a timestamp
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

# Save the combined plot as an image file with timestamp
ggsave(paste0("population_weights_bar_chart_", timestamp, ".png"), plot = combined_plot, width = 10, height = 6 + 3 * length(targets), dpi = 300)
