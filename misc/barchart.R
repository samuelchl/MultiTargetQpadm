library("admixtools")
library("tidyverse")
library("ggplot2")
library("dplyr")
library("grid")  # Load the grid package for textGrob

# Define the dataset prefix
prefix_ho = "c:/datasets/v54.1.p1_HO_public"

# Input strings for right and left populations
input_string <- "Mbuti.DG,Ami.DG,Basque.DG,Biaka.DG,Bougainville.DG,Chukchi.DG,Eskimo_Naukan.DG,Han.DG,Iran_GanjDareh_N,Ju_hoan_North.DG,Karitiana.DG,Papuan.DG,Sardinian.DG,She.DG,Ulchi.DG,Yoruba.DG"
input_string <- "Cameroon_SMA.DG,Russia_MA1_HG.SG,Morocco_Iberomaurusian,Turkey_Epipaleolithic,Turkey_Boncuklu_N,Georgia_Satsurblia.SG,Iran_TepeAbdulHosein_N.SG,Russia_Karelia_HG,Luxembourg_Loschbour,Jordan_PPNB,Mongolia_North_N,Turkey_Alalakh_MLBA,Russia_Samara_EBA_Yamnaya,Turkey_EBA_II.SG,Greece_BA_Mycenaean"
right <- unlist(strsplit(input_string, split = ","))

input_string_left <- "Lebanon_MBA.SG,CanaryIslands_Guanche.SG,Italy_PianSultano_BA.SG,Tajikistan_Ksirov_Kushan"
#input_string_left <- "Lebanon_Chhim_Phoenician.SG,CanaryIslands_Guanche.SG,Italy_Sardinia_SantImbenia_RomanImperial.SG,Tajikistan_Ksirov_Kushan"
left <- unlist(strsplit(input_string_left, split = ","))

#Italy_LA.SG France_BA_GalloRoman Lebanon_Chhim_Phoenician.SG,Italy_LA.SG

# Define the target population
target = "Jew_Ashkenazi.HO"
target = "Jew_Moroccan.HO"

# Run the qpadm analysis
result = qpadm(prefix_ho, left, right, target, allsnps = TRUE)

# Output the results
result$weights
result$popdrop

# Extracting the relevant data from the result object
weights_data <- result$weights %>%
  select(left, weight, se) %>%
  rename(population = left)

# Extracting the p-value for rank 000 from result$popdrop
p_value_rank_000 <- result$popdrop %>%
  filter(pat == "0000" | pat == "000") %>%
  pull(p)

# Calculate the average SE
average_se <- mean(weights_data$se)

# Add SE to the population label
weights_data <- weights_data %>%
  mutate(population_label = paste0(population, "\nSE: ", round(se, 4)))

# Generate a timestamp
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

# Create a horizontal bar chart
p <- ggplot(weights_data, aes(x = weight, y = reorder(population_label, weight), fill = population)) +
  geom_bar(stat = "identity", color = "white", width = 0.7) +  # Adjust bar height
  geom_text(aes(label = scales::percent(weight / sum(weight)), x = weight + 0.02), 
            position = position_dodge(width = 0.9), vjust = 0.5, hjust = 0) +
  labs(title = paste("Population Weights for Target:", target, "\nP-value:", format(p_value_rank_000, scientific = TRUE), 
                     "\nAverage SE:", round(average_se, 4)), 
       x = "Weight", y = "Population") +
  theme_classic() +
  theme(legend.position = "none")

# Generate the right label text
right_label_text <- paste("Right Populations:\n", paste(right, collapse = ", "))

# Plot with the right label as a textbox below the chart
p <- p + annotation_custom(
  grob = textGrob(right_label_text, x = unit(0.5, "npc"), y = unit(-0.3, "npc"), hjust = 0.5, vjust = 1, gp = gpar(fontsize = 10)),
  xmin = -Inf, xmax = Inf, ymin = -Inf, ymax = Inf
)

# Save the plot as an image file with timestamp
ggsave(paste0("population_weights_bar_chart_", timestamp, ".png"), plot = p, width = 8, height = 4, dpi = 300)
