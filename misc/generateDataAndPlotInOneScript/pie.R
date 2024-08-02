library("admixtools")
library("tidyverse")
library("ggplot2")
library("dplyr")


prefix_ho = "c:/datasets/v54.1.p1_HO_public"

input_string <- "Cameroon_SMA.DG,Russia_MA1_HG.SG,Morocco_Iberomaurusian,Turkey_Epipaleolithic,Turkey_Boncuklu_N,Georgia_Satsurblia.SG,Iran_TepeAbdulHosein_N.SG,Russia_Karelia_HG,Luxembourg_Loschbour,Jordan_PPNB,Mongolia_North_N,Turkey_Alalakh_MLBA,Russia_Samara_EBA_Yamnaya,Turkey_EBA_II.SG,Greece_BA_Mycenaean"

# Split the string by comma and convert it to a vector
right <- unlist(strsplit(input_string, split = ","))

input_string_left <- "Lebanon_Chhim_Phoenician.SG,CanaryIslands_Guanche.SG,France_BA_GalloRoman"

left <- unlist(strsplit(input_string_left, split = ","))

# Define the target population
target = "Jew_Ashkenazi.HO"

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
  filter(pat == "000") %>%
  pull(p)

weights_data <- weights_data %>%
  mutate(p = p_value_rank_000)

# Compute percentage
weights_data <- weights_data %>%
  mutate(percentage = weight / sum(weight) * 100)

# Pie chart with improved text placement
ggplot(weights_data, aes(x = "", y = weight, fill = population)) +
  geom_bar(width = 1, stat = "identity", color = "white") +
  coord_polar("y", start = 0) +
  labs(title = "Population Weights", x = NULL, y = NULL) +
  theme_void() +
  geom_text(aes(label = scales::percent(percentage/100), y = cumsum(weight) - weight / 2),
            color = "white", size = 4) +
  geom_text(aes(label = paste("SE:", round(se, 4)), y = cumsum(weight) - weight / 2 - 0.1),
            color = "white", size = 3) +
  geom_text(aes(label = paste("p:", format(p, scientific = TRUE)), y = cumsum(weight) - weight / 2 - 0.2),
            color = "white", size = 3)

# Save the plot as an image file
ggsave("population_weights_pie_chart.png", width = 8, height = 6, dpi = 300)
