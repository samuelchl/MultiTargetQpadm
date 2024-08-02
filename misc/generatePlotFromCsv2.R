# Load required libraries
library(ggplot2)
library(dplyr)
library(readr)
library(gridExtra)
library(grid)

# Load the data
all_weights_data <- read_csv('c:/qpadmdata/all_weights_data.csv')
p_values_df <- read_csv('c:/qpadmdata/p_values.csv')
avg_se_df <- read_csv('c:/qpadmdata/avg_se.csv')

right_populations_df <- read_csv("c:/qpadmdata/right_populations.csv")

# Convert the right populations to a single string
right <- paste(right_populations_df$right, collapse = ", ")

# Convert the p_values_df and avg_se_df to named vectors
p_values_dict <- setNames(p_values_df$p_value, p_values_df$target)
avg_se_dict <- setNames(avg_se_df$avg_se, avg_se_df$target)

# Ensure that the dictionaries are correctly formatted and only contain single values per target
print("p_values_dict:")
print(p_values_dict)
print("avg_se_dict:")
print(avg_se_dict)



# Custom labeller function to create facet labels
custom_labeller <- function(target) {
  p_value <- p_values_dict[target]
  avg_se <- avg_se_dict[target]
  
  # Ensure that p_value and avg_se are single values
  if (is.null(p_value) || is.null(avg_se)) {
    stop(paste("Multiple or no values found for target:", target))
  }
  
  paste0(target, "\nP-value: ", format(p_value, scientific = TRUE), "\nAverage SE: ", round(avg_se, 4))
}

# Add custom labels to the data
all_weights_data <- all_weights_data %>%
  mutate(label = sapply(target, custom_labeller))

# Create a single plot with horizontal stacked bars for each target
plot <- ggplot(all_weights_data, aes(y = label, x = weight, fill = population)) +
  geom_bar(stat = "identity", position = "stack") +
  geom_text(aes(label = paste0(scales::percent(weight), "\n(SE: ", round(se, 4), ")")), 
            position = position_stack(vjust = 0.5), size = 3) +
  scale_x_continuous(expand = c(0, 0), limits = c(0, 1)) +
  labs(x = "Weight", y = NULL, title = "Population Weights for Targets") +
  theme_minimal() +
  theme(legend.position = "bottom",
        axis.text.x = element_blank(),
        axis.ticks.x = element_blank(),
        axis.text.y = element_text(size = 10),
        axis.ticks.y = element_blank())

# Generate the right label text
right_label_text <- paste("Right Populations:\n", paste(strwrap(paste(right, collapse = ", "), width = 60), collapse = "\n"))

# Create a text plot for the right label text with white background
text_plot <- ggplot() + 
  annotate("text", x = 0.5, y = 0.5, label = right_label_text, size = 3.5, hjust = 0.5, vjust = 0.5, lineheight = 0.9) +
  theme_void() +
  theme(plot.background = element_rect(fill = "white", color = NA))

# Combine the plot and text plot into a single figure
combined_plot <- grid.arrange(plot, text_plot, ncol = 1, heights = c(4, 1))

# Generate a timestamp
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

# Save the combined plot as an image file with timestamp
ggsave(paste0("c:/qpadmgraphics/population_weights_bar_chart_", timestamp, ".png"), combined_plot, width = 10, height = 8, dpi = 300)
