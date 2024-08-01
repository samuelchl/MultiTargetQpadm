library("tidyverse")
library("ggplot2")
library("cowplot")

# Load the data
all_weights_data <- read_csv("all_weights_data.csv")
p_values_df <- read_csv("p_values.csv")
avg_se_df <- read_csv("avg_se.csv")

# Convert the p_values_df and avg_se_df to lists
p_values_list <- p_values_df$p_value
avg_se_list <- avg_se_df$avg_se

# Create a named vector for p_values and avg_se
p_values_named <- setNames(p_values_list, p_values_df$target)
avg_se_named <- setNames(avg_se_list, avg_se_df$target)

# Ensure that the lists are correctly formatted
print("p_values_named:")
print(p_values_named)
print("avg_se_named:")
print(avg_se_named)

# Function to create custom labels for facets using the target directly
custom_labeller <- function(target) {
  print(target)
  print(p_values_named)
  # Ensure the target is correctly identified and exists in the named vectors
  if (!target %in% names(p_values_named) || !target %in% names(avg_se_named)) {
    stop(paste("Target not found in dictionaries:", target))
  }
  
  p_value <- p_values_named[target]
  avg_se <- avg_se_named[target]
  
  # Ensure that p_value and avg_se are single values
  if (length(p_value) != 1 || length(avg_se) != 1) {
    stop(paste("Multiple or no values found for target:", target))
  }
  
  paste0(target, "\nP-value: ", format(p_value, scientific = TRUE), "\nAverage SE: ", round(avg_se, 4))
}

# Generate the plot with horizontal bars for each target
p <- ggplot(all_weights_data, aes(x = weight, y = population, fill = population)) +
  geom_bar(stat = "identity", width = 0.7) +
  geom_text(aes(label = paste0(scales::percent(weight), "\n(SE: ", round(se, 4), ")")), 
            position = position_stack(vjust = 0.5), size = 3, color = "black") +
  labs(title = "Population Weights for Targets", 
       x = "Proportion", y = NULL) +
  facet_wrap(~ target, scales = "free_y", ncol = 1, labeller = labeller(target = custom_labeller)) +
  theme_classic() +
  theme(
    legend.position = "bottom",
    axis.text.y = element_blank(), 
    axis.ticks.y = element_blank()
  )

# Generate the right label text
right_label_text <- paste("Right Populations:\n", paste(strwrap(paste(right, collapse = ", "), width = 60), collapse = "\n"))

# Create a text plot for the right label text with white background
text_plot <- ggplot() + 
  annotate("text", x = 0.5, y = 0.5, label = right_label_text, size = 4, hjust = 0.5, vjust = 0.5) +
  theme_void() +
  theme(plot.background = element_rect(fill = "white", color = NA))

# Combine the horizontal bar plot and the text plot into a grid with 2 rows
combined_plot <- plot_grid(p, text_plot, ncol = 1, rel_heights = c(3, 1))

# Generate a timestamp
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

# Save the combined plot as an image file with timestamp
ggsave(paste0("population_weights_bar_chart_", timestamp, ".png"), plot = combined_plot, width = 10, height = 15, dpi = 300)
