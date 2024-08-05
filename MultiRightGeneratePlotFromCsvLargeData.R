# Load required libraries
library(ggplot2)
library(dplyr)
library(readr)
library(gridExtra)
library(grid)
library(ggtext)  # Load ggtext for formatted text

# Load the data
all_weights_data <- read_csv('c:/qpadmdata/all_weights_data.csv')
p_values_df <- read_csv('c:/qpadmdata/p_values.csv')
avg_se_df <- read_csv('c:/qpadmdata/avg_se.csv')
right_populations_df <- read_csv('c:/qpadmdata/right_populations.csv')

# Extract right population IDs from the target labels
right_ids <- unique(as.numeric(sub(".*_Rght(\\d+)$", "\\1", all_weights_data$target)))

# Filter right populations based on extracted IDs
filtered_right_populations_df <- right_populations_df %>%
  filter(RightId %in% right_ids)

# Wrap the right strings to ensure they fit within the table
wrap_text <- function(text, width) {
  sapply(strwrap(text, width = width, simplify = FALSE), paste, collapse = "\n")
}

filtered_right_populations_df <- filtered_right_populations_df %>%
  mutate(Right = wrap_text(Right, 150))  # Adjust the width as needed

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
  
  if (p_value < 0.05) {
    paste0("<span style='color:red;'>", target, " P: ", format(p_value, scientific = TRUE), " SE: ", round(avg_se, 4), "</span>")
  } else {
    paste0("<span style='color:#1e8449;'>",target, " P: ", format(p_value, scientific = TRUE), " SE: ", round(avg_se, 4), "</span>")
  }
}

# Add custom labels to the data
all_weights_data <- all_weights_data %>%
  mutate(label = sapply(target, custom_labeller))

# Adjust the weights to ensure they sum to 1 within each group
all_weights_data <- all_weights_data %>%
  group_by(target) %>%
  mutate(weight = weight / sum(weight))

# Create a single plot with horizontal stacked bars for each target
plot <- ggplot(all_weights_data, aes(y = label, x = weight, fill = population)) +
  geom_bar(stat = "identity", position = "stack") +
  geom_text(aes(label = paste0(scales::percent(weight), "\n(SE: ", round(se, 4), ")")), 
            position = position_stack(vjust = 0.5), size = 2.2) +  # Reduce font size for text inside bars
  scale_x_continuous(expand = c(0, 0), limits = c(0, 1)) +
  labs(x = "Weight", y = NULL, title = "Population Weights for Targets") +
  theme_minimal() +
  theme(legend.position = "bottom",
        axis.text.x = element_blank(),
        axis.ticks.x = element_blank(),
        axis.text.y = element_markdown(size = 8),  # Reduced text size for y-axis labels
        axis.ticks.y = element_blank(),
        plot.margin = unit(c(1, 1, 1, 5), "lines"))  # Increase left margin

# Ensure filtered_right_populations_df is not empty
if (nrow(filtered_right_populations_df) > 0) {
  # Generate the right label text table with wrapped text and reduced font size
  right_label_table <- tableGrob(filtered_right_populations_df, rows = NULL, 
                                 theme = ttheme_minimal(base_size = 8, padding = unit(c(2, 2), "mm")))
} else {
  right_label_table <- textGrob("No right populations found", gp = gpar(fontsize = 10))
}

# Calculate the number of unique targets
num_targets <- length(unique(all_weights_data$target))

# Set dynamic height based on the number of targets and number of rights
height_per_target <- 0.2  # Height per target in inches
base_height <- 5  # Base height for the plot in inches
plot_height <- base_height + (height_per_target * num_targets)

# Calculate height for the right populations table
num_rights <- nrow(filtered_right_populations_df)
right_table_height <- max(2, 0.5 * num_rights)  # Adjust height based on the number of right populations

# Combine the plot and table into a single figure
combined_plot <- grid.arrange(plot, right_label_table, ncol = 1, heights = c(plot_height, right_table_height))  # Adjust heights accordingly

# Generate a timestamp
timestamp <- format(Sys.time(), "%Y%m%d_%H%M%S")

# Save the combined plot as an image file with timestamp
ggsave(paste0("c:/qpadmgraphics/population_weights_bar_chart_", timestamp, ".png"), combined_plot, width = 12, height = plot_height + right_table_height, dpi = 300)
