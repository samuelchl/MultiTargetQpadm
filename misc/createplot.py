import pandas as pd
import matplotlib.pyplot as plt

right = "test"

# Load the data
all_weights_data = pd.read_csv('all_weights_data.csv')
p_values_df = pd.read_csv('p_values.csv')
avg_se_df = pd.read_csv('avg_se.csv')

# Convert the p_values_df and avg_se_df to dictionaries
p_values_dict = pd.Series(p_values_df.p_value.values, index=p_values_df.target).to_dict()
avg_se_dict = pd.Series(avg_se_df.avg_se.values, index=avg_se_df.target).to_dict()

# Ensure that the dictionaries are correctly formatted and only contain single values per target
print("p_values_dict:")
print(p_values_dict)
print("avg_se_dict:")
print(avg_se_dict)

# Custom labeller function to create facet labels
def custom_labeller(target):
    p_value = p_values_dict.get(target)
    avg_se = avg_se_dict.get(target)

    # Ensure that p_value and avg_se are single values
    if p_value is None or avg_se is None:
        raise ValueError(f"Multiple or no values found for target: {target}")

    return f"{target}\nP-value: {format(p_value, '.2e')}\nAverage SE: {round(avg_se, 4)}"

# Add custom labels to the data
all_weights_data['label'] = all_weights_data['target'].apply(custom_labeller)

# Create a figure for the combined plot
fig, axarr = plt.subplots(len(all_weights_data['label'].unique()) + 1, 1, figsize=(10, 15), gridspec_kw={'height_ratios': [1]*len(all_weights_data['label'].unique()) + [1]})

# Plotting with Matplotlib
unique_labels = all_weights_data['label'].unique()
for i, label in enumerate(unique_labels):
    subset = all_weights_data[all_weights_data['label'] == label]
    ax = axarr[i]
    bottom = 0
    for _, row in subset.iterrows():
        ax.barh(label, row['weight'], left=bottom, color='skyblue', edgecolor='black', label=row['population'])
        ax.text(bottom + row['weight'] / 2, label, f"{row['population']} {row['weight']:.1%}\n(SE: {row['se']:.4f})", va='center', ha='center', color='black')
        bottom += row['weight']
    ax.set_xlim(0, 1)
    ax.set_xlabel('Weight')

# Generate the right label text
right_label_text = "Right Populations:\n" + ", ".join(["Your right populations here..."])

# Create a text plot for the right label text with white background
axarr[-1].text(0.5, 0.5, right_label_text, ha='center', va='center', size=12)
axarr[-1].axis('off')

# Adjust layout
plt.tight_layout()

# Save the combined plot as an image file with timestamp
import datetime
timestamp = datetime.datetime.now().strftime("%Y%m%d_%H%M%S")
fig.savefig(f'population_weights_bar_chart_{timestamp}.png', dpi=300, bbox_inches='tight')

plt.show()