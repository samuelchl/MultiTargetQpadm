if (!requireNamespace("remotes", quietly = TRUE)) {
install.packages("remotes")
}
# Install the interactive ADMIXTOOLS app with dependencies
#remotes::install_github("uqrmaie1/admixtools", dependencies = TRUE)
remotes::install_local('C:/Users/samuel.Chlouch/Documents/admixtools', dependencies = TRUE)


# Load the admixtools library
library(admixtools)

# Manually install additional dependencies if needed
install.packages("Rcpp")
install.packages("tidyverse")
install.packages("igraph")
install.packages("plotly")