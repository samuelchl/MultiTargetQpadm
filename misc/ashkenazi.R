library("admixtools")
library("tidyverse")

# Define the dataset prefix
prefix_ho = "c:/datasets/v54.1.p1_HO_public"

# Define the left populations
left = c("Lebanon_MBA.SG", "CanaryIslands_Guanche.SG", "Poland_EBA.SG", "Italy_Imperial.SG", "France_Occitanie_EBA.SG")

left = c( "CanaryIslands_Guanche.SG", "Poland_EBA.SG", "Italy_Imperial.SG")
left = c("CanaryIslands_Guanche.SG",  "Italy_Imperial.SG", "France_Occitanie_EBA.SG")

left = c( "CanaryIslands_Guanche.SG", "Poland_EBA.SG", "Italy_Imperial.SG", "France_Occitanie_EBA.SG")

left = c("Lebanon_MBA.SG", "CanaryIslands_Guanche.SG", "France_Occitanie_EBA.SG")

left = c("Lebanon_MBA.SG", "CanaryIslands_Guanche.SG", "Poland_EBA.SG")


# Define the right populations
right = c("Yoruba.DG", "Turkey_Boncuklu_N", "Turkey_Epipaleolithic", "Morocco_Iberomaurusian", 
          "Serbia_IronGates_Mesolithic", "Luxembourg_Loschbour.DG", "Russia_Karelia_HG", 
          "Georgia_Kotias.SG", "Iran_GanjDareh_N", "China_Tianyuan")



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
