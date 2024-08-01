# Increase memory limit
#memory.limit(size = 16000)

# Clear environment and garbage collect
rm(list = ls())
gc()

# Load necessary libraries
library("admixtools")
library("tidyverse")
library("parallel")

# Define paths
prefix_ho <- "c:/datasets/v54.1.p1_HO_public"
prefix_1240k <- "c:/datasets/v54.1.p1_1240k_public"
f2_folder <- "c:/f2data"

# Define populations
left <- c("Lebanon_MBA.SG","CanaryIslands_Guanche.SG","France_Occitanie_EBA.SG")
left2 <- c("Lebanon_MBA.SG","CanaryIslands_Guanche.SG","France_Occitanie_EBA.SG","Tajikistan_Ksirov_Kushan")
rightPersonal <- c("Mbuti.DG", "Israel_PPNB", "Russia_MA1_HG.SG", "Turkey_Boncuklu_N", "Turkey_Epipaleolithic", "Morocco_Iberomaurusian", "Serbia_IronGates_Mesolithic", "Luxembourg_Loschbour.DG", "Russia_Karelia_HG", "Georgia_Kotias.SG", "Iran_GanjDareh_N", "China_Tianyuan", "Indian_GreatAndaman_100BP.SG", "Mongolia_North_N", "Russia_Tyumen_HG", "Israel_Natufian", "Jordan_PPNB")
right <- c("Mbuti.DG", "Ami.DG", "Basque.DG", "Biaka.DG", "Bougainville.DG", "Chukchi.DG", "Eskimo_Naukan.DG", "Han.DG", "Iran_GanjDareh_N", "Ju_hoan_North.DG", "Karitiana.DG", "Papuan.DG", "Sardinian.DG", "She.DG", "Ulchi.DG", "Yoruba.DG")
target <- "Jew_Moroccan.HO"
poplist2 <- c("Yoruba.DG", "Mbuti.DG", "Morocco_Iberomaurusian", "Ethiopia_4500BP.SG", "Israel_Natufian", "Israel_PPNB", "Lebanon_MBA.SG", "Iran_GanjDareh_N", "Switzerland_Bichon.SG", "Luxembourg_Loschbour.DG", "Russia_Samara_HG", "Russia_Samara_EBA_Yamnaya", "Russia_Karelia_HG", "Russia_EHG", "Greece_BA_Mycenaean", "Italy_Imperial.SG", "France_Occitanie_EBA.SG", "Poland_EBA.SG", "Kazakhstan_Botai_Eneolithic.SG", "Tajikistan_Ksirov_Kushan", "Han.DG", "Mongolia_North_N", "China_Tianyuan", "Papuan.DG", "Karitiana.DG", "Chimp.REF", "Jew_Ashkenazi.HO", "Jew_Moroccan.HO", "Iran_Wezmeh_N.SG", "Iran_TepeAbdulHosein_N.SG")
input_string <- "Yoruba.DG,Israel_Natufian,Morocco_Iberomaurusian,Turkey_Epipaleolithic,Switzerland_Bichon.SG,Russia_Samara_HG,Georgia_Satsurblia.SG,Mongolia_North_N,Russia_Steppe_Eneolithic,Papuan.DG,Spain_ElMiron,Kazakhstan_Botai_Eneolithic.SG,Russia_Tyumen_HG,Mbuti.DG,Russia_MA1_HG.SG,Turkey_Boncuklu_N,Serbia_IronGates_Mesolithic,Luxembourg_Loschbour.DG,Russia_Karelia_HG,Georgia_Kotias.SG,Indian_GreatAndaman_100BP.SG,Russia_DevilsCave_N.SG,Turkey_Alalakh_MLBA,Iran_GanjDareh_N,Turkey_Barcin_LN.SG,Cameroon_SMA.DG,Chimp.REF,Israel_PPNB,Israel_MLBA,Lebanon_MBA.SG,France_Occitanie_EBA.SG,CanaryIslands_Guanche.SG,Poland_EBA.SG,Italy_IsolaSacra_RomanImperial.SG,Tajikistan_Ksirov_Kushan,Italy_Imperial.SG,Greece_NeaNikomedeia_EN.SG,Russia_EHG,Morocco_EN.SG,Ethiopia_4500BP.SG,Russia_Samara_EBA_Yamnaya,Luxembourg_Loschbour,Jordan_PPNB,Turkey_EBA_II.SG,Greece_BA_Mycenaean,Ami.DG,Basque.DG,Biaka.DG,Bougainville.DG,Chukchi.DG,Eskimo_Naukan.DG,Han.DG,Ju_hoan_North.DG,Karitiana.DG,Sardinian.DG,She.DG,Ulchi.DG,China_Tianyuan,Dinka.DG,Jew_Ashkenazi.HO,Jew_Moroccan.HO,Iran_Wezmeh_N.SG,Iran_TepeAbdulHosein_N.SG"

# Split the string by comma and convert it to a vector
poplist <- unlist(strsplit(input_string, split = ","))

# Define chunk size
chunk_size <- 10  # Adjust chunk size based on your memory capacity

# Function to process chunks
process_chunk <- function(chunk) {
  extract_f2(prefix_ho, f2_folder, pops = chunk, overwrite = TRUE, adjust_pseudohaploid = TRUE, qpfstats = TRUE)
}

# Split the population list into chunks
poplist_chunks <- split(poplist, ceiling(seq_along(poplist) / chunk_size))

# Process each chunk
results <- lapply(poplist_chunks, process_chunk)

# Combine results if needed
combined_results <- do.call(rbind, results)
