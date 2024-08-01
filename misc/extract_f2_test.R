# Increase memory limit
#memory.limit(size = 12000)

# Clear environment and garbage collect
rm(list = ls())
gc()


library("admixtools")
library("tidyverse")

parse_populations <- function(population_string) {
  return(unlist(strsplit(population_string, ",")))
}

prefix_ho = "c:/datasets/v54.1.p1_HO_public"
prefix_1240k = "c:/datasets/v54.1.p1_1240k_public"
f2_folder = "c:/f2data"
f2_folder2 = "c:/f2data2"
f2_folder3 = "c:/f2data3"

left = c("Lebanon_MBA.SG","CanaryIslands_Guanche.SG","France_Occitanie_EBA.SG")
left2 = c("Lebanon_MBA.SG","CanaryIslands_Guanche.SG","France_Occitanie_EBA.SG","Tajikistan_Ksirov_Kushan")

rightPersonal = c("Mbuti.DG", "Israel_PPNB", "Russia_MA1_HG.SG", "Turkey_Boncuklu_N", "Turkey_Epipaleolithic", "Morocco_Iberomaurusian", "Serbia_IronGates_Mesolithic", "Luxembourg_Loschbour.DG", "Russia_Karelia_HG", "Georgia_Kotias.SG", "Iran_GanjDareh_N", "China_Tianyuan", "Indian_GreatAndaman_100BP.SG", "Mongolia_North_N", "Russia_Tyumen_HG", "Israel_Natufian", "Jordan_PPNB")
right = c("Mbuti.DG", "Ami.DG", "Basque.DG", "Biaka.DG", "Bougainville.DG", "Chukchi.DG", "Eskimo_Naukan.DG", "Han.DG", "Iran_GanjDareh_N", "Ju_hoan_North.DG", "Karitiana.DG", "Papuan.DG", "Sardinian.DG", "She.DG", "Ulchi.DG", "Yoruba.DG")

target = "Jew_Moroccan.HO"

poplist2 = c("Yoruba.DG", "Mbuti.DG", "Morocco_Iberomaurusian", "Ethiopia_4500BP.SG", "Israel_Natufian", "Israel_PPNB", "Lebanon_MBA.SG", "Iran_GanjDareh_N", "Switzerland_Bichon.SG", "Luxembourg_Loschbour.DG", "Russia_Samara_HG", "Russia_Samara_EBA_Yamnaya", "Russia_Karelia_HG", "Russia_EHG", "Greece_BA_Mycenaean", "Italy_Imperial.SG", "France_Occitanie_EBA.SG", "Poland_EBA.SG", "Kazakhstan_Botai_Eneolithic.SG", "Tajikistan_Ksirov_Kushan", "Han.DG", "Mongolia_North_N", "China_Tianyuan", "Papuan.DG", "Karitiana.DG", "Chimp.REF", "Jew_Ashkenazi.HO", "Jew_Moroccan.HO", "Iran_Wezmeh_N.SG", "Iran_TepeAbdulHosein_N.SG")

input_string <- "Lebanon_ERoman.SG,Lebanon_Roman.SG,Italy_Bivio_Roman.SG,Serbia_Sirmium_Roman.SG,Romania_BA_Arman,Croatia_Zadar_Roman.SG,Italy_Sardinia_SantImbenia_RomanImperial.SG,Algeria_NumidoRoman_Berber.SG,France_BA_GalloRoman,Italy_Tuscany_Siena_Etruscan,Spain_BA.SG,Spain_Roman,Spain_LBA,Poland_Southeast_BellBeaker.SG,Poland_Viking.SG,Poland_EBA_Unetice.SG,Italy_Tuscany_Grosseto_Etruscan,Germany_EMedieval_Alemanic,Germany_CordedWare.SG,Germany_CordedWare,Germany_BellBeaker,Germany_Anderten_Saxon_Medieval,Germany_BA.SG,France_SouthEast_IA2,France_Occitanie_MN.SG,France_Occitanie_MBA.SG,France_Occitanie_LN.SG,France_Occitanie_IA2.SG,France_Occitanie_EMBA.SG,Lebanon_Chhim_Phoenician.SG,Yoruba.DG,Israel_Natufian,Morocco_Iberomaurusian,Turkey_Epipaleolithic,Switzerland_Bichon.SG,Russia_Samara_HG,Georgia_Satsurblia.SG,Mongolia_North_N,Russia_Steppe_Eneolithic,Papuan.DG,Spain_ElMiron,Kazakhstan_Botai_Eneolithic.SG,Russia_Tyumen_HG,Mbuti.DG,Russia_MA1_HG.SG,Turkey_Boncuklu_N,Serbia_IronGates_Mesolithic,Luxembourg_Loschbour.DG,Russia_Karelia_HG,Georgia_Kotias.SG,Indian_GreatAndaman_100BP.SG,Russia_DevilsCave_N.SG,Turkey_Alalakh_MLBA,Iran_GanjDareh_N,Turkey_Barcin_LN.SG,Cameroon_SMA.DG,Chimp.REF,Israel_PPNB,Israel_MLBA,Lebanon_MBA.SG,France_Occitanie_EBA.SG,CanaryIslands_Guanche.SG,Poland_EBA.SG,Italy_IsolaSacra_RomanImperial.SG,Tajikistan_Ksirov_Kushan,Italy_Imperial.SG,Greece_NeaNikomedeia_EN.SG,Russia_EHG,Morocco_EN.SG,Ethiopia_4500BP.SG,Russia_Samara_EBA_Yamnaya,Luxembourg_Loschbour,Jordan_PPNB,Turkey_EBA_II.SG,Greece_BA_Mycenaean,Ami.DG,Basque.DG,Biaka.DG,Bougainville.DG,Chukchi.DG,Eskimo_Naukan.DG,Han.DG,Ju_hoan_North.DG,Karitiana.DG,Sardinian.DG,She.DG,Ulchi.DG,China_Tianyuan,Dinka.DG,Jew_Ashkenazi.HO,Jew_Moroccan.HO,Iran_Wezmeh_N.SG,Iran_TepeAbdulHosein_N.SG"


# Split the string by comma and convert it to a vector
poplist <- parse_populations(input_string)


extract_f2(prefix_ho, f2_folder2, pops = poplist, overwrite = TRUE, adjust_pseudohaploid = TRUE, qpfstats = FALSE,n_cores = 8,maxmem =12000, auto_only = FALSE,maxmiss = 1)

#extract_f2_large(poplist,f2_folder,pops = poplist,overwrite = TRUE,maxmiss = 1)