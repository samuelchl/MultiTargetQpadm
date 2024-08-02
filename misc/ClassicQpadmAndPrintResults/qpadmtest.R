library("admixtools")
library("tidyverse")

parse_populations <- function(population_string) {
  return(unlist(strsplit(population_string, ",")))
}

prefix_ho = "c:/datasets/v54.1.p1_HO_public"
prefix_1240k = "c:/datasets/v54.1.p1_1240k_public"

left3 = c("Lebanon_MBA.SG","CanaryIslands_Guanche.SG","France_Occitanie_EBA.SG")
left2 = c("Lebanon_MBA.SG","CanaryIslands_Guanche.SG","France_Occitanie_EBA.SG","Tajikistan_Ksirov_Kushan")

left <- scan(text = "Lebanon_MBA.SG CanaryIslands_Guanche.SG France_Occitanie_EBA.SG Tajikistan_Ksirov_Kushan", what = character())
left <- scan(text = "Lebanon_MBA.SG CanaryIslands_Guanche.SG France_Occitanie_EBA.SG", what = character())

rightPersonal = c("Mbuti.DG", "Israel_PPNB", "Russia_MA1_HG.SG", "Turkey_Boncuklu_N", "Turkey_Epipaleolithic", "Morocco_Iberomaurusian", "Serbia_IronGates_Mesolithic", "Luxembourg_Loschbour.DG", "Russia_Karelia_HG", "Georgia_Kotias.SG", "Iran_GanjDareh_N", "China_Tianyuan", "Indian_GreatAndaman_100BP.SG", "Mongolia_North_N", "Russia_Tyumen_HG", "Israel_Natufian", "Jordan_PPNB")
right = c("Mbuti.DG", "Ami.DG", "Basque.DG", "Biaka.DG", "Bougainville.DG", "Chukchi.DG", "Eskimo_Naukan.DG", "Han.DG", "Iran_GanjDareh_N", "Ju_hoan_North.DG", "Karitiana.DG", "Papuan.DG", "Sardinian.DG", "She.DG", "Ulchi.DG", "Yoruba.DG")

target = "Jew_Moroccan.HO"

result = qpadm(prefix_ho, left, right, target, allsnps = TRUE)

result$weights
result$popdrop