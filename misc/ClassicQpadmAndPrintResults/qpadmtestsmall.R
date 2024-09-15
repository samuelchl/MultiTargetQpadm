library("admixtools")
library("tidyverse")


prefix_ho = "c:/datasets/v54.1.p1_HO_public"
prefix_1240k = "c:/datasets/v54.1.p1_1240k_public"

input_string <- "Mbuti.DG,Israel_PPNB,Russia_MA1_HG.SG,Turkey_Boncuklu_N,Turkey_Epipaleolithic,Morocco_Iberomaurusian,Serbia_IronGates_Mesolithic,Luxembourg_Loschbour.DG,Russia_Karelia_HG,Georgia_Kotias.SG,Iran_GanjDareh_N,China_Tianyuan,Indian_GreatAndaman_100BP.SG,Mongolia_North_N,Jordan_PPNB,Russia_Tyumen_HG,Israel_Natufian"
#input_string <- "Mbuti.DG,Israel_PPNB,Russia_MA1_HG.SG,Turkey_Boncuklu_N,Turkey_Epipaleolithic,Morocco_Iberomaurusian,Serbia_IronGates_Mesolithic,Luxembourg_Loschbour.DG,Russia_Karelia_HG,Georgia_Kotias.SG,Iran_GanjDareh_N,China_Tianyuan,Indian_GreatAndaman_100BP.SG,Mongolia_North_N,Russia_Tyumen_HG,Israel_Natufian,Jordan_PPNB"

right <- unlist(strsplit(input_string,split = ","))

input_string_left <- "CanaryIslands_Guanche.SG,Italy_PianSultano_BA.SG,Syria_TellQarassa_Umayyad,Syria_TellMasaikh_Medieval.SG,Germany_BA.SG,Iran_DinkhaTepe_BA_IA_1"
left_initial <- unlist(strsplit(input_string_left,split = ","))

target = "Turkish.DG"

result = qpadm(prefix_ho,left_initial,right,target,allsnps = TRUE)

result$weights
result$popdrop