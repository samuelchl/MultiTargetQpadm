library("admixtools")
library("tidyverse")

prefix_ho = "c:/datasets/v54.1.p1_HO_public"
prefix_1240k = "c:/datasets/v54.1.p1_1240k_public"

right_str <- "Cameroon_SMA.DG,Switzerland_Bichon.SG,Russia_Kostenki14,Mongolia_North_N,Georgia_Satsurblia.SG,Turkey_Boncuklu_N,Iran_TepeAbdulHosein_N.SG,Israel_Natufian,Morocco_Iberomaurusian,Russia_MA1_HG.SG,Russia_Karelia_HG,Indian_GreatAndaman_100BP.SG"
right_str <- "Cameroon_SMA.DG,Switzerland_Bichon.SG,Russia_Kostenki14,Mongolia_North_N,Georgia_Satsurblia.SG,Turkey_Boncuklu_N,Iran_TepeAbdulHosein_N.SG,Israel_PPNB,Russia_MA1_HG.SG,Russia_Karelia_HG,Indian_GreatAndaman_100BP.SG,Jordan_PPNB"
right_str <-"Cameroon_SMA.DG,Switzerland_Bichon.SG,Morocco_Iberomaurusian,Russia_Sidelkino_HG.SG,Georgia_Satsurblia.SG,Turkey_Epipaleolithic,Turkey_Boncuklu_N,Iran_TepeAbdulHosein_N.SG,Russia_Tyumen_HG,Indian_GreatAndaman_100BP.SG,Russia_Kostenki14,Russia_AngaraRiver_N.SG,Mongolia_North_N,Ethiopia_4500BP,Russia_Sunghir3.SG,Russia_MA1_HG.SG"
right_str <-"Yoruba.DG,Israel_Natufian,Jordan_PPNB,Turkey_Boncuklu_N,Morocco_Iberomaurusian,Luxembourg_Loschbour.DG,Russia_Samara_HG,Georgia_Satsurblia.SG,Iran_TepeAbdulHosein_N.SG,Russia_Steppe_Eneolithic,Kazakhstan_Botai_Eneolithic.SG"

# Split the string by comma and convert it to a vector
right <- parse_populations(right_str)

left_str <- "Turkey_N,Israel_Natufian,Iran_Wezmeh_N.SG,Georgia_Kotias.SG,Russia_Samara_EBA_Yamnaya,Luxembourg_Loschbour.DG"
left_str <- "Turkey_Barcin_LN.SG,Iran_Wezmeh_N.SG,Israel_PPNB,Russia_Samara_EBA_Yamnaya,Ethiopia_4500BP.SG,Georgia_Kotias.SG,Luxembourg_Loschbour,Morocco_EN.SG"

#Israel_PPNB,Turkey_N,Turkey_Barcin_LN.SG

# Split the string by comma and convert it to a vector
left <- parse_populations(left_str)

target = "Jew_Moroccan.HO"

result = qpadm(prefix_ho, left, right, target, allsnps = TRUE)

result$weights
result$popdrop

