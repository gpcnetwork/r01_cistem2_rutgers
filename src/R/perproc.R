rm(list=ls()); gc()
setwd("C:/repos/r01_cistem2_rutgers")

pacman::p_load(
  tidyverse,
  magrittr,
  stringr
)

path_to_data<-"C:/repos/r01_cistem2_rutgers/data"
path_to_res<-"C:/repos/r01_cistem2_rutgers/res"

aset<-readRDS(file.path(path_to_data,"ktx_ptbl.rda")) %>%
  mutate(
    DEATH_1YR = case_when(DEATH_IND==1&DAYS_TO_CENSOR<=365 ~ 1, TRUE ~ 0),
    DEATH_3YR = case_when(DEATH_IND==1&DAYS_TO_CENSOR<=365*3 ~ 1, TRUE ~ 0),
    DEATH_5YR = case_when(DEATH_IND==1&DAYS_TO_CENSOR<=365*5 ~ 1, TRUE ~ 0)
  )

saveRDS(aset,file=file.path(path_to_data,"ktx_ptbl_preproc.rda"))