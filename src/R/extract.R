rm(list=ls()); gc()
setwd("C:/repos/r01_cistem2_rutgers")

# install.packages("pacman")
pacman::p_load(
  DBI,
  jsonlite,
  odbc,
  tidyverse,
  magrittr,
  dbplyr
)

# make db connection
sf_conn <- DBI::dbConnect(
  drv = odbc::odbc(),
  dsn = 'snowflake_deid',
  uid = '',
  pwd = ''
)

path_to_data<-"C:/repos/r01_cistem2_rutgers/data"
# path_to_ref<-"C:/repos/r01_cistem2_rutgers/ref"

# collect final aset
aset<-tbl(sf_conn,sql("select * from KTX_TBL1")) %>% collect()
#- data
saveRDS(aset,file=file.path(path_to_data,"ktx_tbl2.rda"))

# disconnect
DBI::dbDisconnect(sf_conn)

