require(data.table)
require(randomForest)
require(ranger)
require(foreach)
require(dplyr)
require(reshape2)
library(doParallel)
library(tidyr)
require(sf)
library(here)
require(RPostgreSQL)

addVars <- function(dat){
  dat[,`:=`(PPT_MJ = PPT05+PPT06,
            PPT_JAS = PPT07+PPT08+PPT09,
            PPT.dormant = PPT_at+PPT_wt)]
  dat[,`:=`(CMD.def = 500-PPT.dormant)]
  dat[CMD.def < 0, CMD.def := 0]
  dat[,`:=`(CMDMax = CMD07,
            CMD.total = CMD.def +CMD)]
  return(dat)
}

drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, user = "postgres", host = "localhost",password = "Kiriliny41", port = 5432, dbname = "cciss_data")

datDir <- "~/ClimBC_Tiles/"
load("WNAv11_35_VAR_SubZone_ranger.Rdata")
varImport <- c("Year","ID1","ID2", "Tmax_sp", "Tmax_sm", "Tmin_wt", "Tmin_sp", "Tave_at", "PPT_wt", 
  "PPT_sp", "PPT_sm", "PPT_at", "MSP",
  "DD5_wt", "DD5_sp", "DD5_sm", "DD5_at", "PAS_wt", "PAS_sp", "PAS_sm", 
  "PAS_at", "Eref_sp", "Eref_sm", "NFFD", "bFFP", "eFFP", "MWMT", 
  "MCMT", "AHM", "SHM", "CMD", "PPT05", "PPT06", "PPT07", "PPT08", 
  "PPT09", "CMD07","CMD_sp", "CMD_at")

tile_predict <- function(Y1, maxSize = 6000000){
  n = nrow(Y1)
  brks <- seq(1,n,by = maxSize)
  brks <- c(brks,n)
  Y1[,BGC.pred := NA_character_]
  for(j in 1:(length(brks)-1)){
    Y1[brks[j]:brks[j+1],BGC.pred := predict(BGCmodel, Y1[brks[j]:brks[j+1],-c(1:3)])[['predictions']]]
  }
  TRUE
}


for(i in 14:19){
  cat("Processing tile",i,"... \n")
    dat <- fread(paste0(datDir,"Tile",i,"_Out.csv"),select = varImport)
    Y1 <- addVars(dat)
    
    vars <- BGCmodel[["forest"]][["independent.variable.names"]]
    varList = c("Model", "SiteNo", "BGC", vars)
    colnames (Y1) [1:3] = c("Model", "SiteNo", "BGC")
    Y1=Y1[,..varList]
    Y1 <- Y1[complete.cases(Y1),]
    
    ##Predict future subzones######
    tile_predict(Y1)
    gc()
    Y1 <- separate(Y1, Model, into = c("GCM","Scenario","FuturePeriod"), sep = "_", remove = T)
    gc()
    Y1$FuturePeriod <- gsub(".gcm","",Y1$FuturePeriod)
    Y1 <- Y1[,c("GCM","Scenario","FuturePeriod","SiteNo","BGC","BGC.pred")]
    setnames(Y1, c("gcm","scenario","futureperiod","siteno","bgc","bgc_pred"))
    dbWriteTable(con, "cciss_400m", Y1,row.names = F, append = T)
    rm(Y1,dat)
    gc()
    
}

grd <- st_read(dsn = "../BCGrid/HexGrd400.gpkg")
pts <-st_read(dsn = "../BCGrid/HexPts400.gpkg")
colnames(grd)[1] <- "id"
st_write(grd, con, drop = T)
test <- st_read(con,query = "SELECT * FROM grd WHERE id IN (5,6);")

districts <- st_read(dsn = "../CommonTables/ForestRegions.gpkg", layer = "ForestRegions_clipped")
districts <- districts[,c("REGION_NAM","ORG_UNIT")]

distJn <- st_join(pts,districts)
distJn <- distJn[!is.na(distJn$ORG_UNIT),]
dj <- st_drop_geometry(distJn) %>% as.data.table()
dj <- dj[!is.na(ORG_UNIT),]

districts <- st_read(dsn = "../CommonTables/ForestRegions.gpkg", layer = "ForestDistricts")
districts <- districts[,c("DISTRICT_N","ORG_UNIT")]
distJn2 <- st_join(pts,districts)
dj2 <- st_drop_geometry(distJn2) %>% as.data.table()
dj2 <- dj2[!is.na(ORG_UNIT),]

tID <- fread(file.choose())

dj3 <- dj2[dj, on = "ID"]
setnames(tID, c("ID","TileNum"))
dj3 <- tID[dj3, on = "ID"]
setnames(dj3,c("siteno","tileno","district","dist_code","region","reg_code"))

dbWriteTable(con,"id_atts", dj3,row.names = F)
dbGetQuery(con, "select distinct region from id_atts")

  sk <- districts[4,]

sites <- dbGetQuery(con, "select distinct siteno from cciss_400m")
sites <- dbGetQuery(con, "select * from cciss_400m 
                    inner join id_atts on cciss_400m.siteno = id_atts.siteno 
                    where reg_code = 'RSK'")
sno <- dbGetQuery(con, "select siteno from id_atts where reg_code = 'RSK'")

q <- paste0("select * from cciss_400m where siteno in (",paste(sno$siteno, collapse = ","),")")      
test <- dbGetQuery(con,q)
