##read in climateBC files, predict, and send to db
##Kiri Daust, Dec 2020

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

##this part for updating ID on climateBC outputs
# IDCross <- fread("Old_NewHexCrosswalk.csv")
# varNames <- fread(paste0("~/ClimBC_Tiles/Tile",1,"_Out.csv"), nrows = 0)
# varNames <- colnames(varNames)
# varNames <- c(varNames[c(1:6,175:253)],"PPT05", "PPT06", "PPT07", "PPT08", 
#               "PPT09","Tmax07","DD5_05","DD5_06","DD5_07","DD5_08","DD5_09",
#               "CMD05","CMD06","CMD07","CMD08","CMD09","Tave10","Tave04")
# deleteDT <- function(DT, del.idxs) {           # pls note 'del.idxs' vs. 'keep.idxs'
#   keep.idxs <- setdiff(DT[, .I], del.idxs);  # select row indexes to keep
#   cols = names(DT);
#   DT.subset <- data.table(DT[[1]][keep.idxs]); # this is the subsetted table
#   setnames(DT.subset, cols[1]);
#   for (col in cols[2:length(cols)]) {
#     DT.subset[, (col) := DT[[col]][keep.idxs]];
#     DT[, (col) := NULL];  # delete
#   }
#   return(DT.subset);
# }
# 
# dat <- fread("~/ClimBC_Tiles/NormalData/Tile6_In_Normal_1961_1990MSY.csv", select = "ID1")
# dat2 <- fread("~/ClimBC_Tiles/Tile14_Out.csv", select = "ID1")
# dat2 <- unique(dat2)
# 
# for(i in c(2:13)){
#   dat <- fread(paste0("~/ClimBC_Tiles/NormalData/Tile",i,"_In_Normal_1961_1990MSY.csv"))
#   dat[IDCross, NewID := i.NewID, on = c(ID1 = "OldID")]
#   dat[,ID1 := NULL]
#   nms <- colnames(dat)
#   nms <- nms[c(1,length(nms),2:(length(nms)-1))]
#   setcolorder(dat, nms)
#   toDelete <- which(is.na(dat$NewID))
#   dat <- deleteDT(dat,toDelete)
#   gc()
#   fwrite(dat,file = paste0("~/Desktop/BCHex_ClimateBC/Normal/Tile",i,"_Norm.csv"))
#   rm(dat)
#   gc()
# }
# 
# decs <- c("1991_2000","2001_2010","2011_2019")
# for(i in c(2:13)){
#   dat <- foreach(j = decs, .combine = rbind) %do% {
#     temp <- fread(paste0("~/ClimBC_Tiles/CurrentData/Tile",i,"_In_Decade_",j,"MSY.csv"))
#     #temp <- fread(paste0("~/ClimBC_Tiles/CurrentData/NewPnts_In_Decade_",j,"MSY.csv"))
#   }
#   dat <- dat[,lapply(.SD, mean), by = .(ID1,ID2)]
#   dat[IDCross, NewID := i.NewID, on = c(ID1 = "OldID")]
#   dat[,ID1 := NULL]
#   nms <- colnames(dat)
#   nms <- nms[c(1,length(nms),2:(length(nms)-1))]
#   setcolorder(dat, nms)
#   toDelete <- which(is.na(dat$NewID))
#   dat <- deleteDT(dat,toDelete)
#   gc()
#   fwrite(dat,file = paste0("~/Desktop/BCHex_ClimateBC/Current/Tile",i,"_Curr.csv"))
#   rm(dat)
#   gc()
# }


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
#con <- dbConnect(drv, user = "postgres", host = "localhost",password = "Kiriliny41", port = 5432, dbname = "cciss_data")
con <- dbConnect(drv, user = "postgres", host = "192.168.1.64",password = "Kiriliny41", port = 5432, dbname = "cciss_data") ### for local use
datDir <- "~/ClimBC_Tiles/"
load("./BGC_models/WNAv12_Subzone_19_Var_ranger_outlier_weights4_noclhs.Rdata")

grd <- st_read("BC_HexPoly400m.gpkg")
st_write(grd, dsn = con,"hex_grid")
districts <- st_read(file.choose())
districts <- districts[,c("DISTRICT_N","ORG_UNIT","REGION_ORG")]
pnts <- st_read("BC_HexPoints400m.gpkg")

grd2 <- st_join(pnts,districts)
colnames(grd2)[2:4] <- c("district","dist_code","reg_code")
st_write(grd2, dsn = con,"hex_points")
atts <- as.data.table(st_drop_geometry(grd2))
atts <- atts[!is.na(dist_code),]
dbWriteTable(con,"id_atts",atts, row.names = F)

# crosstab <- fread("Old_NewHexCrosswalk.csv")
# crosstab <- crosstab[!is.na(OldID),]
# setnames(crosstab,c("new_id","old_id"))
# dbWriteTable(con, "id_crosswalk", crosstab, row.names = F)

datDir <- "~/ClimBC_Tiles/"
load("./BigDat/WNAv11_35_VAR_SubZone_ranger.Rdata")
varImport <- c("Year","ID1","ID2", "Tmax_sp", "Tmax_sm", "Tmin_wt", "Tmin_sp", "Tave_at", "PPT_wt", 
  "PPT_sp", "PPT_sm", "PPT_at", "MSP",
  "DD5_wt", "DD5_sp", "DD5_sm", "DD5_at", "PAS_wt", "PAS_sp", "PAS_sm", 
  "PAS_at", "Eref_sp", "Eref_sm", "NFFD", "bFFP", "eFFP", "MWMT", 
  "MCMT", "AHM", "SHM", "CMD", "PPT05", "PPT06", "PPT07", "PPT08", 
  "PPT09", "CMD07","CMD_sp", "CMD_at")

##helper predict function if the tiles are too big for prediction all at once
##doesn't return anything, adds in place
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

## future periods
tableName <- "cciss_future"
for(i in 1:19){
  cat("Processing tile",i,"... \n")
    dat <- fread(paste0(datDir,"Tile",i,"_Out.csv"),select = varImport)
    Y1 <- addVars(dat)
    
    vars <- BGCmodel[["forest"]][["independent.variable.names"]]
    varList = c("Model", "SiteNo", "BGC", vars)
    colnames (Y1) [1:3] = c("Model", "SiteNo", "BGC")
    Y1=Y1[,..varList]
    Y1 <- Y1[Tmax_sp > -100,]
    Y1 <- Y1[complete.cases(Y1),]
    
    ##Predict future subzones######
    tile_predict(Y1)
    gc()
    Y1 <- separate(Y1, Model, into = c("GCM","Scenario","FuturePeriod"), sep = "_", remove = T)
    gc()
    Y1$FuturePeriod <- gsub(".gcm","",Y1$FuturePeriod)
    Y1 <- Y1[,c("GCM","Scenario","FuturePeriod","SiteNo","BGC","BGC.pred")]
    setnames(Y1, c("gcm","scenario","futureperiod","siteno","bgc","bgc_pred"))

    Y1[,old_id := NA]
    dbWriteTable(con, tableName, Y1,row.names = F, append = T)
    rm(Y1,dat)
    gc()
    
}

## Normal Period
for(i in 1){
  cat("Processing tile",i,"... \n")
  dat <- fread(paste0(datDir,"Tile",i,"_Out.csv"),select = varImport) ##point to climateBC data
  Y1 <- addVars(dat)
  
  vars <- BGCmodel[["forest"]][["independent.variable.names"]]
  varList = c("SiteNo", "BGC", vars)
  setnames(Y1, old = c("ID1","ID2"), new = c("SiteNo", "BGC"))
  Y1=Y1[,..varList]
  Y1 <- Y1[Tmax_sp > -100,]
  Y1 <- Y1[complete.cases(Y1),]
  
  ##Predict future subzones######
  Y1[,BGC.pred := predict(BGCmodel, Y1[,-c(1:2)])[['predictions']]]
  gc()
  Y1[,Period := "Normal61"]
  Y1 <- Y1[,c("Period","SiteNo","BGC","BGC.pred")]
  setnames(Y1, c("period","siteno","bgc","bgc_pred"))
  Y1[,old_id := NA]
  dbWriteTable(con, "cciss_historic", Y1,row.names = F, append = T)
  rm(Y1,dat)
  gc()
  
}

## Current Period
datDir <- "~/ClimBC_Tiles/CurrentData/"
inputName <- "NewPnts_In"
for(i in 1){
  cat("Processing tile",i,"... \n")
  dat1 <- fread(paste0(datDir,"Tile",i,"In_Decade_1991_2000MSY.csv"),select = varImport) ##point to climateBC data
  dat2 <- fread(paste0(datDir,"Tile",i,"In_Decade_2001_2010MSY.csv"),select = varImport)
  dat3 <- fread(paste0(datDir,"Tile",i,"In_Decade_2011_2019MSY.csv"),select = varImport)
  dat <- rbind(dat1,dat2,dat3)
  dat <- dat[Tmax_sp > -100,]
  dat <- dat[,lapply(.SD,mean),by = .(ID1,ID2)]
  rm(dat1,dat2,dat3)
  gc()
  Y1 <- addVars(dat)
  
  vars <- BGCmodel[["forest"]][["independent.variable.names"]]
  varList = c("SiteNo", "BGC", vars)
  setnames(Y1, old = c("ID1","ID2"), new = c("SiteNo", "BGC"))
  Y1=Y1[,..varList]
  Y1 <- Y1[Tmax_sp > -100,]
  Y1 <- Y1[complete.cases(Y1),]
  
  ##Predict future subzones######
  Y1[,BGC.pred := predict(BGCmodel, Y1[,-c(1:2)])[['predictions']]]
  gc()
  Y1[,Period := "Current91"]
  Y1 <- Y1[,c("Period","SiteNo","BGC","BGC.pred")]
  setnames(Y1, c("period","siteno","bgc","bgc_pred"))
  Y1[,old_id := NA]
  dbWriteTable(con, "cciss_historic", Y1,row.names = F, append = T)
  rm(Y1,dat)
  gc()
  
}


