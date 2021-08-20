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
require(RPostgreSQL)
library(disk.frame)
require(RPostgres)

monthNms <- c("PPT05","PPT06","PPT07","PPT08","PPT09","CMD07")

monthDat <- csv_to_disk.frame("~/BC_HexGrid/ClimBC_Out/RCB_ClimBC_260 GCMsM.csv",
                              in_chunk_size = 1000000,
                              backend = "data.table",
                              chunk_reader = "readLines",
                              overwrite = T)

test <- fread("~/BC_HexGrid/ClimBC_Out/MonthSelect.csv")
test <- fread("ClimBC_Out/RCB_ClimBC_Normal_1991_2020MSY.csv", nrows = 0)
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
  dat[,DD_delayed := ((DD_0_at+ DD_0_wt )*0.0238) - 1.8386]
  dat[DD_delayed < 0, DD_delayed := 0]
  return(dat)
}

drv <- Postgres()
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, user = "postgres", host = "localhost",password = "Kiriliny41", port = 5432, dbname = "cciss_data")
con <- dbConnect(drv, user = "postgres", host = "138.197.168.220",password = "postgres", port = 5432, dbname = "cciss") ### for local use

# grd <- st_read("BC_HexPoly400m.gpkg")
# st_write(grd, dsn = con,"hex_grid")
# districts <- st_read(file.choose())
# districts <- districts[,c("DISTRICT_N","ORG_UNIT","REGION_ORG")]
# pnts <- st_read("BC_HexPoints400m.gpkg")
# 
# grd2 <- st_join(pnts,districts)
# colnames(grd2)[2:4] <- c("district","dist_code","reg_code")
# st_write(grd2, dsn = con,"hex_points")
# atts <- as.data.table(st_drop_geometry(grd2))
# atts <- atts[!is.na(dist_code),]
# dbWriteTable(con,"id_atts",atts, row.names = F)

# crosstab <- fread("Old_NewHexCrosswalk.csv")
# crosstab <- crosstab[!is.na(OldID),]
# setnames(crosstab,c("new_id","old_id"))
# dbWriteTable(con, "id_crosswalk", crosstab, row.names = F)

load("./WNAv12_Subzone_11_Var_ranger_8July21.Rdata")



##helper predict function if the tiles are too big for prediction all at once
##doesn't return anything, adds in place
tile_predict <- function(Y1, maxSize = 6000000){
  n = nrow(Y1)
  brks <- seq(1,n,by = maxSize)
  brks <- c(brks,n)
  Y1[,BGC.pred := NA_character_]
  for(j in 1:(length(brks)-1)){
    Y1[brks[j]:brks[j+1],BGC.pred := predict(BGCmodel2, Y1[brks[j]:brks[j+1],-c(1:3)])[['predictions']]]
  }
  TRUE
}

setwd("/media/data/ClimateBC_Data/")
for(tile in 15:22){
  fname <- paste0("Tile",tile,"_In_280_GCMsMSY.csv")
  system(paste0("cut -d, -f2 ",fname," > Tile",tile,"_ID.csv"))
}

dat <- vroom("./ClimBC_Out/Tile10_In_224 GCMsMSY.csv")
## future periods
tableName <- "cciss_future"
datDir <- "~/Desktop/BCHex_ClimateBC/Future/"
futNums <- c(0:5,7,8,10:19) ##tiles 6 and 9 are too big, I split them up into 14:19
for(i in futNums){
  cat("Processing tile",i,"... \n")
  varImport <- c("ID1","ID2","AHM", "CMD_sp", "DD5", "DD5_sm", 
                 "DD5_sp", "MCMT", "MWMT", "NFFD", "PAS_sp", 
                 "PAS_wt", "SHM", "PPT_at","PPT_wt","PPT05", "PPT06", "PPT07", "PPT08", 
                 "PPT09", "CMD07","CMD","DD_0_at","DD_0_wt","CMI","PAS")
  # if(i == 0) varImport[2] <- "ID1"
  # dat <- fread(paste0(datDir,"Tile",i,"_Fut.csv"),select = varImport)
  loc <- "~/BC_HexGrid/ClimBC_Out/RCB_ClimBC_260 GCMs"
  allNms <- c("M.csv","S.csv","Y.csv")
  nms <- fread(paste0(loc,allNms[3]),nrows = 0)
  yearNms <- varImport[varImport %in% names(nms)]
  yearDat <- fread(paste0(loc,allNms[3]),select = yearNms)
  seasNms <- c("PPT_at","PPT_wt","DD_0_at","DD_0_wt")
  seasDat <- fread(paste0(loc,allNms[2]),select = seasNms)
  dat <- cbind(yearDat,seasDat)
  rm(yearDat,seasDat)
  gc()
  monthDat <- fread("ClimBC_Out/MonthSelect.csv")
  gc()
  dat[,`:=`(Year = monthDat$Year,
            PPT05 = monthDat$PPT05,
            PPT06 = monthDat$PPT06,
            PPT07 = monthDat$PPT07,
            PPT08 = monthDat$PPT08,
            PPT09 = monthDat$PPT09,
            CMD07 = monthDat$CMD07)]
  rm(monthDat)
  gc()
  fwrite(dat[1:105000000,],file = "RCB_Tile1.csv")
  gc()
  fwrite(dat[105000001:nrow(dat),],file = "RCB_Tile2.csv")
  gc()
  addVars(dat)
  gc()
  
  dat <- fread("ClimBC_Out/RCB_Tile1.csv")
  addVars(dat)
  gc()
  vars <- BGCmodel2[["forest"]][["independent.variable.names"]]
  varList = c("Model", "SiteNo", "BGC", vars)
  setnames(dat,old = c("Year","ID1","ID2"), new = c("Model","SiteNo","BGC"))
  setcolorder(dat,varList)

  Y1 <- na.omit(Y1)
  
  ##Predict future subzones######
  tile_predict(dat)
  gc()
  dat[,c("CMD.total", "DD_delayed", "AHM", 
         "CMDMax", "CMI", "DD5", "NFFD", "PAS", "PPT_JAS", "PPT_MJ", "SHM", 
         "MCMT", "MWMT", "CMD", "PPT_at", "PPT_wt", "DD_0_at", "DD_0_wt", 
         "PPT05", "PPT06", "PPT07", "PPT08", "PPT09", "CMD07", "PPT.dormant","CMD.def") := NULL]
  gc()
  dat[,c("GCM","Scenario","FuturePeriod") := tstrsplit(Model, split = "_", fixed = T)]
  dat[,FuturePeriod := gsub(".gcm","",FuturePeriod)]
  dat[,Model := NULL]
  setcolorder(dat,c("GCM","Scenario","FuturePeriod","SiteNo","BGC","BGC.pred"))
  setnames(dat, c("gcm","scenario","futureperiod","OldIdx","bgc","bgc_pred"))
  dat[newID,siteno := i.Index, on = "OldIdx"]
  dat[,OldIdx := NULL]
  setcolorder(dat,c("gcm","scenario","futureperiod","siteno","bgc","bgc_pred"))
  dat <- fread("RCB_FuturePreds_Tile1.csv")
  dbWriteTable(con, "test_future", dat,row.names = F, append = T)
  rm(Y1,dat)
  gc()
    
}

##Current Period Probability
load("./WNAv12_Subzone_12_Var_ranger_prob.Rdata")
varImport <- c("ID1","ID2","AHM", "CMD_sp", "DD5", "DD5_sm", 
               "DD5_sp", "MCMT", "MWMT", "NFFD", "PAS_sp", 
               "PAS_wt", "SHM", "PPT_at","PPT_wt","PPT05", "PPT06", "PPT07", "PPT08", 
               "PPT09", "CMD07","CMD","DD_0_at","DD_0_wt","CMI","PAS","EXT","bFFP")
dat <- fread("~/BC_HexGrid/ClimBC_Out/RCB_ClimBC_Normal_1991_2020MSY.csv",select = varImport)
addVars(dat)
vars <- BGCmodel_prob[["forest"]][["independent.variable.names"]]
varList = c("SiteNo", "BGC", vars)
setnames(dat, old = c("ID1","ID2"), new = c("SiteNo", "BGC"))
Y1 <- dat
Y1=Y1[,..varList]
Y1 <- na.omit(Y1)

grid.pred <- predict(BGCmodel_prob, data = Y1[,-c(1:2)])
predMat <- grid.pred$predictions
predMat <- as.data.table(predMat) 
predMat[,`:=`(SiteNo = Y1$SiteNo,BGC = Y1$BGC)]
predMat <- melt(predMat, id.vars = c("SiteNo","BGC"), variable.name = "Pred",value.name = "Prob")
predMat <- predMat[Prob > 0.1,]
setorder(predMat,SiteNo)
predMat[,ProbAdj := Prob/sum(Prob), by = SiteNo]
predMat[,Prob := NULL]
#predMat[bgcID, bgc := i.ID2, on = c(ID = "ID1")]
predMat[,period := "1991-2020"]
setnames(predMat,old = "SiteNo",new = "OldIdx")

newID <- fread("RCB_CrosswalkTable.csv")
predMat[newID, SiteNo := i.Index, on = "OldIdx"]
predMat[,OldIdx := NULL]
setcolorder(predMat, c("SiteNo","period","BGC","Pred","ProbAdj"))
setnames(predMat,c("siteno","period","bgc","bgc_pred","prob"))
dbWriteTable(con, "test_prob", predMat,row.names = F, append = T)

## Normal Period
tableName <- "cciss_historic"
datDir <- "~/Desktop/BCHex_ClimateBC/Normal/"
varImport <- c("ID1","ID2","AHM", "CMD_sp", "DD5", "DD5_sm", 
               "DD5_sp", "MCMT", "MWMT", "NFFD", "PAS_sp", 
               "PAS_wt", "SHM", "PPT_at","PPT_wt","PPT05", "PPT06", "PPT07", "PPT08", 
               "PPT09", "CMD07","CMD","DD_0_at","DD_0_wt","CMI","PAS")
for(i in 0:13){
  # cat("Processing tile",i,"... \n")
  # IDName <- "NewID"
  # if(i == 0) IDName <- "ID1"
  # varImport[1] <- IDName
  dat <- fread(paste0(datDir,"Tile",i,"_Norm.csv"),select = varImport) ##point to climateBC data
  dat <- fread("~/BC_HexGrid/ClimBC_Out/RCB_ClimBC_Normal_1991_2020MSY.csv",select = varImport)
  addVars(dat)
  
  vars <- BGCmodel2[["forest"]][["independent.variable.names"]]
  varList = c("SiteNo", "BGC", vars)
  setnames(dat, old = c("ID1","ID2"), new = c("SiteNo", "BGC"))
  Y1 <- dat
  Y1=Y1[,..varList]
  Y1 <- na.omit(Y1)
  
  ##Predict future subzones######
  Y1[,BGC.pred := predict(BGCmodel2, Y1[,-c(1:2)])[['predictions']]]
  gc()
  Y1[,Period := "Normal61"]
  Y1 <- Y1[,.(Period,SiteNo,BGC,BGC.pred)]
  setnames(Y1, c("period","siteno","bgc","bgc_pred"))
  dbWriteTable(con, tableName, Y1,row.names = F, append = T)
  rm(Y1,dat)
  gc()
  
}

## Current Period
tableName <- "cciss_historic"
datDir <- "~/Desktop/BCHex_ClimateBC/Current/"
varImport <- c("ID1","ID2","AHM", "bFFP", "CMD_sp", "DD5", "DD5_sm", 
               "DD5_sp", "Eref_sm", "Eref_sp", "MCMT", "MWMT", "NFFD", "PAS_sp", 
               "PAS_wt", "SHM", "Tmax_sm","PPT_at","PPT_wt","PPT05", "PPT06", "PPT07", "PPT08", 
               "PPT09", "CMD07","CMD","DD_0_at","DD_0_wt")
for(i in 0:13){
  cat("Processing tile",i,"... \n")
  IDName <- "NewID"
  if(i == 0) IDName <- "ID1"
  varImport[1] <- IDName
  dat <- fread(paste0(datDir,"Tile",i,"_Curr.csv"),select = varImport)
  Y1 <- addVars(dat)
  vars <- BGCmodel2[["forest"]][["independent.variable.names"]]
  varList = c("SiteNo", "BGC", vars)
  setnames(Y1, old = c(IDName,"ID2"), new = c("SiteNo", "BGC"))
  Y1=Y1[,..varList]
  Y1 <- Y1[Tmax_sm > -100,]
  Y1 <- na.omit(Y1)
  
  ##Predict future subzones######
  Y1[,BGC.pred := predict(BGCmodel2, Y1[,-c(1:2)])[['predictions']]]
  gc()
  Y1[,Period := "Current91"]
  Y1 <- Y1[,.(Period,SiteNo,BGC,BGC.pred)]
  setnames(Y1, c("period","siteno","bgc","bgc_pred"))
  dbWriteTable(con, tableName, Y1,row.names = F, append = T)
  rm(Y1,dat)
  gc()
  
}


