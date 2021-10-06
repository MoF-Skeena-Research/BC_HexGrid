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

##make preselected points dataset
st_layers("~/CommonTables/ForestRegions.gpkg")
dists <- st_read("~/CommonTables/ForestRegions.gpkg","ForestDistricts2")
dists <- dists["ORG_UNIT"]
dists$ORG_UNIT <- as.character(dists$ORG_UNIT)
dists$ORG_UNIT[dists$ORG_UNIT == "DSS"] <- "CAS"
colnames(dists)[1] <- "dist_code"
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, user = "postgres", host = "138.197.168.220",password = "PowerOfBEC", port = 5432, dbname = "cciss") ### for local use
st_write(dists,con,"district_map")
dbExecute(con,"create table district_ids as
              select district_map.dist_code,
              hex_points.siteno 
              from hex_points
              inner join district_map
              on ST_Intersects(hex_points.geom,district_map.geom)")

dat1 <- fread("/media/data/ClimateBC_Data/SelectVars/Tile9_Y_SV.csv")
dat2 <- fread("/media/data/ClimateBC_Data/SelectVars/Tile9_S_SV.csv")
datYS <- cbind(dat1,dat2)
datYS <- datYS[Year != "GFDL-ESM4_ssp585_2061-2080.gcm",]
dat2 <- fread("/media/data/ClimateBC_Data/SelectVars/Tile9_1_M_SV.csv")
dat3 <- fread("/media/data/ClimateBC_Data/SelectVars/Tile9_2_M_SV.csv")
dat4 <- rbind(dat2,dat3)
dat4 <- dat4[Year != "GFDL-ESM4_ssp585_2061-2080.gcm",]
dat4[,c("Year","ID1") := NULL]
datYSM <- cbind(datYS,dat4)
rm(dat1,dat2,datYS,dat3,dat4)
gc()
fwrite(datYSM,"Tile8_AllDat.csv")

dat <- fread("./Output/Tile14_2.csv")
dat <- fwrite(dat,"./Output/Tile14_2_fix.csv",eol = "\r\n")

monthNms <- c("PPT05","PPT06","PPT07","PPT08","PPT09","CMD07")
colMonth <- c(47,48,49,50,51,157)
annNms <- c("AHM","CMI","DD5","NFFD","PAS","SHM","CMD")
colAnn <- c(1,2,3,13,14,16,19,23,28,30)
seasNms <- c("PPT_at","PPT_wt","DD_0_at","DD_0_wt")
colSeas <- c(19,22,27,30)
allNms <- c(monthNms,annNms,seasNms)

fread("BC_HexGrid/ClimBC_Out/Tile8_In_280 GCMsM.csv",nrows = 1)
test <- fread("BC_HexGrid/ClimBC_Out/Tile14_1_280_GCMsMSY.csv", select = "Year")
finished <- unique(test$Year)
testY <- fread("/media/data/ClimateBC_Data/Tile9_In_280_GCMsM.csv", select = monthNms)
dput(which(names(testY) %in% monthNms))
cols <- c(1L, 2L, 3L, 47L, 48L, 49L, 50L, 51L, 157L, 199L, 202L, 207L, 210L, 253L, 
          254L, 256L, 259L, 263L, 268L, 270L)


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
con <- dbConnect(drv, user = "postgres", host = "138.197.168.220",password = "PowerOfBEC", port = 5432, dbname = "cciss") ### for local use

futSiteno <- dbGetQuery(con, "select * from temp_siteno")
futSiteno <- as.data.table(futSiteno)
futSiteno[,InDB := TRUE]

grd <- st_read("~/BC_HexGrid/BC_HexPoints400m.gpkg")
grd <- as.data.table(grd)
grd[futSiteno,InDB := i.InDB, on = "siteno"]
grd2 <- grd[(InDB),]
grd2 <- st_as_sf(grd2)
grd2 <- grd2["siteno"]
st_write(grd2,dsn = "GrdPointsInDB.gpkg", layer = "GrdPoints", format = "GPKG")


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

load("~/BC_HexGrid/WNAv12_Subzone_11_Var_ranger_6Sept21.Rdata")

##WARNING!! Below code will permanently delete existing tables from the database.
###########################################################################
dbExecute(con, "DROP TABLE cciss_future")
dbExecute(con, "DROP TABLE cciss_historic")
dbExecute(con, "DROP TABLE future_sf")
dbExecute(con, "DROP TABLE historic_sf")
#######################################################################3

load("./BGC_models/WNAv12_Subzone_19_Var_ranger_17Jan21.Rdata")

##helper predict function if the tiles are too big for prediction all at once
##doesn't return anything, adds in place
tile_predict <- function(Y1, maxSize = 6000000){
  n = nrow(Y1)
  brks <- seq(1,n,by = maxSize)
  brks <- c(brks,n)
  Y1[,BGC.pred := NA_character_]
  for(j in 1:(length(brks)-1)){
    Y1[brks[j]:brks[j+1],BGC.pred := predict(BGCmodel2, Y1[brks[j]:brks[j+1],-c(1:3)],num.threads = 14)[['predictions']]]
  }
  TRUE
}

##files with all variables
setwd("/media/data/ClimateBC_Data/")
setwd("./ClimBC_Out/")
files <- list.files("./ClimBC_Out/")
allVars <- files[grep("Tile14",files)]
filesUse <- allVars[grep("Tile21|Tile22|Tile23|Tile17|Tile18|Tile13",allVars)]
for(tile in allVars){
  cat(".")
  outname <- gsub("MSY.*","",tile)
  system(paste0("cut -d, -f 1,2,3,47,48,49,50,51,157,199,202,207,210,253,254,256,259,263,268,270 ",
                tile," > ",outname,"_SV.csv"))
}

##Year
allVars <- files[grep("GCMsY",files)]
for(tile in allVars){
  cat(".")
  outname <- gsub("_.*","",tile)
  system(paste0("cut -d, -f 1,2,3,13,14,16,19,23,28,30 ",
                tile," > SelectVars/",outname,"_Y_SV.csv"))
}

##seas
allVars <- files[grep("GCMsS",files)]
for(tile in allVars){
  cat(".")
  outname <- gsub("_.*","",tile)
  system(paste0("cut -d, -f 19,22,27,30 ",
                tile," > SelectVars/",outname,"_S_SV.csv"))
}

##month
allVars <- files[grep("GCMsM.csv",files)]
setwd("~/BC_HexGrid/ClimBC_Out/")
files <- list.files()
allVars <- files[grep("GCMsM.csv|Month",files)]
tile <- "Tile23_In_280_GCMsM.csv"
for(tile in allVars){
  cat(".")
  outname <- gsub("_In.*","",tile)
  system(paste0("cut -d, -f 1,2,47,48,49,50,51,157 ",
                tile," > ",outname,"_M_SV.csv"))
}

setwd("/media/data/ClimateBC_Data/SelectVars/temp")
tileNums <- c(8,9,24,25)
for(tile in tileNums){
  yNm <- paste0("Tile",tile,"_Y_SV.csv")
  sNm <- paste0("Tile",tile,"_S_SV.csv")
  mNm <- paste0("Tile",tile,"_M_SV.csv")
  outNm <- paste0("Tile",tile,"_SV.csv")
  system(paste0("paste -d ',' ",yNm," ",sNm," ",mNm," > ",outNm))
}

setwd("~/BC_HexGrid/")
d1 <- fread("Tile14_1_133__SV.csv")
d1 <- d1[Year != "GFDL-ESM4_ssp245_2041-2060.gcm",]
d2 <- fread("Tile14_1_Part2__SV.csv")
Tile1 <- rbind(d1,d2)

d1 <- fread("Tile14_2_133__SV.csv")
d1 <- d1[Year != "GFDL-ESM4_ssp245_2041-2060.gcm",]
d2 <- fread("Tile14_2_fix_Part2__SV.csv")
Tile2 <- rbind(d1,d2)

Tile3 <- fread("Tile14_3_280_GCMs_SV.csv")

dat <- rbind(Tile1,Tile2,Tile3)
fwrite(dat,"Tile14_Final.csv")
##done 11,12,13,15,16,17,18,19,20,21,22,7,1,2,3,4,5
setwd("/media/data/ClimateBC_Data/SelectVars/")
files <- list.files()
filesUse <- files[-(1:2)]
## future periods
tableName <- "cciss_future12"
#datDir <- "E:/BGC_Hex/BCHex_ClimateBC/Future/"
#futNums <- c(0:5,7,8,10:19) ##tiles 6 and 9 are too big, I split them up into 14:19
for(input in filesUse){
  cat("Processing tile",input,"... \n")
  # varImport <- c("Year", "ID1","ID2","AHM", "CMD_sp", "DD5", "DD5_sm", 
  #                "DD5_sp", "MCMT", "MWMT", "NFFD", "PAS_sp", 
  #                "PAS_wt", "SHM", "PPT_at","PPT_wt","PPT05", "PPT06", "PPT07", "PPT08", 
  #                "PPT09", "CMD07","CMD","DD_0_at","DD_0_wt","CMI","PAS")
  # if(i == 0) varImport[2] <- "ID1"
  # dat <- fread(paste0(datDir,"Tile",i,"_Fut.csv"),select = varImport)

  
  dat <- fread(input)
  addVars(dat)
  gc()
  vars <- BGCmodel2[["forest"]][["independent.variable.names"]]
  varList = c("Model", "SiteNo", "BGC", vars)
  setnames(dat,old = c("Year","ID1","ID2"), new = c("Model","SiteNo","BGC"))
  setcolorder(dat,varList)

  dat <- na.omit(dat)
  
  ##Predict future subzones######
  tile_predict(dat)
  gc()
  dat[,c("CMD.total", "DD_delayed", "AHM", 
         "CMDMax", "CMI", "DD5", "NFFD", "PAS", "PPT_JAS", "PPT_MJ", "SHM", 
         "CMD", "PPT_at", "PPT_wt", "DD_0_at", "DD_0_wt", 
         "PPT05", "PPT06", "PPT07", "PPT08", "PPT09", "CMD07", "PPT.dormant","CMD.def") := NULL]
  gc()
  dat <- dat[!grepl("ensemble",Model),]
  dat[,c("GCM","Scenario","FuturePeriod") := tstrsplit(Model, split = "_", fixed = T)]
  dat[,FuturePeriod := gsub(".gcm","",FuturePeriod)]
  dat[,Model := NULL]
  setcolorder(dat,c("GCM","Scenario","FuturePeriod","SiteNo","BGC","BGC.pred"))
  setnames(dat, c("gcm","scenario","futureperiod","siteno","bgc","bgc_pred"))
  # dat[newID,siteno := i.Index, on = "OldIdx"]
  # dat[,OldIdx := NULL]
  setcolorder(dat,c("gcm","scenario","futureperiod","siteno","bgc","bgc_pred"))
  print(nrow(dat))
  con <- dbConnect(drv, user = "postgres", host = "138.197.168.220",password = "PowerOfBEC", port = 5432, dbname = "cciss") ### for local use
  dbWriteTable(con, "cciss_future12", dat,row.names = F, append = T)
  dbDisconnect(con)
  rm(dat)
  gc()
}

##Current Period Probability
setwd("~/BC_HexGrid/")
load("./WNAv12_Subzone_11_Var_ranger_probability_24Aug21.Rdata")
varImport <- c("ID1","ID2","AHM", "CMD_sp", "DD5", "DD5_sm", 
               "DD5_sp", "MCMT", "MWMT", "NFFD", "PAS_sp", 
               "PAS_wt", "SHM", "PPT_at","PPT_wt","PPT05", "PPT06", "PPT07", "PPT08", 
               "PPT09", "CMD07","CMD","DD_0_at","DD_0_wt","CMI","PAS","EXT","bFFP")
files <- list.files("./ClimBC_Out/")
setwd("~/BC_HexGrid/ClimBC_Out/")
for(input in filesUse){
  cat("Processing",input)
  dat <- fread(input,select = varImport)
  addVars(dat)
  vars <- BGCmodel2p[["forest"]][["independent.variable.names"]]
  varList = c("SiteNo", "BGC", vars)
  setnames(dat, old = c("ID1","ID2"), new = c("SiteNo", "BGC"))
  Y1 <- dat
  Y1=Y1[,..varList]
  Y1 <- na.omit(Y1)
  
  grid.pred <- predict(BGCmodel2p, data = Y1[,-c(1:2)])
  predMat <- grid.pred$predictions
  predMat <- as.data.table(predMat) 
  predMat[,`:=`(SiteNo = Y1$SiteNo,BGC = Y1$BGC)]
  predMat <- melt(predMat, id.vars = c("SiteNo","BGC"), variable.name = "Pred",value.name = "Prob")
  predMat <- predMat[Prob > 0.1,]
  setorder(predMat,SiteNo)
  predMat[,ProbAdj := Prob/sum(Prob), by = SiteNo]
  predMat[,Prob := NULL]
  #predMat[bgcID, bgc := i.ID2, on = c(ID = "ID1")]
  predMat[,period := "1991"]
  # setnames(predMat,old = "SiteNo",new = "OldIdx")
  # 
  # newID <- fread("RCB_CrosswalkTable.csv")
  # predMat[newID, SiteNo := i.Index, on = "OldIdx"]
  # predMat[,OldIdx := NULL]
  setcolorder(predMat, c("SiteNo","period","BGC","Pred","ProbAdj"))
  setnames(predMat,c("siteno","period","bgc","bgc_pred","prob"))
  con <- dbConnect(drv, user = "postgres", host = "138.197.168.220",password = "PowerOfBEC", port = 5432, dbname = "cciss") ### for local use
  dbWriteTable(con, "cciss_prob12", predMat,row.names = F, append = T)
  dbDisconnect(con)
  rm(dat,Y1,grid.pred,predMat)
  gc()
}


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
  
  ##Predict normal period subzones######
  Y1[,BGC.pred := predict(BGCmodel2, Y1[,-c(1:2)])[['predictions']]]
  gc()
  Y1[,Period := "Normal61"]
  Y1 <- Y1[,.(Period,SiteNo,BGC,BGC.pred)]
  setnames(Y1, c("period","siteno","bgc","bgc_pred"))
  dbWriteTable(con, tableName, Y1,row.names = F, append = T)
  rm(Y1,dat)
  gc()
  
}

## Current Period 1991-2019
###This will use a probability model and weight the historic BGC slightly for conservatism.
### The probabilities will be used in the species selection apps this will Likely need to store the data in the future table as there will now be multiple plausible BGC states
### In addition however, a majority vote layer (incorporating the present BGC weighting) should also be created for use in the CreateBCMaps function - this could remained stored in the historic period tables
### For the CreateBCMaps
tableName <- "cciss_historic"
datDir <-"E:/BGC_Hex/BCHex_ClimateBC/Current/"
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
  
  ##Predict 1991-2019 subzones######
  Y1[,BGC.pred := predict(BGCmodel2, Y1[,-c(1:2)])[['predictions']]]
  gc()
  Y1[,Period := "Current91"]
  Y1 <- Y1[,.(Period,SiteNo,BGC,BGC.pred)]
  setnames(Y1, c("period","siteno","bgc","bgc_pred"))
  dbWriteTable(con, tableName, Y1,row.names = F, append = T)
  rm(Y1,dat)
  gc()
  
}

###now do joins and create indices
dbExecute(con, "create table historic_sf as select cciss_historic.*,grid_dist.dist_code,grid_dist.geom from cciss_historic,grid_dist where cciss_historic.siteno = grid_dist.siteno")
dbExecute(con, "create table future_sf as select cciss_future.*,grid_dist.dist_code,grid_dist.geom from cciss_future,grid_dist where cciss_future.siteno = grid_dist.siteno")
dbExecute(con, "create index fut_sf_idx on future_sf (dist_code,scenario,gcm,futureperiod)")
dbExecute(con, "create index hist_sf_idx on historic_sf (dist_code,period)")
dbExecute(con, "create index fut_idx on cciss_future (siteno)")
dbExecute(con, "create index hist_idx on cciss_historic (siteno)")



###OLD CODE
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

###add hex grid to db
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
