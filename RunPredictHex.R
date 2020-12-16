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
for(i in 1){
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
    dbWriteTable(con, "cciss_future", Y1,row.names = F, append = T)
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


