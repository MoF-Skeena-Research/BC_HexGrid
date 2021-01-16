## extract data from postgres, and merge with hex grid to plot
## Kiri Daust 2020
library(data.table)
library(sf)
library(RPostgreSQL)
library(dplyr)
library(dbplyr)
library(foreach)
library(rmapshaper)
library(tictoc)

##connect to database
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, user = "postgres", host = "192.168.1.64",password = "Kiriliny41", port = 5432, dbname = "cciss_data") ### for local use
#con <- dbConnect(drv, user = "postgres", host = "localhost",password = "Kiriliny41", port = 5432, dbname = "cciss_data") ## for local machine
#con <- dbConnect(drv, user = "postgres", host = "smithersresearch.ca",password = "Kiriliny41", port = 5432, dbname = "cciss_data") ### for external use

cleanCrumbs <- function(minNum = 3, dat){
  minArea <- minNum*138564.1
  units(minArea) <- "m^2"
  dat <- st_cast(dat,"MULTIPOLYGON") %>% st_cast("POLYGON")
  dat$Area <- st_area(dat)
  dat$ID <- seq_along(dat$BGC)
  bgcIdx <- data.table(ID = dat$ID,BGC = dat$BGC)
  tooSmall <- dat[dat$Area < minArea,]
  tooSmall$smallID <- seq_along(tooSmall$BGC)
  if(nrow(tooSmall) < 1){
    return(dat[,"BGC"])
  }
  smallIdx <- as.data.table(st_drop_geometry(tooSmall[,c("ID","smallID")]))
  
  temp <- st_intersects(tooSmall,dat,dist = 5)
  cat(".")
  idTouch <- data.table()
  for(i in 1:length(temp)){
    t1 <- data.table(Touching = temp[[i]])
    t1[,ID := i]
    idTouch <- rbind(idTouch,t1,fill = T)
  }
  cat(".")
  idTouch[smallIdx, origIdx := i.ID, on = c(ID = "smallID")]
  idTouch <- idTouch[Touching != origIdx,]
  idTouch[bgcIdx,BGC := i.BGC, on = c(Touching = "ID")]
  idTouch <- idTouch[, .(Num = .N,Touching), by = .(origIdx,BGC)
    ][order(-Num), head(.SD,1), by = origIdx]
  idTouch[,Num := NULL]
  dat <- as.data.table(dat)
  dat[idTouch, BGC := i.BGC, on = c(ID = "origIdx")]
  dat <- st_as_sf(dat)
  dat <- aggregate(dat[,"geometry"],  by = list(dat$BGC), FUN = mean)
  cat(".")
  colnames(dat)[1] <- "BGC"
  return(dat)
}

## pull district codes from database
#distcodes <- dbGetQuery(con, "select * from districts")[,1]
distcodes <- dbGetQuery(con, "select * from dist_codes")[,1]

library(doParallel)
cl <- makeCluster(detectCores()-2)
registerDoParallel(cl)

###future - currently just looping through models, assuming only 1 time periods and scenario. Could change this
gcms <- c("ACCESS1-0","CanESM2","CCSM4","CESM1-CAM5","CNRM-CM5","CSIRO-Mk3-6-0","GFDL-CM3","GISS-E2R","HadGEM2-ES",
          "INM-CM4","IPSL-CM5A-MR","MIROC5","MIROC-ESM","MRI-CGCM3","MPI-ESM-LR")

#gcms <- c("CESM1-CAM5","CNRM-CM5","CSIRO-Mk3-6-0","GFDL-CM3")
scn <- "rcp45"
per <- 2055
tic()
for(mod in gcms){
  provClean <- foreach(dcode = distcodes,.combine = rbind) %do% { ##for each district
    q1 <- paste0("select siteno,bgc_pred,dist_code,geom from future_sf where dist_code = '",dcode,
                 "' and scenario = '",scn,"'and gcm = '",mod,"' and futureperiod = '",per,"'")
    cat("Processing",dcode,"\n")
    ##pull data
    dist <- st_read(con, query = q1)
    ##create tile grid
    testTile <- st_make_grid(dist,n = c(4,4))
    testTile <- st_as_sf(data.frame(tID = 1:16, geom = testTile))
    temp <- st_join(dist,testTile)
    
    tic()
    ##for each tile, dissolve and cleanup
    out <- foreach(tid = unique(temp$tID), .combine = rbind, .packages = c("sf","data.table")) %dopar% {
      datSub <- temp[temp$tID == tid,]
      distDiss <- aggregate(datSub[,"geom"],  by = list(datSub$bgc_pred), FUN = mean)
      colnames(distDiss)[1] <- "BGC"
      datClean <- cleanCrumbs(3, dat = distDiss)
      datClean <- cleanCrumbs(3, dat = datClean)
      datClean
    }
    toc()
    datClean <- aggregate(out[,"geometry"],  by = list(out$BGC), FUN = mean) ##union tiles
    colnames(datClean)[1] <- "BGC"
    
    rm(dist,out)
    gc()
    datClean
  }
  fname <- paste0("./maps/BCMap_",per,"_",scn,"_",mod,".gpkg")
  st_write(provClean,fname, append = FALSE)
}
toc()
### historic data - 61-90 or 91 - 2019
tic()
per <- "Normal61"## "Current91"#
provClean <- foreach(dcode = distcodes,.combine = rbind) %do% {
  q1 <- paste0("select siteno,period,bgc_pred,dist_code,geom from historic_sf where dist_code = '",dcode,
               "' and period = '",per,"'")
  cat("Processing",dcode,"\n")
  dist <- st_read(con, query = q1)
  testTile <- st_make_grid(dist,n = c(4,4))
  testTile <- st_as_sf(data.frame(tID = 1:16, geom = testTile))
  temp <- st_join(dist,testTile)
  
  tic()
  out <- foreach(tid = unique(temp$tID), .combine = rbind, .packages = c("sf","data.table")) %dopar% {
    datSub <- temp[temp$tID == tid,]
    distDiss <- aggregate(datSub[,"geom"],  by = list(datSub$bgc_pred), FUN = mean)
    colnames(distDiss)[1] <- "BGC"
    datClean <- cleanCrumbs(3, dat = distDiss)
    datClean <- cleanCrumbs(3, dat = datClean)
    datClean
  }
  toc()
  datClean <- aggregate(out[,"geometry"],  by = list(out$BGC), FUN = mean)
  colnames(datClean)[1] <- "BGC"
  
  rm(dist,out)
  gc()
  datClean
}

st_write(provClean,"./maps/BC_1961-1990.gpkg", append = FALSE)
toc()

