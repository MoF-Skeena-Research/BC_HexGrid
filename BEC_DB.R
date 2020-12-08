## extract data from postgres, and merge with hex grid to plot
## Kiri Daust 2020
library(data.table)
library(sf)
library(RPostgreSQL)
library(dplyr)
library(dbplyr)
library(foreach)
library(rmapshaper)

##connect to database
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, user = "postgres", host = "192.168.1.64",password = "Kiriliny41", port = 5432, dbname = "cciss_data") ### for local use
con <- dbConnect(drv, user = "postgres", host = "localhost",password = "Kiriliny41", port = 5432, dbname = "cciss_data") ## for local machine
con <- dbConnect(drv, user = "postgres", host = "smithersresearch.ca",password = "Kiriliny41", port = 5432, dbname = "cciss_data") ### for external use

cleanCrumbs <- function(minNum = 3, dat){
  minArea <- minNum*138564.1
  dat <- st_cast(dat,"MULTIPOLYGON") %>% st_cast("POLYGON")
  dat$Area <- st_area(dat)
  dat$ID <- seq_along(dat$BGC)
  bgcIdx <- data.table(ID = dat$ID,BGC = dat$BGC)
  tooSmall <- dat[dat$Area < minArea,]
  tooSmall$smallID <- seq_along(tooSmall$BGC)
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

distcodes <- dbGetQuery(con, "select * from districts")[,1]

per <- "Current91"
provClean <- foreach(dcode = distcodes,.combine = rbind) %do% {
  q1 <- paste0("select siteno,period,bgc_pred,dist_code,geom from comb_norm_sf where dist_code = '",dcode,
               "' and period = '",per,"'")
  cat("Processing",dcode,"\n")
  cat("Pulling data - ")
  dist <- st_read(con, query = q1)
  cat("Joining - ")
  distDiss <- aggregate(dist[,"geom"],  by = list(dist$bgc_pred), FUN = mean)
  colnames(distDiss)[1] <- "BGC"
  cat("Cleaning \n")
  datClean <- cleanCrumbs(3, dat = distDiss)
  rm(dist,distDiss)
  gc()
  datClean
}




##pull tables from database
src_dbi(con)
allDat <- tbl(con,"dat_comb") ## joins of the next two tables
dat <- tbl(con,"cciss_400m") ## predicted BGC
att <- tbl(con, "id_atts")##region district ID
dat_norm <- tbl(con, "dat_comb_normal")

##Will - you probably want something like this
datTemp <- dat %>%
  filter(gcm %in% c(...), scenario == "xxx", futureperiod == "xxx") %>%
  select(gcm,scenario,bgc,bgc_pred,siteno)

### user selected parameters

reg.opts <- tbl(con,"regions") %>% collect()
mod.opts <- tbl(con,"models") %>% collect()
RegSelect <- select.list(reg.opts$reg_code, multiple = T, graphics = T)
period <- select.list(c("Normal61","Current91", "2025","2055","2085"), multiple = T, graphics = T)
outputName <- "MapTest91.gpkg"

##pull data using parameters
if(any(period %in% c("2025","2055","2085"))){
  model <- select.list(mod.opts$gcm, multiple = T, graphics = T)
  scn <- select.list(c("rcp45","rcp85"), multiple = T, graphics = T)
  dat <- allDat %>%
    filter(reg_code %in% RegSelect, gcm %in% model, scenario %in% scn, futureperiod %in% period) %>%
    select(reg_code,gcm,scenario,futureperiod, siteno,bgc,bgc_pred) %>%
    collect()
  datFut <- as.data.table(dat)
}

if(any(period %in% c("Normal61","Current91"))){
  per2 <- period
  dat <- dat_norm %>%
    filter(reg_code %in% RegSelect, period %in% per2) %>%
    dplyr::select(reg_code,period, siteno,bgc,bgc_pred) %>%
    collect()
  datCurr <- as.data.table(dat)
}


grd <- st_read("D:/CommonTables/HexGrids/HexGrd400.gpkg")

grd <- st_read(dsn = "/media/kiridaust/MrBig/BCGrid/HexGrd400.gpkg") ## base 400m hexgrid of province
#grd <- st_read("C:/Users/whmacken/Sync/CCISS_data/BC_400mbase_hexgrid/HexGrd400.gpkg")

grd <- as.data.table(grd)
grd <- st_sf(grd)
setnames(grd, c("siteno","geom"))

# require(doParallel)
# cl <- makePSOCKcluster(detectCores()-2)
# registerDoParallel(cl)

### this section aggregates by BGC to reduce the size of the resultant layers
if(exists("datFut")){
  outname <- paste0("Future_",outputName)
  foreach(mod = model, .combine = c) %:%
    foreach(s = scn, .combine = c) %:%
    foreach(per = period, .combine = c,.packages = c("sf","data.table")) %do% {
      sub <- datFut[gcm == mod & scenario == s & futureperiod == per,]
      map <- merge(grd,sub, by = "siteno", all = F)
      
      mapComb <- aggregate(map[,"geometry"],  by = list(map$bgc_pred), FUN = mean)
      colnames(mapComb)[1] <- "BGC"
      
      st_write(mapComb, dsn = outputName, layer = paste(RegSelect,mod,s,per,sep = "_"), append = T)
      TRUE
    }
}

if(exists("datCurr")){
  outname <- paste0("Current_",outputName)
  foreach(per = unique(datCurr$period), .combine = c,.packages = c("sf","data.table")) %do% {
    sub <- datCurr[period == per,]
    map <- merge(grd,sub, by = "siteno", all = F)
    
    mapComb <- aggregate(map[,"geometry"],  by = list(map$bgc_pred), FUN = mean)
    colnames(mapComb)[1] <- "BGC"
    
    st_write(mapComb, dsn = outputName, layer = paste(RegSelect,mod,s,per,sep = "_"), append = T)
    TRUE
  }
}

##example despeckling
library(fasterize)
map <- as.data.table(mapComb) %>% st_as_sf()
##have to convert BGC labels to ints
legend <- data.table(BGC = unique(map$BGC))
legend[,Value := seq_along(BGC)]
map <- legend[map, on = "BGC"]
map <- st_as_sf(map) %>% st_cast("MULTIPOLYGON")
rast <- raster(map, resolution = 200) ##covert to raster with twice the resolution
rast <- fasterize(map,rast, field = "Value")

tMat <- as.matrix(values(rast))
Rcpp::sourceCpp("_RasterDespeckle.cpp")
tMat <- pem_focal(tMat, wrow = 7, wcol = 7) ##7*7 focal window to finding mode
tMat[tMat < 0] <- NA
values(rast) <- tMat
writeRaster(rast, "TestBGCRaster.tif",format = "GTiff")

if(exists("datCurr")){
  outname <- paste0("Current_",outputName)
    foreach(per = period, .combine = c,.packages = c("sf","data.table")) %do% {
      sub <- dat[period == per,]
      map <- merge(grd,sub, by = "siteno", all = F)
      
      mapComb <- aggregate(map[,"geometry"],  by = list(map$bgc_pred), FUN = mean)
      colnames(mapComb)[1] <- "BGC"
      st_write(mapComb, dsn = outputName, layer = paste(RegSelect,per,sep = "_"), append = T)
      TRUE
    }
}




