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
source("./_functions/_cleancrumbs.R")

##connect to database
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, user = "postgres", 
                 host = "138.197.168.220",
                 password = "PowerOfBEC", port = 5432, 
                 dbname = "cciss") ### for local use
#con <- dbConnect(drv, user = "postgres", host = "localhost",password = "Kiriliny41", port = 5432, dbname = "cciss_data") ## for local machine
#con <- dbConnect(drv, user = "postgres", host = "smithersresearch.ca",password = "Kiriliny41", port = 5432, dbname = "cciss_data") ### for external use

###read grid
tic()
hexGrid <- st_read(con, query = "select * from hex_grid")
toc()
#st_write(hexGrid,"./outputs/basehex.gpkg", append = FALSE)

# hexGrid <- st_read("HexGrid400m.gpkg") ##whatever yours is called
# #hexGrid <- st_read("E:/Sync/CCISS_data/SpatialFiles/BC_400mbase_hexgrid/HexGrd400.gpkg") 
# colnames(hexGrid)[1] <- "siteno"
# hexGrid <- as.data.table(hexGrid) ##convert to data table because join is much faster
# hexGrid[datPred, bgc_pred := i.bgc_pred, on = "siteno"]
<<<<<<< HEAD

test <- dbGetQuery(con,"select distinct gcm, scenario, futureperiod from cciss_future12 where siteno = 6476644")
test2 <- dbGetQuery(con,"select distinct gcm, scenario, futureperiod from cciss_future12 where siteno = 1953822")
setDT(test2)
test2[,Type := "Full"]
test[,Type := "Error"]
test2[test, Col := i.Type, on = c("gcm","scenario","futureperiod")]

hexGrid <- st_read(con, query = "select * from hex_grid")
hexGrid <- st_read("HexGrid400m_Sept2021.gpkg")
setDT(hexGrid)
hexGrid[datPred, BGC := i.bgc_pred, on = "siteno"]
hexGrid <- hexGrid[!is.na(BGC),]
hexGrid <- st_as_sf(hexGrid)
st_write(hexGrid,"HexMap.gpkg")

noBGC_grid <- st_read(dsn)
=======
>>>>>>> 4424ddba224637786c159bc002e5b168528e0f28
############


##just so you know what options are available
gcms <- dbGetQuery(con,"select distinct gcm from future_params")[,1]
futureperiods <- dbGetQuery(con,"select distinct futureperiod from future_params")[,1]
rcps <- dbGetQuery(con,"select distinct scenario from future_params")[,1]

### select options
gcm <- "EC-Earth3"
futureperiod <- "2041-2060"
rcp <- "ssp245"

q <- paste0("select siteno,bgc_pred from cciss_future12 where gcm = '",gcm,"' and futureperiod = '", futureperiod,"' and scenario = '",rcp,"'")
tic()
datPred <- setDT(dbGetQuery(con,q))## read from database
toc()

 colnames(hexGrid)[1] <- "siteno"
 tic()
 hexGrid <- as.data.table(hexGrid)
 toc()
 ##convert to data table because join is much faster
 hexGrid[datPred, bgc_pred := i.bgc_pred, on = "siteno"]
hexGrid <- hexGrid %>%  rename(BGC = bgc_pred)
 
 hexGrid.pred <-  st_as_sf(hexGrid)
 
st_write(hexGrid.pred,"./outputs/EC-Earth3_r45_2050.gpkg", append = FALSE)



### this is now old stuff, but some could be reused.
## pull district codes from database
#distcodes <- dbGetQuery(con, "select * from districts")[,1]
distcodes <- dbGetQuery(con, "select * from dist_codes")[,1]

# library(doParallel)
# cl <- makeCluster(detectCores()-2)
# registerDoParallel(cl)
##################################
###future - currently just looping through models, assuming only 1 time periods and scenario. Could change this
gcms <- c("ACCESS1-0","CanESM2","CCSM4","GISS-E2R","HadGEM2-ES",
           "INM-CM4","IPSL-CM5A-MR","MIROC5","MIROC-ESM","MRI-CGCM3","MPI-ESM-LR")#,"CESM1-CAM5","CNRM-CM5","CSIRO-Mk3-6-0","GFDL-CM3"

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
  
  mapClean <- aggregate(provClean[,"geometry"],  by = list(provClean$BGC), FUN = mean) ##union tiles
  colnames(mapClean)[1] <- "BGC"
  
  fname <- paste0("./maps/BCMap_",per,"_",scn,"_",mod,".gpkg")
  st_write(mapClean,fname, append = FALSE)
}
toc()

#######################################
### Current - 91 - 2019
tic()
per <- "Current91"#"Normal61"## 
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

mapClean <- aggregate(provClean[,"geometry"],  by = list(provClean$BGC), FUN = mean) ##union tiles
colnames(mapClean)[1] <- "BGC"

st_write(mapClean,"./maps/BC_1991-2019.gpkg", append = FALSE)
toc()
###############################################################
### Historic - 61-90
tic()
per <- "Normal61"## 
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

mapClean <- aggregate(provClean[,"geometry"],  by = list(provClean$BGC), FUN = mean) ##union tiles
colnames(mapClean)[1] <- "BGC"
st_write(mapClean,"./maps/BC_1961-1990.gpkg", append = FALSE)
toc()

####From build USA_BEC script
require(lwgeom)
##############link predicted Zones to Polygons and write shape file

hexpoly <- st_read(dsn = paste0("./hexpolys/", region, "_bgc_hex400.gpkg"))#, layer = "USA_bgc_hex_800m")
hexpoly$hex_id <- as.character(hexpoly$hex_id)
hexZone <- left_join(hexpoly, X1.pred, by = c("hex_id" = "ID1"))%>% st_transform(3005) %>% st_cast()
temp <- hexZone %>% select(BGC, geom)
temp2 <- st_zm(temp, drop=T, what='ZM') 
##unremark for undissolved layer
#st_write(temp2, dsn = paste0("./outputs/", region, "_", "SubZoneMap_hex400_undissolved.gpkg"), driver = "GPKG", delete_dsn = TRUE)

## unremark for Dissolved
##hexZone <- st_read(dsn = "./outputs/WA_bgc_hex8000_ungrouped.gpkg")#, layer = "USA_bgc_hex_800m") ## need to read it back in to ensure all type Polygon is consistent
temp3 <- hexZone
temp3$BGC <- droplevels(temp3$BGC)
temp3 <-  st_as_sf(temp3)#
st_precision(temp3) <- .5
temp3$BGC <- forcats::fct_explicit_na(temp3$BGC,na_level = "(None)")
temp3 <- temp3[,c("BGC","geom")]
t2 <- aggregate(temp3[,-1], by = list(temp3$BGC), do_union = T, FUN = mean) %>% dplyr::rename(BGC = Group.1)
t2 <- st_zm(t2, drop=T, what='ZM') %>% st_transform(3005) %>% st_cast()

t2 <- t2 %>% st_buffer(0) ## randomly fixes geometry problems
#mapView(t2)
BGC_area <- t2 %>%
  mutate(Area = st_area(.)) %>% mutate (Area = Area/1000000) %>%
  mutate(ID = seq_along(BGC)) %>% dplyr::select(BGC, Area) %>% st_set_geometry(NULL)
#write.csv(BGC_area, paste0("./outputs/", region, "_", timeperiod, "_", covcount, "_BGC_area_predicted_mahbgcreduced.csv"))
st_write(t2, dsn = paste0("./outputs/", covcount, "_", "Subzone_Map_hex400_dissolved_reduced8July21_5.gpkg"), layer = paste0(region, "_", timeperiod, "_", region ,"_", covcount, "_vars_ranger"), driver = "GPKG", delete_layer = TRUE)









