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
library(ccissdev)
drv <- dbDriver("PostgreSQL")
con <- dbConnect(drv, user = "postgres", 
                 host = "138.197.168.220",
                 password = "PowerOfBEC", port = 5432, 
                 dbname = "cciss") ### for local use


# tic()
# hexGrid <- st_read(con, query = "select * from hex_grid")
# toc()
tic()
hexGrid <- st_read("F:/OneDrive - Government of BC/CCISSv12/working_files/HexGrid400m_Sept2021.gpkg")
toc()

##just so you know what options are available
gcms <- dbGetQuery(con,"select distinct gcm from future_params")[,1]
gcms
futureperiods <- dbGetQuery(con,"select distinct futureperiod from future_params")[,1]
rcps <- dbGetQuery(con,"select distinct scenario from future_params")[,1]

### select options
gcm <- "MRI-ESM2-0"
futureperiod <- "2041-2060"
rcp <- "ssp245"


q <- paste0("select siteno,bgc_pred from cciss_future12 where gcm = '",gcm,"' and futureperiod = '", futureperiod,"' and scenario = '",rcp,"'")

tic()
datPred <- dbGetQuery(con,q)## read from database
toc()
#datPred2 <- as.data.table(datPred) %>% distinct()
 #colnames(hexGrid)[1] <- "siteno"
 #fwrite(datPred, "./outputs/datPred.csv")
#datPred.missing <- datPred %>% filter(is.na(bgc_pred))  
# tic()
 hexGrid <- as.data.table(hexGrid) #%>% distinct()##convert to data table because join is much faster
# toc()
# hexGrid2 <- hexGrid1 %>% select(siteno, bgc_pred)
# fwrite(hexGrid2, "./outputs/hexGrid.csv")
#hexGrid <- hexGrid1
#hexGrid2 <- left_join(hexGrid,datPred)
#hexGrid3 <- hexGrid2 %>% distinct()
 hexGrid[datPred, bgc_pred := i.bgc_pred, on = "siteno"]
 hexGrid <- hexGrid %>% select(-BGC)
hexGrid <- hexGrid %>%  rename(BGC = bgc_pred)
#hexGrid.missing <- hexGrid %>% filter(is.na(BGC)) 
#misjoint <- anti_join(hexGrid, datPred, by = "siteno")
#misjoint2 <- anti_join(datPred, hexGrid, by = "siteno")
#misjoint <- misjoint %>% filter(is.na(BGC))
 hexGrid.pred <-  st_as_sf(hexGrid)
st_write(hexGrid.pred, "./outputs/MRI-ESM2_rcp45_2050_2.gpkg", append = FALSE, delete_dsn = TRUE)

hexGrid.missing <- hexGrid.pred %>% filter(is.na(BGC)) 
hexGrid.have <- hexGrid.pred %>% filter(!is.na(BGC)) 
hexmiss <- hexGrid.missing$siteno

hexGrid2 <- as.data.table(hexGrid)
hexGrid2 <- hexGrid2[hexGrid2$siteno %in% hexmiss, ]
datPred2 <- datPred[datPred$siteno %in% hexmiss,]

st_write(hexGrid.missing,"./outputs/NoBGChexGrid.gpkg", append = FALSE)

###_____________________________________________________________
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




#_____________

##script to process 4km subsampled data and create feasibility ratings
##adjust gcm weight or rcp weight below
gcm_weight <- data.table(gcm = c("ACCESS-ESM1-5", "BCC-CSM2-MR", "CanESM5", "CNRM-ESM2-1", "EC-Earth3", 
                                 "GFDL-ESM4", "GISS-E2-1-G", "INM-CM5-0", "IPSL-CM6A-LR", "MIROC6", 
                                 "MPI-ESM1-2-HR", "MRI-ESM2-0", "UKESM1-0-LL"),
                         weight = c(1,1,0,1,1,1,1,0,1,1,1,1,0))

rcp_weight <- data.table(rcp = c("ssp126","ssp245","ssp370","ssp585"), 
                         weight = c(0.8,1,0.8,0))

all_weight <- as.data.table(expand.grid(gcm = gcm_weight$gcm,rcp = rcp_weight$rcp))
all_weight[gcm_weight,wgcm := i.weight, on = "gcm"]
all_weight[rcp_weight,wrcp := i.weight, on = "rcp"]
all_weight[,weight := wgcm*wrcp]
modWeights <- all_weight

##function to process data in postgres
dbGetCCISS_4km <- function(con, period = "2041", modWeights){
  
  modWeights[,comb := paste0("('",gcm,"','",rcp,"',",weight,")")]
  weights <- paste(modWeights$comb,collapse = ",")
  groupby = "siteno"
  ##cciss_future is now test_future  
  cciss_sql <- paste0("
  WITH cciss AS (
    SELECT futureperiod,
           siteno,
           bgc,
           bgc_pred,
           w.weight
    FROM pts4km_future12
    JOIN (values ",weights,") 
    AS w(gcm,scenario,weight)
    ON pts4km_future12.gcm = w.gcm AND pts4km_future12.scenario = w.scenario
    WHERE futureperiod IN ('",period,"')
  
  ), cciss_count_den AS (
  
    SELECT ", groupby, " siteref,
           futureperiod,
           SUM(weight) w
    FROM cciss
    GROUP BY ", groupby, ", futureperiod
  
  ), cciss_count_num AS (
  
    SELECT ", groupby, " siteref,
           futureperiod,
           bgc,
           bgc_pred,
           SUM(weight) w
    FROM cciss
    GROUP BY ", groupby, ", futureperiod, bgc, bgc_pred
  
  )
  
  SELECT cast(a.siteref as text) siteref,
         a.futureperiod,
         a.bgc,
         a.bgc_pred,
         a.w/cast(b.w as float) bgc_prop
  FROM cciss_count_num a
  JOIN cciss_count_den b
    ON a.siteref = b.siteref
   AND a.futureperiod = b.futureperiod
   WHERE a.w <> 0
  ")
  
  dat <- setDT(RPostgres::dbGetQuery(con, cciss_sql))
  
  setnames(dat, c("SiteRef","FuturePeriod","BGC","BGC.pred","BGC.prop"))
  dat <- unique(dat) ##should fix database so not necessary
  #print(dat)
  return(dat)
}

##adapted feasibility function
ccissMap <- function(SSPred,suit){
  ### generate raw feasibility ratios
  
  suit <- suit[,.(BGC,SS_NoSpace,Spp,Feasible)]
  suit <- unique(suit)
  suit <- na.omit(suit)
  SSPred <- SSPred[,.(SiteRef,FuturePeriod,BGC,SS_NoSpace,SS.pred,SSprob)]
  Site_BGC <- unique(SSPred[,.(SiteRef,BGC)])
  SSPred <- na.omit(SSPred)
  setkey(SSPred,SS.pred)
  setkey(suit,SS_NoSpace)
  suitMerge <- suit[SSPred, allow.cartesian = T]
  suitMerge <- na.omit(suitMerge)
  setnames(suitMerge, old = c("SS_NoSpace", "i.SS_NoSpace"), new = c("SS.pred", "SS_NoSpace"))
  suitVotes <- data.table::dcast(suitMerge, SiteRef + Spp + FuturePeriod + SS_NoSpace ~ Feasible, 
                                 value.var = "SSprob", fun.aggregate = sum)
  # Fill with 0 if columns does not exist, encountered the error at SiteRef 3104856 
  set(suitVotes, j = as.character(1:5)[!as.character(1:5) %in% names(suitVotes)], value = 0)
  suitVotes[,VoteSum := `1`+`2`+`3`+`4`+`5`]
  suitVotes[,X := 1 - VoteSum]
  suitVotes[,VoteSum := NULL]
  suitVotes[,X := X + `5` + `4`]
  suitVotes[,`:=`(`5` = NULL, `4` = NULL)]
  setkey(suitVotes, SS_NoSpace, Spp)
  setkey(suit, SS_NoSpace, Spp)
  suitVotes[suit, Curr := i.Feasible]
  suitVotes[is.na(Curr), Curr := 5]
  setorder(suitVotes,SiteRef,SS_NoSpace,Spp,FuturePeriod)
  suitVotes[,FuturePeriod := as.integer(FuturePeriod)]
  suitVotes[Curr > 3.5, Curr := 4]
  colNms <- c("1","2","3","X")
  suitVotes <- suitVotes[,lapply(.SD, sum),.SDcols = colNms, 
                         by = .(SiteRef,FuturePeriod, SS_NoSpace,Spp,Curr)]
  suitVotes[,NewSuit := round(`1`+(`2`*2)+(`3`*3)+(X*5))]
  suitVotes <- suitVotes[,.(SiteRef,FuturePeriod,SS_NoSpace,Spp,Curr,NewSuit)]
  return(suitVotes)
}

##pull bgc data
bgc <- dbGetCCISS_4km(con,"2041",all_weight) ##takes about 15 seconds
edaZonal <- E1[grep("01$|h$|00$",SS_NoSpace),]
##edatopic overlap
SSPreds <- edatopicOverlap(bgc,edaZonal,E1_Phase) ##also about 15 seconds
SSPreds <- SSPreds[grep("01$|h$|00$",SS_NoSpace),]
newFeas <- ccissMap(SSPreds,S1) ##ignore warning
## now need to connect to 4km hex map
########################################################################



#con <- dbConnect(drv, user = "postgres", host = "localhost",password = "Kiriliny41", port = 5432, dbname = "cciss_data") ## for local machine
#con <- dbConnect(drv, user = "postgres", host = "smithersresearch.ca",password = "Kiriliny41", port = 5432, dbname = "cciss_data") ### for external use

###read grid
# tic()
# hexGrid <- st_read(con, query = "select * from hex_grid")
# toc()
tic()
hexGrid <- st_read("F:/OneDrive - Government of BC/CCISSv12/working_files/HexGrid400m_Sept2021.gpkg")
toc()
#st_write(hexGrid,"./outputs/basehex.gpkg", append = FALSE)

hexGrid <- st_read("HexGrid400m_Sept2021.gpkg") ##whatever yours is called
datPred <- fread("datPred.csv")

### select options
gcm <- "EC-Earth3"
futureperiod <- "2041-2060"
rcp <- "ssp245"

q <- paste0("select siteno,bgc_pred from cciss_future12 where gcm = '",gcm,"' and futureperiod = '", futureperiod,"' and scenario = '",rcp,"'")

datPred2 <- dbGetQuery(con,q)
# #hexGrid <- st_read("E:/Sync/CCISS_data/SpatialFiles/BC_400mbase_hexgrid/HexGrd400.gpkg") 
# colnames(hexGrid)[1] <- "siteno"
# hexGrid <- as.data.table(hexGrid) ##convert to data table because join is much faster
# hexGrid[datPred, bgc_pred := i.bgc_pred, on = "siteno"]

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

############


