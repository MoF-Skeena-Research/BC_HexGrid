##read in provincial outline, tile, and output for climatebc
##Kiri Daust

library(data.table)
library(sf)
library(foreach)
library(tidyverse)
library(raster)
library(rmapshaper)
library(sp)
vertDist <- function(x){(sideLen(x)*3)/2}
sideLen <- function(x){x/sqrt(3)}

bgc <- st_read(dsn = "~/CommonTables/WNA_BGC_v12_12Oct2020.gpkg")
bgc <- bgc[is.na(bgc$State),c("BGC")]
dem <- raster("./BigDat/BC_25m_DEM_WGS84.tif")
BC <- st_read(dsn = "./BigDat/BC_Province_Outline_Clean4.gpkg")
BC <- ms_simplify(BC, keep = 0.05)
library(mapview)
mapview(BC)
st_write(BC,dsn = "BC_Simplified.gpkg")
st_is_valid(BC)

BC <- st_read("./BigDat/TempBC2.gpkg")
st_crs(BC) <- 3005
grdAll <- st_make_grid(BC,cellsize = 400, square = F, flat_topped = F)
st_write(grdAll, dsn = "TempGrid400m.gpkg")

BC <- st_read("./BC_Simplified.gpkg")
grdAll <- st_read("./BC_Hex400m.gpkg")
grdAll$geom <- grdAll$geom + c(-167,-95)
grdAll$siteno <- 1:nrow(grdAll)
grdAll <- grdAll["siteno"]
st_write(grdAll,dsn = "./BC_HexPoly400m.gpkg", driver = "GPKG")


# temp <- st_make_grid(BC, cellsize = c(100000,vertDist(100000)))
# temp <- st_as_sf(data.frame(ID = 1:length(temp)),geom = temp)
# t1 <- st_intersection(BC,temp)
# ids <- c(8,9,10,11)
# 
# out <- foreach(id = ids, .combine = rbind) %do% {
#   t2 <- t1[t1$ID == id,]
#   grdAll <- st_make_grid(t2,cellsize = 400, square = F, flat_topped = F)
#   grd <- st_as_sf(grdAll)
#   grd$Tile <- id
#   grd
# }
# 
# st_write(out,dsn = "TestTileGrd.gpkg")
# 
# t2 <- t1[t1$ID == 8,]
# bb <- st_as_sfc(st_bbox(BC))
# tic()
# grdAll <- st_make_grid(bb,cellsize = 400, square = F, flat_topped = F)
# toc()
# 
# BC <- as(BC, "Spatial")
# samp <- spsample(BC,type = "hexagonal",cellsize = 400)
# grd <- HexPoints2SpatialPolygons(samp, dx = 400)

grdAll <- st_read(dsn = "./BigDat/HexGrd400m.gpkg")
grdAll <- grdAll["id"]
grdAll$id <- 1:nrow(grdAll)
oldPoints <- st_read("/media/kiridaust/MrBig/BCGrid/HexPts400.gpkg")
colnames(oldPoints)[1] <- "OldID"
st_crs(grdAll) <- 3005
temp <- st_intersects(grdAll,oldPoints,sparse = T)
test <- unlist_sgbp(temp) ##c++ function
crosswalk <- data.table(NewID = grdAll$siteno, OldID = test)
crosswalk[OldID == 0, OldID := NA]
pntsNeeded <- crosswalk[is.na(OldID),NewID]

ptsAll <- st_centroid(grdAll)
st_write(ptsAll, dsn = "./BC_HexPoints400m.gpkg", driver = "GPKG", overwrite = T, append = F)

ptsNew <- ptsAll[ptsAll$siteno %in% pntsNeeded,]

tiles <- st_make_grid(BC, cellsize = c(250000,vertDist(307000)))
plot(tiles)
plot(BC, add = T)

tilesID <- st_as_sf(data.frame(tID = 1:length(tiles)),geom = tiles)
tilesUse <- st_join(tilesID,BC)
tilesUse <- tilesUse[!is.na(tilesUse$State),]
tilesUse <- tilesUse[,"tID"]
library(mapview)
mapview(BC)+
  tilesUse

tilesUse$tID <- 1:nrow(tilesUse)
st_write(tilesUse,dsn = "TileOutlines.gpkg")

datOut <- foreach(tile = tilesUse$tID, .combine = rbind) %do% {
  cat("Processing tile",tile,"... \n")
  testTile <- tilesUse[tile,]
  testGrd <- st_intersection(grdPts, testTile)
  if(nrow(testGrd) > 1){
    grdBGC <- st_join(testGrd,bgc)
    grdBGC$el <- raster::extract(dem, grdBGC)
    grdBGC <- st_transform(grdBGC, 4326)
    out <- cbind(st_drop_geometry(grdBGC),st_coordinates(grdBGC)) %>% as.data.table()
    out <- out[,.(ID1 = ID, ID2 = BGC, lat = Y, long = X, el)]
    out[,TileNum := tile]
    out
  }else{
    NULL
  }

}

dat <- unique(datOut, by = "ID1")
dat <- dat[!is.na(ID2),]
fwrite(dat, "AllHexLocations.csv")
tileID <- dat[,.(ID1,TileNum)]
fwrite(tileID,"TileIDs.csv")

for(i in unique(tileID$TileNum)){
  dat2 <- dat[TileNum == i,]
  dat2[,TileNum := NULL]
  dat2 <- dat2[complete.cases(dat2),]
  fwrite(dat2, paste0("./Output/Tile",i,"_In.csv"), eol = "\r\n")
}

library(climatenaAPI)
library(tictoc)
tileTest <- dat[TileNum == 1,]
tileTest[,TileNum := NULL]
fwrite(temp,"FileforClimBC.csv")

GCMs <- c("ACCESS1-0","CanESM2","CCSM4","CESM1-CAM5","CNRM-CM5","CSIRO-Mk3-6-0","GFDL-CM3","GISS-E2R","HadGEM2-ES",
"INM-CM4","IPSL-CM5A-MR","MIROC5","MIROC-ESM","MRI-CGCM3","MPI-ESM-LR")
rcps <- c("rcp45","rcp85")
pers <- c("2025.gcm","2055.gcm","2085.gcm")
test <- expand.grid(GCMs,rcps,pers)
modNames <- paste(test$Var1,test$Var2,test$Var3, sep = "_")



tic()
tile1_all <- foreach(i = 1:90, .combine = rbind) %do% {
  tile1_out <- climatebc_mult("FileforClimBC.csv",vip = 1,period = modNames[i], ysm = "YS")
  tile1_out$ModName <- modNames[i]
  tile1_out
}
toc()


climBC_JSON <- function(body,period,ysm,url = "http://api6.climatebc.ca/api/clmApi6"){
  colnames(body) <- c("ID1", "ID2", "lat", "lon", "el")
  body$prd <- period
  body$varYSM <- ysm
  nGrp <- floor(nrow(body)/1000) + 1
  for (grp in 0:(nGrp - 1)) {
    body2 <- body[(grp * 1000 + 1):(grp * 1000 + 1000), ]
    out <- sapply(1:1000, function(i) {
      num <- i
      if (num <= nrow(body2)) {
        paste(c(paste0(sprintf("[%d][ID1]", i - 1), "=", 
                       body2$ID1[num]), paste0(sprintf("[%d][ID2]", 
                                                       i - 1), "=", body2$ID2[num]), paste0(sprintf("[%d][lat]", 
                                                                                                    i - 1), "=", body2$lat[num]), paste0(sprintf("[%d][lon]", 
                                                                                                                                                 i - 1), "=", body2$lon[num]), paste0(sprintf("[%d][el]", 
                                                                                                                                                                                              i - 1), "=", body2$el[num]), paste0(sprintf("[%d][prd]", 
                                                                                                                                                                                                                                          i - 1), "=", body2$prd[num]), paste0(sprintf("[%d][varYSM]", 
                                                                                                                                                                                                                                                                                       i - 1), "=", body2$varYSM[num])), collapse = "&")
      }
    })
    out <- paste(out, collapse = "&")
    result <- POST(url = url, body = out, add_headers(`Content-Type` = "application/x-www-form-urlencoded"), 
                   timeout(4e+05))
    output <- fromJSON(rawToChar(result$content))
    head(output)
    dim(output)
    if (grp == 0) {
      cmb <- output
    }
    else {
      cmb <- rbind(cmb, output)
    }
  }
  head(cmb)
  dim(cmb)
  ver = cmb$Version[1]
  ver
  cmb2 <- subset(cmb, select = -c(prd, varYSM, Version))
  cmb3 <- data.frame(ID1 = cmb2[, 1], ID2 = as.factor(cmb2[, 
                                                           2]), lapply(cmb2[, 3:ncol(cmb2)], function(x) as.numeric(as.character(x))))
  print(ver)
  outF <- paste0(gsub(".csv", "", inputFile), "_", gsub(".nrm", 
                                                        "", period), ysm, ".csv")
  outF
}