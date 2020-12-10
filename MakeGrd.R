##read in provincial outline, tile, and output for climatebc
##Kiri Daust

library(data.table)
library(sf)
library(foreach)
library(tidyverse)
library(raster)
library(rmapshaper)

#bgc <- st_read(dsn = "~/CommonTables/WNA_BGC_v12_12Oct2020.gpkg")
bgc <- st_read(dsn = "D:/CommonTables/BGC_maps/WNA_BGC_v12_12Oct2020.gpkg")
bgc <- bgc[is.na(bgc$State),c("BGC")]
#dem <- raster("./BigDat/BC_25m_DEM_WGS84.tif")
dem <- raster("D:/CommonTables/DEMs/BC_25m_DEM_WGS84.tif")
#BC <- st_read(dsn = "./BigDat/BC_Province_Outline_Clean.gpkg")
BC <- st_read(dsn = "D:/CommonTables/BC_AB_US_Shp/BC_Province_Outline_Clean.gpkg")
BC <- st_buffer(BC, dist = 0)
BC <- ms_simplify(BC, keep = 0.2)
grdAll <- st_make_grid(BC,cellsize = 4000, square = F, flat_topped = F)
ptsAll <- st_centroid(grdAll)
grdPts <- st_sf(ID = seq(length(ptsAll)), geometry = ptsAll)
st_write(grdPts, dsn = "./BigDat/HexPts400.gpkg", layer = "HexPts400", driver = "GPKG", overwrite = T, append = F)

grdPoly <- st_sf(ID = seq(length(grdAll)),geometry = grdAll)
st_write(grdPoly, dsn = "./BigDat/HexGrd400", layer = "HexGrd400", driver = "GPKG")

vertDist <- function(x){(sideLen(x)*3)/2}
sideLen <- function(x){x/sqrt(3)}

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

####old code
temp <- st_sf(tiles, ID = seq(65))
t1 <- temp[1,]
t2 <- st_intersection(t1,BC)

tile_width <- st_bbox(tiles[2,])[1] - st_bbox(tiles[1,])[1]

grd1 <- st_make_grid(tiles[1,],cellsize = 400, square = F, flat_topped = F)
grd2 <- st_make_grid(tiles[2,],cellsize = 400, square = F, flat_topped = F)
grd3 <- st_make_grid(tiles[8,],cellsize = 400, square = F, flat_topped = F)

plot(tiles[c(1,2,8),])
plot(grd1, add = T)
plot(grd2, add = T)
plot(grd3, add = T)

test <- st_difference(grd1,st_union(grd2))
plot(tiles[c(1,2,8),])
plot(test, add = T)

grd1 <- st_make_grid(tiles[1,],cellsize = 2000,square = F, flat_topped = T)
grd2 <- st_make_grid(tiles[2,],cellsize = 2000,square = F, flat_topped = T)
grd3 <- st_make_grid(tiles[3,],cellsize = 2000,square = F, flat_topped = T)

out <- st_sf(geom = c(grd1,grd2,grd3),ID = rep(c(1,2,3), c(length(grd1),length(grd2),length(grd3))))
st_write(out, dsn = "TestGrid",layer = "Test4", driver = "ESRI Shapefile",append = T, overwrite = T)

temp <- st_sf(geom = grd2, id = seq(length(grd2)))
plot(temp[1,])
st_area(temp[1,])

#grd2 <- st_make_grid(t2,offset = c(bb[1],bb[2]), cellsize = 1000, square = F)
t3 <- c(grd1,grd2)
plot(t3)
st_write(t3, dsn = "TestGrid",layer = "test1", driver = "ESRI Shapefile",append = F, overwrite = T)
st_write(tiles[1:2,],dsn = "TestGrid",layer = "Tiles", driver = "ESRI Shapefile", append = T)
st_write(grd4, dsn = "TestGrid",layer = "Offset2", driver = "ESRI Shapefile",append = T)

grdAll <- st_make_grid(BC, cellsize = 1000, square = F)

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