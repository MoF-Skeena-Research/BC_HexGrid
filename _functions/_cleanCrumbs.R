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
