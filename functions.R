# this file contains functions for the simulation of wind power generation from MERRA-2 data
# and also for performing different types of bias correction



# function to calculate power generated in one location without using correction factors
# method: interpolation method to use (1:NN,2:BLI,4:IDW, no BCI because useless)
calcstatpower <- function(method){
  # get data of windparks: capacities and start dates and sort by start dates for each location
  load(paste(dirwindparks,"/windparks_complete.RData",sep=""))
  
  windparks <- data.frame(windparks,comdate=as.POSIXct(paste(windparks$year,"-",windparks$month,"-",windparks$day," 00:00:00",sep=""),tz="UTC"))
  windparks <- windparks[which(windparks$comdate < as.POSIXct("2017-08-31 00:00:00",tz="UTC")),]
  windparks$comdate[which(windparks$comdate<date.start)] <- date.start
  # extract information on wind turbine type, rotor diameter, capacity of wind turbines, and number of installed turbines
  # turbine type
  ind_type <- which(!is.na(windparks$turbines))
  windparks$type <- NA
  windparks$type[ind_type] <- sapply(strsplit(sapply(strsplit(windparks$turbines[ind_type],":"),"[[",2),"[(]"),"[[",1)
  # rotor diameter
  ind_diam <- ind_cap <- which(unlist(lapply(strsplit(windparks$turbines,"power"),length))==2)
  windparks$diam <- NA
  windparks$diam[ind_diam] <- as.numeric(sapply(strsplit(sapply(strsplit(windparks$turbines[ind_diam],"diameter"),"[[",2),"m\\)"),"[[",1))
  # capacity
  windparks$tcap <- NA
  windparks$tcap[ind_diam] <- as.numeric(sapply(strsplit(sapply(strsplit(windparks$turbines[ind_diam],"power"),"[[",2),"kW"),"[[",1))
  # number of turbines
  nturb <- gsub("turbines","",sapply(strsplit(windparks$turbines,":"),"[[",1))
  nturb <- gsub("turbine","",nturb)
  nturb <- as.numeric(gsub("Turbine\\(s\\)","",nturb))
  windparks$n <- nturb
  # fill in missing information with mean number of turbines
  windparks$n[is.na(windparks$n)] <- mean(as.numeric(windparks$n),na.rm=TRUE)
  
  # add specific turbine power (in W, for using Ryberg power curve model)
  # see https://doi.org/10.1016/j.energy.2019.06.052
  windparks$sp <- windparks$tcap*1000/(windparks$diam^2/4*pi)
  # fill in missing information with mean specific power weighted by number of turbines and year
  ind_sp <- which(!is.na(windparks$sp))
  yearly_sp <- aggregate((windparks$sp*windparks$n)[ind_sp],by=list(year(windparks$comdate)[ind_sp]),mean)
  yearly_sp[,2] <- yearly_sp[,2]/aggregate(windparks$n[ind_sp],by=list(year(windparks$comdate)[ind_sp]),mean)[,2]
  windparks$sp[is.na(windparks$sp)] <- yearly_sp[match(year(windparks$comdate),yearly_sp[,1])[is.na(windparks$sp)],2]
  
  # add hypothetical hubheight (is not included in dataset but linear function to estimate it from rotor diamter was fitted from US wind turbine database)
  windparks$hh <- 1.3566*windparks$diam - 18.686
  # fill in missing values with mean hh weighted by number of turbines and year
  ind_hh <- which(!is.na(windparks$hh))
  yearly_hh <- aggregate((windparks$hh*windparks$n)[ind_hh],by=list(year(windparks$comdate)[ind_hh]),mean)
  yearly_hh[,2] <- yearly_hh[,2]/aggregate(windparks$n[ind_hh],by=list(year(windparks$comdate)[ind_hh]),mean)[,2]
  windparks$hh[is.na(windparks$hh)] <- yearly_hh[match(year(windparks$comdate),yearly_hh[,1])[is.na(windparks$hh)],2]
  
  statpowlist <- list()
  
  for(ind in c(1:length(windparks[,1]))){
    pplon <- windparks$long[ind]
    pplat <- windparks$lat[ind]
    # find nearest neightbour MERRA and extrapolate to hubheight
    long <<- pplon
    lat <<- pplat
    lldo <<- distanceorder()
    NNmer <- NNdf(method,windparks$hh[ind])
    # cut-out wind speed: 25 m/s (-> set everything above to 0)
    NNmer[which(NNmer[,2]>25),2] <- 0
    
    # calculate power output for all hours from power curve in kWh
    # values are interpolated linearly betweer points of power curve
    
    # create power curve
    RybCoeff <- read.csv(paste0(ryberg_path,"/ryberg_coeff_sel.csv"),sep=";")
    names(RybCoeff) <- c("CF","A","B")
    v <- mapply(function(A,B) exp(A+B*log(windparks$sp[ind])),
                RybCoeff$A,
                RybCoeff$B)
    # calculate power output
    statpower <- as.data.frame(approx(x=c(0,v,100),y=c(0,RybCoeff$CF/100,1)*windparks$cap[ind],xout=NNmer$vext))
    # set production before commissioning 0
    statpower$y[which(NNmer$date<windparks$comdate[ind])] <- 0
    
    # add to results
    statpowlist[[ind]] <- data.frame(NNmer[,1],statpower$y)
  }
  
  return(statpowlist)
}




# function to calculate power generated in one location without using correction factors
# method: interpolation method to use (1:NN,2:BLI,4:IDW, no BCI because useless)
calcstatpower_old <- function(method,type="E82"){
  # get data of windparks: capacities and start dates and sort by start dates for each location
  load(paste(dirwindparks,"/windparks_complete.RData",sep=""))
  
  windparks <- data.frame(windparks,comdate=as.POSIXct(paste(windparks$year,"-",windparks$month,"-",windparks$day," 00:00:00",sep=""),tz="UTC"))
  windparks <- windparks[which(windparks$comdate < as.POSIXct("2017-08-31 00:00:00",tz="UTC")),]
  windparks$comdate[which(windparks$comdate<date.start)] <- date.start
  
  
  statpowlist <- list()
  
  for(ind in c(1:length(windparks[,1]))){
    pplon <- windparks$long[ind]
    pplat <- windparks$lat[ind]
    # find nearest neightbour MERRA and extrapolate to hubheight
    long <<- pplon
    lat <<- pplat
    lldo <<- distanceorder()
    NNmer <- NNdf(method,108)
    
    # calculate power output for all hours from power curve in kWh
    # values are interpolated linearly betweer points of power curve
    
    # vary wind turbine type
    if(type == "E82"){
      # Enercon E82
      ratedpower <- 2000
      windspeed <- c(0:25,100)
      powercurve <- c(0,0,3,25,82,174,312,532,815,1180,1580,1810,1980,rep(2050,13),2050)
    }else if(type=="SWT2.3"){
      # Siemens SWT 2.3
      ratedpower <- 2300
      windspeed <- c(0:25,100)
      powercurve <- c(0,0,0,0,98,210,376,608,914,1312,1784,2164,2284,2299,rep(2300,12))
    }else if(type=="V80"){
      # Vestas V80
      ratedpower <- 1800
      windspeed <- c(0:25,100)
      powercurve <- c(0,0,0,0,2,97,255,459,726,1004,1330,1627,1772,1797,rep(1082,10),rep(1080,3))
    }else{
      print("wrong turbine type")
      return(NULL)
    }
    
    # calculate power output
    statpower <- as.data.frame(approx(x=windspeed,y=powercurve*windparks$cap[ind]/ratedpower,xout=NNmer$vext))
    # set production before commissioning 0
    statpower$y[which(NNmer$date<windparks$comdate[ind])] <- 0
    
    # add to results
    statpowlist[[ind]] <- data.frame(NNmer[,1],statpower$y)
  }
  
  return(statpowlist)
}











#function for calculating distances and order by distances for MERRA
distanceorder <- function(){
  distance <- 6378.388*acos(sin(rad*lat) * sin(rad*LonLat$lat) + cos(rad*lat) * cos(rad*LonLat$lat) * cos(rad*LonLat$long-rad*long))
  lonlatdistao <- data.frame(LonLat,distance,c(1:length(distance)))
  names(lonlatdistao) <- c("Longitude","Latitude","distance","MRRnum")
  lonlatdistao <- lonlatdistao[order(lonlatdistao[,3]),]
  return(lonlatdistao)
}



# Interpolation of MERRA wind speeds
# returns a dataframe with nearest neighbour time, wind speeds and disph
# method refers to the method of interpolation:
# 1... nearest neighbour interpolation
# 2... bilinear interpolation
# 3... bicubic interpolation
# 4... inverse weighting of distances
NNdf <- function(method,hubheight=10){
  setwd(dirmerra)
  switch(method,
         
         {
           ########## 1. Nearest Neighbour ##########
           # first row (nearest neighbour) is extracted
           # first column of list is taken (distance to station)
           # and columns 4 to 27, long and lat are excluded
           MRRdf <- getMerraPoint(lldo$Longitude[1],lldo$Latitude[1])
           
           # Wind speeds Nearest Neighbor
           WHuv50 <- sqrt(MRRdf$U50M^2+MRRdf$V50M^2)
           WHuv10 <- sqrt(MRRdf$U10M^2+MRRdf$V10M^2)
           
           # WH MERRA-DF
           MWH <- data.frame(MRRdf$MerraDate,WHuv50,WHuv10,MRRdf$DISPH)
           MWH1 <- MWH[which(MWH[,1]>=date.start),]
           
           MWH1ext <-data.frame(MWH1[,1],extrap(MWH1,hubheight))
           names(MWH1ext) <- c("date","vext")
           
           return(MWH1ext)
         },
         
         
         {
           ########## 2. Bilinear Interpolation ##########
           # first row (nearest neighbour) is extracted
           # three other neighbours in square around station are searched
           # in case point lies between two points (on lon or lat line) only 2 points are used for calculation
           # in case point lies exactly on Merra point, Nearest Neighbour method is used instead
           MRRdfs <- list()
           WHuv50 <- list()
           WHuv10 <- list()
           WHuvext <- list()
           #find coordinates of square points around station
           lonNN <- lldo[1,1]
           latNN <- lldo[1,2]
           if((lonNN==long)&&(latNN==lat)){
             ################################
             # in case station is on MERRA point
             ################################
             #print(paste("Using NN Method for station",statn))
             return(NNdf(1,hubheight))
             
           }else if(lonNN==long){
             ################################
             # in case station is on lon line
             ################################
             #print(paste("For station",statn,"interpolation only between lats"))
             
             if(latNN < lat){
               lat1 <- latNN
               lat2 <- latNN+0.5
             }else{
               lat2 <- latNN
               lat1 <- latNN-0.5
             }
             
             lats <- NULL
             lons <- lonNN
             lats[1] <- lldo$Latitude[first(which(as.numeric(as.factor(abs(lldo$Latitude-lat1)))==1))]
             lats[2] <- lldo$Latitude[first(which(as.numeric(as.factor(abs(lldo$Latitude-lat2)))==1))]
             
             setwd(dirmerra)
             MRRdfs[[1]] <- getMerraPoint(lons[1],lats[1])
             MRRdfs[[2]] <- getMerraPoint(lons[2],lats[1])
             
             # calculation of coefficients
             coeff1 <- (lats[2]-lat)/(lats[2]-lats[1])
             coeff2 <- (lat-lats[1])/(lats[2]-lats[1])
             
             # wind speeds square
             for(i in c(1:2)){
               WHuv10[[i]] <- sqrt(MRRdfs[[i]]$U10M^2 + MRRdfs[[i]]$V10M^2)
               WHuv50[[i]] <- sqrt(MRRdfs[[i]]$U50M^2 + MRRdfs[[i]]$V50M^2)
             }
             
             # extrapolation
             for(i in c(1:2)){
               WHuvext[[i]] <- extrap(data.frame(MRRdfs[[i]]$MerraDate,WHuv50[[i]],WHuv10[[i]],MRRdfs[[i]]$DISPH),hubheight)
             }
             
             #interpolation
             UVBLIext <- coeff1*WHuvext[[1]]+coeff2*WHuvext[[2]]
             
             
           }else if(latNN==lat){
             ################################
             # in case station is on lat line
             ################################
             #print(paste("For station",statn,"interpolation only between lons"))
             
             if(lonNN < long){
               lon1 <- lonNN
               lon2 <- lonNN+0.625
             }else{
               lon2 <- lonNN
               lon1 <- lonNN-0.625
             }
             
             lats <- latNN
             lons <- NULL
             lons[1] <- lldo$Longitude[first(which(as.numeric(as.factor(abs(lldo$Longitude-lon1)))==1))]
             lons[2] <- lldo$Longitude[first(which(as.numeric(as.factor(abs(lldo$Longitude-lon2)))==1))]
             
             setwd(dirmerra)
             MRRdfs[[1]] <- getMerraPoint(lons[1],lats[1])
             MRRdfs[[2]] <- getMerraPoint(lons[2],lats[1])
             
             # calculation of coefficients
             coeff1 <- (lons[2]-long)/(lons[2]-lons[1])
             coeff2 <- (long-lons[1])/(lons[2]-lons[1])
             
             # wind speeds square
             for(i in c(1:2)){
               WHuv10[[i]] <- sqrt(MRRdfs[[i]]$U10M^2 + MRRdfs[[i]]$V10M^2)
               WHuv50[[i]] <- sqrt(MRRdfs[[i]]$U50M^2 + MRRdfs[[i]]$V50M^2)
             }
             
             # extrapolation
             for(i in c(1:2)){
               WHuvext[[i]] <- extrap(data.frame(MRRdfs[[i]]$MerraDate,WHuv50[[i]],WHuv10[[i]],MRRdfs[[i]]$DISPH),hubheight)
             }
             
             #interpolation
             UVBLIext <- coeff1*WHuvext[[1]]+coeff2*WHuvext[[2]]
             
           }else{
             ################################
             # in case station is inside square
             ################################
             if(lonNN < long){
               lon1 <- lonNN
               lon2 <- lonNN+0.625
             }else{
               lon2 <- lonNN
               lon1 <- lonNN-0.625
             }
             if(latNN < lat){
               lat1 <- latNN
               lat2 <- latNN+0.5
             }else{
               lat2 <- latNN
               lat1 <- latNN-0.5
             }
             
             # get coordinates from LonLat because numeric problems...
             lats <- NULL
             lons <- NULL
             lats[1] <- lldo$Latitude[first(which(as.numeric(as.factor(abs(lldo$Latitude-lat1)))==1))]
             lats[2] <- lldo$Latitude[first(which(as.numeric(as.factor(abs(lldo$Latitude-lat2)))==1))]
             lons[1] <- lldo$Longitude[first(which(as.numeric(as.factor(abs(lldo$Longitude-lon1)))==1))]
             lons[2] <- lldo$Longitude[first(which(as.numeric(as.factor(abs(lldo$Longitude-lon2)))==1))]
             
             setwd(dirmerra)
             MRRdfs[[1]] <- getMerraPoint(lons[1],lats[1])
             MRRdfs[[2]] <- getMerraPoint(lons[1],lats[2])
             MRRdfs[[3]] <- getMerraPoint(lons[2],lats[1])
             MRRdfs[[4]] <- getMerraPoint(lons[2],lats[2])
             
             #calculation of coefficients for bilinear interpolation
             coeff1 <- (lons[2]-long)/(lons[2]-lons[1])*(lats[2]-lat)/(lats[2]-lats[1])
             coeff2 <- (lons[2]-long)/(lons[2]-lons[1])*(lat-lats[1])/(lats[2]-lats[1])
             coeff3 <- (long-lons[1])/(lons[2]-lons[1])*(lats[2]-lat)/(lats[2]-lats[1])
             coeff4 <- (long-lons[1])/(lons[2]-lons[1])*(lat-lats[1])/(lats[2]-lats[1])
             
             # wind speeds square
             for(i in c(1:4)){
               WHuv10[[i]] <- sqrt(MRRdfs[[i]]$U10M^2 + MRRdfs[[i]]$V10M^2)
               WHuv50[[i]] <- sqrt(MRRdfs[[i]]$U50M^2 + MRRdfs[[i]]$V50M^2)
             }
             
             # extrapolation
             for(i in c(1:4)){
               WHuvext[[i]] <- extrap(data.frame(MRRdfs[[i]]$MerraDate,WHuv50[[i]],WHuv10[[i]],MRRdfs[[i]]$DISPH),hubheight)
             }
             
             #interpolation
             UVBLIext <- coeff1*WHuvext[[1]]+coeff2*WHuvext[[2]]+coeff3*WHuvext[[3]]+coeff4*WHuvext[[4]]
             
           }
           
           # WH MERRA-DF
           MWH <- data.frame(MRRdfs[[1]]$MerraDate,UVBLIext)
           MWH1 <- MWH[which(MWH[,1]>=date.start),]
           names(MWH1) <- c("date","vext")
           
           return(MWH1)
           
         },
         
         {
           ########## 3. Bicubic Interpolation ##########
           # dismissed
         },
         {
           ########## 4. Inverse Distance Weighting ##########
           # first four rows (4 nearest neighbours) are extracted
           # the inverse distances are calculated
           # new velocities are found by weighting by the inverse distances
           
           
           MRRdfs <- list()
           WHuv50 <- list()
           WHuv10 <- list()
           for(i in c(1:4)){
             MRRdfs[[i]] <- getMerraPoint(lldo$Longitude[i],lldo$Latitude[i])
           }
           # Wind speeds 4 Neighbors Square
           for(i in c(1:4)){
             WHuv50[[i]] <- sqrt(MRRdfs[[i]]$U50M^2+MRRdfs[[i]]$V50M^2)
             WHuv10[[i]] <- sqrt(MRRdfs[[i]]$U10M^2+MRRdfs[[i]]$V10M^2)
           }
           
           # extrapolation
           WHext <- list()
           for(i in c(1:4)){
             WHext[[i]] <- extrap(data.frame(MRRdfs[[1]]$MerraDate,WHuv50[[i]],WHuv10[[i]],MRRdfs[[i]]$DISPH),hubheight)
           }
           WHextdf <- as.data.frame(WHext)
           names(WHextdf) <-c("v1","v2","v3","v4")
           #calculation of coefficients for inverse distance weighting and interpolation
           IDWcoeffs <- list()
           distas <- c(lldo$distance[1],lldo$distance[2],lldo$distance[3],lldo$distance[4])
           invdistasum <- sum(1/distas[1],1/distas[2],1/distas[3],1/distas[4])
           for(i in c(1:4)){
             IDWcoeffs[[i]] <- (1/(distas[i]))/(invdistasum)
           }
           coeffdf <- data.frame(matrix(rep(unlist(IDWcoeffs),each=length(WHextdf[,1])),nrow=length(WHextdf[,1]),ncol=4))
           WHextIDW1 <- WHextdf*coeffdf
           WHextIDW <- WHextIDW1[,1]+WHextIDW1[,2]+WHextIDW1[,3]+WHextIDW1[,4]
           
           MWH <-data.frame(MRRdfs[[1]]$MerraDate,WHextIDW)
           MWH1 <- MWH[which(MWH[,1]>=date.start),]
           names(MWH1) <- c("date","vext")
           
           
           return(MWH1)
           
         })
}


#Extrapolation height
#Height of INMET data: 10m
extrap <- function(MWH1,hIN=10){
  #alpha friction coefficient
  alpha <- (log(MWH1[,2])-log(MWH1[,3]))/(log(50)-log(10+MWH1[,4]))
  #wind speed with power law
  vext <- MWH1[,2]*(hIN/50)^alpha
  return(vext)
}





# function to sum up power generation per state
# spl statpowlist
makeSTATEpowlist <- function(spl){
  load(paste0(dirwindparks,"/windparks_complete.RData"))
  windparks <- data.frame(windparks,comdate=as.POSIXct(paste(windparks$year,"-",windparks$month,"-",windparks$day," 00:00:00",sep=""),tz="UTC"))
  windparks <- windparks[which(windparks$comdate < as.POSIXct("2017-08-31 00:00:00",tz="UTC")),]
  windparks$comdate[which(windparks$comdate<date.start)] <- date.start
  
  states <- unique(windparks$state)
  states<- states[order(states)]
 
  STATEpowlist <- list()
  for(i in c(1:length(states))){
    print(states[i])
    statepow <- NULL
    for(j in c(which(windparks$state==states[i]))){
      if(length(statepow>0)){statepow[,2]=statepow[,2]+spl[[j]][,2]}else{statepow=spl[[j]]}
    }
    STATEpowlist[[i]] <- statepow
  }
  names(STATEpowlist) <- states
  return(STATEpowlist)
}


# function to sum up power generation per selected station
# spl statpowlist
makeWPpowlist <- function(spl){
  load(paste0(dirwindparks,"/windparks_complete.RData"))
  windparks <- data.frame(windparks,comdate=as.POSIXct(paste(windparks$year,"-",windparks$month,"-",windparks$day," 00:00:00",sep=""),tz="UTC"))
  windparks <- windparks[which(windparks$comdate < as.POSIXct("2017-08-31 00:00:00",tz="UTC")),]
  windparks$comdate[which(windparks$comdate<date.start)] <- date.start
  
  load(paste0(dirwindparks_sel,"/selected_windparks.RData"))
  states <- unique(sel_windparks$state)
  
  ind <- match(sel_windparks$name,windparks$name)
  
  
  statpowlist <- list()
  for(i in c(1:length(states))){
    print(states[i])
    statpow <- NULL
    for(j in c(ind[which(sel_windparks$state==states[i])])){
      if(length(statpow>0)){statpow[,2]=statpow[,2]+spl[[j]][,2]}else{statpow=spl[[j]]}
    }
    statpowlist[[i]] <- statpow
  }
  names(statpowlist) <- c("BA","CE","PE","PI","RN","RS","SC")
  return(statpowlist)
}



# make sum of wind power generation for all of brazil
sum_brasil <- function(complist){
  df <- complist[[1]]
  for(i in c(2:length(complist))){
    df[,2] <- df[,2]+complist[[i]][,2]
  }
  return(df)
}

# make sum of wind power generation for subsystems NE and S
sum_subsystem <- function(complist){
  subs <- data.frame(states=c("Bahia","Ceará","Maranhão","Minas Gerais","Paraíba","Paraná","Pernambuco","Piaui","RiodeJaneiro","RioGrandedoNorte","RioGrandedoSul","SantaCatarina","Sergipe"),subsystem=c("NE","NE","NE","SE","NE","S","NE","NE","SE","NE","S","S","NE"))
  dfNE <- NULL
  dfS <- NULL
  for(i in which(subs[,2]=="NE")){
    if(length(dfNE)>0){
      dfNE[,2] <- dfNE[,2]+complist[[i]][,2]
    }else{
      dfNE <- complist[[i]]
    }
  }
  for(i in which(subs[,2]=="S")){
    if(length(dfS)>0){
      dfS[,2] <- dfS[,2]+complist[[i]][,2]
    }else{
      dfS <- complist[[i]]
    }
  }
  dflist <- list(dfNE,dfS)
  names(dflist) <- c("NE","S")
  return(dflist)
}

# daily aggregate wind power generation
dailyaggregate <- function(statepowlist){
  splagd <- list()
  for(i in c(1:length(statepowlist))){
    days <- format(statepowlist[[1]][,1],"%Y%m%d")
    listnew <- aggregate(statepowlist[[i]][,2],by=list(days),sum)
    # insert dates in datetime format
    listnew[,1] <- as.POSIXct(paste(substr(listnew[,1],1,4),substr(listnew[,1],5,6),substr(listnew[,1],7,8),sep="-"),tz="UTC")
    names(listnew) <- c("time","wp")
    splagd[[i]] <- listnew
  }
  
  return(splagd)
}

# function for loading measured wind power for subsystems or brazil
getprodSUBBRA <- function(area){
  a <- data.frame(a=c("NE","S","BRASIL"),b=c("nordeste","sul","brasil"))
  prod <- read.table(paste(dirwindprodsubbra,"/",a$b[match(area,a$a)],"_dia.csv",sep=""),sep=";",header=T,stringsAsFactors=F,dec=",")
  # extract date and generation in GWh
  prod <- data.frame(date=as.POSIXct(paste(substr(prod[,1],7,10),substr(prod[,1],4,5),substr(prod[,1],1,2),sep="-")[-1],tz="UTC"),prod_GWh=prod[-1,8])
  return(prod)
}

# function for loading measured wind power for states
getSTATEproddaily <- function(state){
  states <- gsub(".csv","",list.files(path=dirwindproddaily))
  if(!is.na(match(state,states))){
    STATEprod <- read.table(paste(dirwindproddaily,"/",states[match(state,states)],".csv",sep=""),sep=";",header=T,stringsAsFactors=F,dec=',')
    # first row is useless
    STATEprod <- STATEprod[2:length(STATEprod[,1]),]
    # extract yearmonth and generation in GWh
    STATEprod <- data.frame(date=as.POSIXct(paste(substr(STATEprod[,1],7,10),substr(STATEprod[,1],4,5),substr(STATEprod[,1],1,2),sep="-"),tz="UTC"),prod_GWh=STATEprod[,8])
    return(STATEprod)
  }else{
    return(NULL)
  }
}


getstatproddaily <- function(state){
  files <- list.files(path=dirwindparks_sel,".csv")
  states <- substr(files,1,2)
  if(!is.na(match(state,states))){
    STATEprod <- read.table(paste(dirwindparks_sel,"/",files[match(state,states)],sep=""),sep=";",header=T,stringsAsFactors=F,dec=',')
    # sometimes first row is useless
    if(STATEprod[1,1]==""){
      STATEprod <- STATEprod[2:length(STATEprod[,1]),]
    }
    # extract yearmonth and generation in GWh
    STATEprod <- data.frame(date=as.POSIXct(paste(substr(STATEprod[,1],7,10),substr(STATEprod[,1],4,5),substr(STATEprod[,1],1,2),sep="-"),tz="UTC"),prod_GWh=STATEprod[,8])
    return(STATEprod)
  }else{
    return(NULL)
  }
}

# function that cuts two data frames to same length
# data frames with two columns, first column has dates, second has data
csl <- function(df1,df2){
  cut1 <- max(df1[1,1],df2[1,1])
  cut2 <- min(df1[nrow(df1),1],df2[nrow(df2),1])
  df1.1 <- df1[which(df1[,1]==cut1):which(df1[,1]==cut2),]
  df2.1 <- df2[which(df2[,1]==cut1):which(df2[,1]==cut2),]
  df <- data.frame(df1.1[,1],df1.1[,2],df2.1[,2])
  return(df)
}


# statn is number of station
# function reads INMET data into a dataframe
# long and lat are saved too
readINMET <- function(statn,startdate){
  stations <- read.table(paste(dirinmetmeta,"/stations_meta_data.csv",sep=""),sep=";",header=T,stringsAsFactors=F)
  # remove first and last two because they are not in Brazil
  stations <- stations[2:(length(stations[,1])-2),]
  final1 <- read.csv2(paste(dirinmet,"/",stations$name[statn],".csv",sep=""))
  # shift by 12 hours and convert to same time zone as MERRA data (UTC)
  dates.num <- format(final1[,1],tz="UTC")
  dates<-as.POSIXct(as.numeric(dates.num)-12*3600,tz="UTC",origin="1970-01-01")
  dates1<-dates[which(as.POSIXct(dates, tz="UTC")>=startdate)]
  wind<-final1[,9]
  wind1<-wind[which(as.POSIXct(dates, tz="UTC")>=startdate)]
  suppressWarnings(final_data<-data.frame(dates1,as.numeric(paste(wind1))))
  # remove last row because from next day
  final_data<-final_data[1:(length(final_data$dates1)-1),1:2]
  names(final_data) <- c("dates1","wind1")
  
  long<<-stations$lon[statn]
  lat<<-stations$lat[statn]
  return(final_data)
}




# function which removes rows of at least len same entries occuring in col column in dataframe x 
# used to prepare INMET data because there are erroneous sequences
rmrows <- function(x,len,col){
  lengths <- data.frame(num=rle(x[,col])$lengths,cum=cumsum(rle(x[,col])$lengths))
  lengths <- rbind(c(0,0),lengths)
  
  
  whichs <- which(lengths$num>=len)
  if(length(whichs)>0){
    rowrm <- NULL
    for(i in c(1:length(whichs))){
      rowrm <- c(rowrm,c((lengths[whichs[i]-1,2]+1):(lengths[whichs[i],2])))
    }
    y <- x[-c(rowrm),]
  }else{
    y=x
  }
  
  return(y)
}


# function to calculate power generated in one location with mean wind speed approximation
# parameters: rated power and height of used wind turbine as well as data for its power curve
# method: interpolation method to use (1:NN,2:BLI,4:IDW, no BCI because useless)
# selection: which wind parks to use? all or only selected (largest of each state)? ("all" or "sel")
# wscdata: which data to use for wind speed correction? ("INMET" or "WINDATLAS")
calcstatpower_meanAPT <- function(ratedpower,height,windspeed,powercurve,method=1,selection="all",wscdata,applylim=1){
  # get data of windparks: capacities and start dates and sort by start dates for each location
  if(selection=="all"){
    load(paste(dirwindparks,"/windparks_complete.RData",sep=""))
  }else{
    load(paste(dirwindparks_sel,"/selected_windparks.RData",sep=""))
    windparks=sel_windparks
    rm(sel_windparks)
  }
  windparks <- windparks[order(windparks$long),]
  windparks <- data.frame(windparks,comdate=as.POSIXct(paste(windparks$year,"-",windparks$month,"-",windparks$day," 00:00:00",sep=""),tz="UTC"))
  windparks <- windparks[which(windparks$comdate < as.POSIXct("2017-08-31 00:00:00",tz="UTC")),]
  windparks$comdate[which(windparks$comdate<date.start)] <- date.start
  numlon <- as.vector(unlist(rle(windparks$long)[1]))
  # counter for locations
  pp <- 1
  statpowlist <- list()
  powlistind <- 1
  # list for saving correction factors
  cfs_mean <<-list()
  while(pp<=length(windparks$long)){
    print(powlistind)
    numstat <- numlon[powlistind]
    pplon <- windparks$long[pp]
    pplat <- windparks$lat[pp]
    # find nearest neightbour MERRA and extrapolate to hubheight
    long <<- pplon
    lat <<- pplat
    lldo <<- distanceorder()
    NNmer <- NNdf(method,height)
    if(wscdata=="INMET"){
      
      ##########################################################
      ##### INMET wind speed correction ########################
      ##########################################################
      
      stations<-read.table(paste(dirinmetmeta,"/stations_meta_data.csv",sep=""),sep=";",header=T,stringsAsFactors=F)
      # remove first and last two because they are not in Brasil
      stations <- stations[2:(length(stations[,1])-2),]
      # extract longitudes and latitudes
      statlons <- stations$lon
      statlats <- stations$lat
      rm(stations)
      ppINdistance <- 6378.388*acos(sin(rad*pplat) * sin(rad*statlats) + cos(rad*pplat) * cos(rad*statlats) * cos(rad*statlons-rad*pplon))
      # only use if within maximum distance
      if((min(ppINdistance)<INmaxdist)|applylim==0){
        # find data of nearest station
        statn <- which(ppINdistance==min(ppINdistance))
        final_data <- readINMET(statn,date.start)
        # get wind speed data of nearest MERRA point (global long and lat variables have changed in readINMET function)
        lldo <<- distanceorder()
        windMER10m <- NNdf(method,10)
        wind_df <- data.frame(final_data$dates1,final_data$wind1,windMER10m[1:length(final_data$wind1),2])
        wind_df <- na.omit(wind_df)
        wind_df_r <- rmrows(wind_df,120,2)
        cf <- mean(wind_df_r[,2])/mean(wind_df_r[,3])
        cfs_mean[[powlistind]] <<- cf
      }else{
        cf <- 1
        cfs_mean[[powlistind]] <<- 1
      }
      # adapt mean wind speed
      NNmer[,2] <- NNmer[,2]*cf
    }else{
      
      ##########################################################
      ##### WIND ATLAS wind speed correction ###################
      ##########################################################
      
      load(paste(dirwindatlas,"/wind_atlas.RData",sep=""))
      ppWAdistance <- 6378.388*acos(sin(rad*pplat) * sin(rad*windatlas[,2]) + cos(rad*pplat) * cos(rad*windatlas[,2]) * cos(rad*windatlas[,1]-rad*pplon))

      # find data of nearest station
      pointn <- which(ppWAdistance==min(ppWAdistance))
      long <<- windatlas[pointn,1]
      lat <<- windatlas[pointn,2]
      # get wind speed data of nearest MERRA point (at 50m height! as wind atlas data)
      lldo <<- distanceorder()
      windMER50m <- NNdf(method,50)
      cf <- as.numeric(windatlas[pointn,3])/mean(windMER50m[,2])
      cfs_mean[[powlistind]] <<- cf
      # adapt mean wind speed
      NNmer[,2] <- NNmer[,2]*cf
    }

    # get startdates and capacities from municipios
    capdate <- data.frame(windparks$comdate[pp:(pp+numstat-1)],windparks$cap[pp:(pp+numstat-1)],rep(NA,numstat))
    names(capdate) <- c("commissioning","capacity","capacitysum")
    capdate <- capdate[order(capdate$commissioning),]
    capdate$capacitysum <- cumsum(capdate$capacity)
    # make a list of capacities for all dates
    caplist <- data.frame(NNmer[,1],rep(0,length(NNmer[,1])))
    match <- match(capdate$commissioning,caplist[,1])
    for(i in c(1:length(match))){
      caplist[match[i]:length(caplist[,1]),2] <- capdate$capacitysum[i]
    }
    
    
    # calculate power output for all hours from power curve in kWh
    # values are interpolated linearly betweer points of power curve
    whichs <- findInterval(NNmer[,2],windspeed)
    whichs.1 <- whichs+1
    whichs.1[which(whichs.1>length(powercurve))] <- length(powercurve)
    statpower <- caplist[,2]/ratedpower*((powercurve[whichs]-powercurve[whichs.1])/(windspeed[whichs]-windspeed[whichs.1])*(NNmer[,2]-windspeed[whichs.1])+powercurve[whichs.1])
    # replace NAs created where which is last element of powercurve with power of last element
    # because powercurve becomes flat and no higher power is generated
    statpower[which(whichs==length(powercurve))] <- caplist[which(whichs==length(powercurve)),2]/ratedpower*powercurve[length(powercurve)]
    
    statpowlist[[powlistind]] <- data.frame(NNmer[,1],statpower)
    
    pp <- pp + numstat
    powlistind <- powlistind +1
    
  }
  
  return(statpowlist)
}

# monthly correction
# function is given data frame with dates, INMET and MERRA wind speeds
# monthly sums of INMET and MERRA wind speeds are calculated
# missing data are removed in both datasets
# 12 monthly correction factors are calculated
# function returns list of [1] correlation of corrected MERRa data with INMET data
# and [2] correction factors with columns (1) month (2) cf
# in case wind speeds for one month are missing in the dataset, the correction factor results in 1
corrm <- function(wind_df){
  # extract month from date
  listmon=month(wind_df[,1])
  wind_df<-data.frame(listmon,wind_df[,2:3])
  names(wind_df)<-c("month","windIN","windMER")
  agINm<-aggregate(wind_df$windIN,by=list(wind_df$month),sum)
  agMERm<-aggregate(wind_df$windMER,by=list(wind_df$month),sum)
  cfm <- data.frame(c(1:12),1)
  cfm[match(agINm[,1],cfm[,1]),2] <- as.vector(unlist(agINm[2]/agMERm[2]))
  names(cfm)<-c("month","cf")
  
  #monthly corrected Wind MERRA
  cwindMERm <- wind_df$windMER*cfm[wind_df$month,2]
  corm <- cor(wind_df$windIN,cwindMERm)
  
  return(list(corm,cfm))
}



# hourly and monthly correction
# function is given data frame with dates, INMET and MERRA wind speeds
# monthly and hourly sums of INMET and MERRA wind speeds are calculated
# missing data are removed in both datasets
# 12*24 monthly and hourly correction factors are calculated
# function returns list of [1] correlation of corrected MERRa data with INMET data
# and [2] correction factors with columns (1) month (2) cf
# in case wind speeds for one hour or month are missing in the dataset, the correction factor results in 1
corrhm <- function(wind_df){
  gc()
  listh <- hour(wind_df[,1])
  listmon <- month(wind_df[,1])
  listhm <- format(wind_df[,1],"%m%H")
  
  wind_df <- data.frame(listh,listmon,listhm,wind_df[,2:3])
  names(wind_df) <- c("hour","month","monthhour","windIN","windMER")
  agINmh<-aggregate(wind_df$windIN,by=list(wind_df$monthhour),sum)[,2]
  agMERmh<-aggregate(wind_df$windMER,by=list(wind_df$monthhour),sum)[,2]
  mh <- aggregate(wind_df$windIN,by=list(wind_df$monthhour),sum)[,1]
  listcfmh <- agINmh/agMERmh
  m <- rep(c("01","02","03","04","05","06","07","08","09","10","11","12"),each=24)
  h <- rep(c("00","01","02","03","04","05","06","07","08","09","10","11","12","13","14","15","16","17","18","19","20","21","22","23"),12)
  mh1 <- NULL
  for(i in c(1:288)){mh1[i] <- paste(m[i],h[i],sep="")}
  dfcfmh <- data.frame(mh1,1)
  dfcfmh[match(mh,dfcfmh$mh1),2] <- listcfmh
  
  cfmh <- as.data.frame(matrix(dfcfmh[,2],nrow=24,ncol=12))
  names(cfmh) <- c(1:12)
  # replaced loop with faster method
  listhm <- data.frame(listh+1,listmon)
  listcf <- cfmh[as.matrix(listhm)]
  cwindMERmh <- wind_df$windMER*listcf
  cormh <- cor(wind_df$windIN,cwindMERmh)
  
  return(list(cormh,cfmh))
}

# function for calculating correction factors for all stations
# mean correction and removal of long time series of same values added
# int.method determines the method for interpolation
# 1 ... Nearest Neighbour
# 2 ... Bilinear Interpolation
# 3 ... Bicubic Interpolation
# 4 ... Inverse Distance weighting
calccfs_r <- function(int.method=1){
  setwd(dirinmetmeta)
  stations1<-read.table("stations_meta_data.csv",sep=";",header=T,stringsAsFactors=F)
  # remove first and last two because they are not in Brasil
  stations1 <- stations1[2:(length(stations1[,1])-2),]
  # remove stations that have time series with poor quality
  load(paste(dirresults,"/rmdf_",mindaynum,"_",minmonth,"_",monthlim,".RData",sep=""),envir=.GlobalEnv)
  stations_r <- stations1[rmdf$statn[which(rmdf$rr<=monthlim)],]
  # correlation lists
  corm_r <<- list()
  corhm_r <<- list()
  # correction factor lists
  cfm_r <<- list()
  cfhm_r <<- list()
  
  # read INMET data for all stations and calculate correction factors
  stations <<- stations_r
  for(i in c(1:length(stations[,1]))){
    statn <<- i
    final_data <- readINMET(statn,date.start)
    lldo <<- distanceorder()
    # interpolation to be used for further calculations
    MWH1 <- NNdf(int.method)
    vwM <-MWH1[,2]
    # calculation of correlation without correction
    wind_df <- data.frame(final_data$dates1,final_data$wind1,vwM[1:length(final_data$wind1)])
    wind_df <- na.omit(wind_df)
    wind_df_r <- rmrows(wind_df,120,2)
    
    
    # remove short months (less than shortmonths days)
    ym_r <- as.numeric(format(wind_df_r[,1],"%Y%m"))
    wind_df_r_mrm <- rmrows_small(data.frame(ym_r,wind_df_r),shortmonths*24,1)
    wind_df_r_mrm <- wind_df_r_mrm[,2:4]
    
    # calculation of monthly correlation and correction factors
    cm <- corrm(wind_df_r_mrm)
    cfm_r[[i]] <<- cm[[2]]
    corm_r[[i]] <<- cm[[1]]
    # calculation of hourly and monthly correlation and correction factors
    chm <- corrhm(wind_df_r_mrm)
    cfhm_r[[i]] <<- chm[[2]]
    corhm_r[[i]] <<- chm[[1]]
    
    rm(MWH1,vwM)
  }
}




# function which removes rows of less than len same entries occuring in col column in dataframe x 
rmrows_small <- function(x,len,col){
  lengths <- data.frame(num=rle(x[,col])$lengths,cum=cumsum(rle(x[,col])$lengths))
  
  whichs <- which(lengths$num<len)
  rmdf[statn,2] <<- length(whichs)
  lengths <- rbind(c(0,0),lengths)
  if(length(whichs)>0){
    rowrm <- NULL
    for(i in c(1:length(whichs))){
      rowrm <- c(rowrm,c((lengths[whichs[i],2]+1):(lengths[whichs[i]+1,2])))
    }
    y <- x[-c(rowrm),]
  }else{
    y=x
  }
  
  return(y)
}


# function for cleaning INMET data
# minmonth defines at least how many months need to be "complete"
# mindaynum defines the number of days which is sufficient for a month to be complete
# monthlim defines how many of the 12 months are allowed to have less than minmonth months with mindaynum days (1 for february)
# shortmonths defines the minimum number of days that a month must contain in ordner not to be removed from the data
# rmrows defines whether long rows (5 or more days) of same wind speeds shall be removed as they are considered error in the data
remove_months <- function(minmonth,mindaynum,monthlim,shortmonths,rmrows){
  date.start=as.POSIXct("1999-01-01 00:00:00",tz="UTC")
  setwd(dirinmetmeta)
  stations <<-read.table("stations_meta_data.csv",sep=";",header=T,stringsAsFactors=F)
  # remove first and last two because they are not in Brazil
  stations <<- stations[2:(length(stations[,1])-2),]
  # r: how many months are removed because they are too short? (less than 5 days)
  # rr: how many months have less than minmonth full months?
  rmdf <<- data.frame(statn=c(1:length(stations[,2])),r=rep(0,length(stations[,1])),rr=rep(0,length(stations[,1])))
  for(statn in c(1:length(stations[,1]))){
    statn <<-statn
    final_data <- readINMET(statn,date.start)
    wind_df <- na.omit(final_data)
    if(rmrows>0){wind_df_r <- rmrows(wind_df,120,2)}else{wind_df_r <- wind_df}
    ym <- as.numeric(format(wind_df_r[,1],"%Y%m"))
    wind_df_rr <- rmrows_small(data.frame(ym,wind_df_r),shortmonths*24,1)
    wind_df_rr <- wind_df_rr[,-c(1)]
    ym <- as.numeric(format(wind_df_rr[,1],"%Y%m"))
    df <- data.frame(rle(ym)$lengths,rle(ym)$values)
    # only select full months
    df <- df[which(df[,1]>=mindaynum*24),]
    # get full months
    m <- (df[,2])%%100
    m <- m[order(m)]
    genugmon <- which(rle(m)$lengths>=minmonth)
    rmdf[statn,3] <- 12-length(genugmon)
  }
  setwd(dirresults)
  save(rmdf,file=paste("rmdf_",mindaynum,"_",minmonth,"_",monthlim,if(rmrows==0){"normr"},".RData",sep=""))
}





# function to calculate power generated in one location with mean wind speed approximation
# parameters: rated power and height of used wind turbine as well as data for its power curve (windpseed and powercurve)
# limits for use of INMET data: distance limit (INmaxdist) - maximum allowed distance to closest INMET station
# and correlation limit (corrlimit) - minimum correlation after correction required
# method: interpolation method to use (1:NN,2:BLI,4:IDW, no BCI because useless)
# selection: which wind parks to use? all or only selected (largest of each state)? ("all" or "sel")
# mhm: parameter which defines the type of correction to apply (hourly and monthly "hm" or only monthly "m")
# applylim: optionally leave out applying the limits (distance an correlation) - relevant for comparison of particular wind parks
calcstatpower_windcor <- function(ratedpower,height,windspeed,powercurve,INmaxdist,corrlimit,method=1,selection="all",mhm,applylim=1){
  # load locations of wind speed measurement stations
  stations<-read.table(paste(dirinmetmeta,"/stations_meta_data.csv",sep=""),sep=";",header=T,stringsAsFactors=F)
  # remove first and last two because they are not in Brasil
  stations <- stations[2:(length(stations[,1])-2),]
  # remove stations that have time series with poor quality
  load(paste(dirresults,"/rmdf_",mindaynum,"_",minmonth,"_",monthlim,".RData",sep=""))
  stations <- stations[rmdf$statn[which(rmdf$rr<=monthlim)],]
  # extract longitudes and latitudes
  statlons <- stations$lon
  statlats <- stations$lat
  rm(stations)
  # load correction factors
  load(paste(dirresults,"/cfscors_r.RData",sep=""))
  # get data of windparks: capacities and start dates and sort by start dates for each location
  # also load mean correction factors for locations where distance is not too far away but correlation is too low
  # to apply monthly or hourly and monthly correction factors
  if(selection=="all"){
    load(paste(dirwindparks,"/windparks_complete.RData",sep=""))
    # load wind atlas correction factors
    load(paste(dirresults,"/cfs_WA.RData",sep=""))
  }else{
    load(paste(dirwindparks_sel,"/selected_windparks.RData",sep=""))
    windparks=sel_windparks
    rm(sel_windparks)
    # load wind atlas correction factors
    load(paste(dirresults,"/cfs_WA_stats.RData",sep=""))
  }
  windparks <- windparks[order(windparks$long),]
  windparks <- data.frame(windparks,comdate=as.POSIXct(paste(windparks$year,"-",windparks$month,"-",windparks$day," 00:00:00",sep=""),tz="UTC"))
  windparks <- windparks[which(windparks$comdate < as.POSIXct("2017-08-31 00:00:00",tz="UTC")),]
  windparks$comdate[which(windparks$comdate<date.start)] <- date.start
  numlon <- as.vector(unlist(rle(windparks$long)[1]))
  # counter for locations
  pp <- 1
  statpowlist <- list()
  powlistind <- 1
  while(pp<=length(windparks$long)){
    print(powlistind)
    numstat <- numlon[powlistind]
    pplon <- windparks$long[pp]
    pplat <- windparks$lat[pp]
    # find nearest neightbour MERRA and extrapolate to hubheight
    long <<- pplon
    lat <<- pplat
    lldo <<- distanceorder()
    NNmer <- NNdf(method,height)
    
    # find nearest wind measurement station
    ppINdistance <- 6378.388*acos(sin(rad*pplat) * sin(rad*statlats) + cos(rad*pplat) * cos(rad*statlats) * cos(rad*statlons-rad*pplon))
    # only use if within maximum distance
    if((min(ppINdistance)<INmaxdist)|applylim==0){
      # find data of nearest station
      statn <- which(ppINdistance==min(ppINdistance))
      # correct wind speeds
      if((mhm=="hm")&&((corhm_r[[statn]]>=corrlimit)|applylim==0)){
        h <- hour(NNmer[,1])
        m <- month(NNmer[,1])
        listhm <- data.frame(h+1,m)
        listcf <- cfhm_r[[statn]][as.matrix(listhm)]
        cwindMER <- NNmer[,2]*listcf
        wsc <- data.frame(time=NNmer[,1],ws=cwindMER)
      }else if((mhm=="m")&&((corm_r[[statn]]>=corrlimit)|applylim==0)){
        m <- month(NNmer[,1])
        listcf <- cfm_r[[statn]][m,2]
        cwindMER <- NNmer[,2]*listcf
        wsc <- data.frame(time=NNmer[,1],ws=cwindMER)
      }else{
        wsc <- NNmer
      }
      # then apply wind atlas mean wind speed correction
      wsc[,2] <- wsc[,2]*cfs_mean[[powlistind]]
      names(wsc) <- c("time","ws")
    }else{
      # only apply wind atlas wind speed correction
      wsc <- NNmer
      wsc[,2] <- wsc[,2]*cfs_mean[[powlistind]]
      names(wsc) <- c("time","ws")
    }
    
    # get startdates and capacities from municipios
    capdate <- data.frame(windparks$comdate[pp:(pp+numstat-1)],windparks$cap[pp:(pp+numstat-1)],rep(NA,numstat))
    names(capdate) <- c("commissioning","capacity","capacitysum")
    capdate <- capdate[order(capdate$commissioning),]
    capdate$capacitysum <- cumsum(capdate$capacity)
    # make a list of capacities for all dates
    caplist <- data.frame(NNmer[,1],rep(0,length(NNmer[,1])))
    match <- match(capdate$commissioning,caplist[,1])
    for(i in c(1:length(match))){
      caplist[match[i]:length(caplist[,1]),2] <- capdate$capacitysum[i]
    }
    
    
    # calculate power output for all hours from power curve in kWh
    # values are interpolated linearly between points of power curve
    whichs <- findInterval(wsc[,2],windspeed)
    whichs.1 <- whichs+1
    whichs.1[which(whichs.1>length(powercurve))] <- length(powercurve)
    statpower <- caplist[,2]/ratedpower*((powercurve[whichs]-powercurve[whichs.1])/(windspeed[whichs]-windspeed[whichs.1])*(wsc[,2]-windspeed[whichs.1])+powercurve[whichs.1])
    # replace NAs created where which is last element of powercurve with power of last element
    # because powercurve becomes flat and no higher power is generated
    statpower[which(whichs==length(powercurve))] <- caplist[which(whichs==length(powercurve)),2]/ratedpower*powercurve[length(powercurve)]
    
    statpowlist[[powlistind]] <- data.frame(wsc[,1],statpower)
    
    pp <- pp + numstat
    powlistind <- powlistind +1
    
  }
  
  return(statpowlist)
}









# function for writing data frames into csv
# adapted from the function write_dataframes_to_csv from the package sheetr
# df_list is list of dataframes that shall be written to file
# file is filename with path
write.list2 <- function (df_list, file) 
{
  headings <- names(df_list)
  seperator <- paste(replicate(50, "_"), collapse = "")
  sink(file)
  for (df.i in seq_along(df_list)) {
    if (headings[df.i] != "") {
      cat(headings[df.i])
      cat("\n")
    }
    write.table(df_list[[df.i]],sep=";")
    cat(seperator)
    cat("\n\n")
  }
  sink()
  return(file)
}






