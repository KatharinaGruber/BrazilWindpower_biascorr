# before running this script download the Global Wind Atlas for Brazil from
# here: https://irena.masdar.ac.ae/gallery/#map/103
# or here: https://globalwindatlas.info/

# create an account for downloading MERRA-2 data: https://urs.earthdata.nasa.gov/home
# login for downloading merra data
name <- "..."
password <- "..."




# base directory
dirbase <- "C:/..."
#directory where INMET stations are stored
dirinmet <- "C:/..."
# directory where MERRA data per point are stored
dirmerra <- "C:/..."
# directory where INMET meta data are stored
dirinmetmeta <- "C:/..."
# directory where results shall be stored
dirresults <- "C:/..."
# directory where windparkdata are stored
dirwindparks <- "C:/..."
# directory where windparkdata for selected windparks for comparison are stored
dirwindparks_sel = "C:/..."
# base MERRA data directory
dirmerrabase <- "C:/..."
# directory where recorded wind power generation data are stored
dirwindproddaily <- "C:/..."
# directory where recorded wind power generation data are stored for brazil and subsystems
dirwindprodsubbra <- "C:/..."
# directory where image and table results are stored
dirimtabres <- "C:/..."
# directory where wind atlas tif is stored
dirwindatlas <- "C:/..."
# directory where capacities provided by ONS are stored
dircaps <- "C:/..."
# directory where results with capacity correction are stored
dirresultscapc <- "C:/..."

# load script for handling merra data
source("C:/.../MERRA_data.R")
# load functions
source("C:/.../functions.R")







library(lubridate)
library(tibble)
library(feather)
library(dplyr)
library(tidyverse)
library(Metrics)
library(reshape2)
library(sheetr)

library(ggplot2)
library(BBmisc)
library(readxl)
library(hash)
library(gtools)
library(plotly)
library(raster)
library(rgdal)
library(ncdf4)
library(httr)
library(parallel)
library(forecast)
library(tseries)
library(fitdistrplus)
library(zoo)


##########################################################################################
##### DOWNLOAD ONS DATA ##################################################################
##########################################################################################
# downlaod ONS data:
# for this script data downloaded manually from http://ons.org.br/Paginas/resultados-da-operacao/historico-da-operacao/geracao_energia.aspx
# to download select "Simples" on top and on the left
# Selectione "Geração de Energia (GWh)"
# Escala de tempo "Dia"
# Subsistema, Estado and and Usina, the region to download
# Tipo de Usina "Eólica"
# and Período since 2006
# then click on the graph and select download and the csv
# data can also be downloaded automatically with ONSDownload Script, but only since 2015
dironsdownload <- "C:/..."
source(paste0(dironsdownload,"/ONSDownload.R"))


##########################################################################################
##### DOWNLOAD INMET DATA ################################################################
##########################################################################################
# directory where stations_meta_data.csv is stored
dirinmetmeta <- "C:/..."
# directory where download function for inmet are stored
dirinmetdownload <- "C:/..."
# connection fails often, therefore try again until it works
class(x) <- "try-error"
while(class(x)=="try-error"){
  x <- try(source(paste0(dirinmetdownload,"/INMETDownload.R")),silent = TRUE)
  stopCluster(cl)
}




##########################################################################################
##### DOWNLOAD OF MERRA DATA #############################################################
##########################################################################################

####the boundary of the box to download
####
lon1<--74.1
lat1<--36
lon2<--33
lat2<-5.5

# define time span for download
date_seq<-seq(as.POSIXct("1980-01-01",tz="UTC"),as.POSIXct("2017-08-31",tz="UTC"),by="d")

# download
# some files may not be downloaded correctly (they are smaller)
# if this happens, delete them and repeat the downlaod
getMERRADataBox(lon1,lat1,lon2,lat2,
                date_seq,c("U10M"),
                name,
                password,
                TRUE)
getMERRADataBox(lon1,lat1,lon2,lat2,
                date_seq,c("U50M"),
                name,
                password,
                TRUE)
getMERRADataBox(lon1,lat1,lon2,lat2,
                date_seq,c("V10M"),
                name,
                password,
                TRUE)
getMERRADataBox(lon1,lat1,lon2,lat2,
                date_seq,c("V50M"),
                name,
                password,
                TRUE)
getMERRADataBox(lon1,lat1,lon2,lat2,
                date_seq,c("DISPH"),
                name,
                password,
                TRUE)

# split date sequence due to memory restrictions
date_seq<-list(seq(as.POSIXct("1980-01-01",tz="UTC"),as.POSIXct("1984-12-31",tz="UTC"),by="d"),seq(as.POSIXct("1985-01-01",tz="UTC"),as.POSIXct("1989-12-31",tz="UTC"),by="d"),seq(as.POSIXct("1990-01-01",tz="UTC"),as.POSIXct("1994-12-31",tz="UTC"),by="d"),seq(as.POSIXct("1995-01-01",tz="UTC"),as.POSIXct("1999-12-31",tz="UTC"),by="d"),seq(as.POSIXct("2000-01-01",tz="UTC"),as.POSIXct("2004-12-31",tz="UTC"),by="d"),seq(as.POSIXct("2005-01-01",tz="UTC"),as.POSIXct("2009-12-31",tz="UTC"),by="d"),seq(as.POSIXct("2010-01-01",tz="UTC"),as.POSIXct("2014-12-31",tz="UTC"),by="d"),seq(as.POSIXct("2015-01-01",tz="UTC"),as.POSIXct("2017-08-31",tz="UTC"),by="d"))
setwd(dirmerrabase)

# convert to feather format
lapply(date_seq,convertMerraFeather,lon1,lat1,lon2,lat2,"U10M","U10m")
lapply(date_seq,convertMerraFeather,lon1,lat1,lon2,lat2,"U50M","U50m")
lapply(date_seq,convertMerraFeather,lon1,lat1,lon2,lat2,"V10M","V10m")
lapply(date_seq,convertMerraFeather,lon1,lat1,lon2,lat2,"V50M","V50m")
lapply(date_seq,convertMerraFeather,lon1,lat1,lon2,lat2,"DISPH","disph")

lonlat<-read_feather(paste(paste("./feather/LonLat","U10M",lon1,lat1,lon2,lat2,format(date_seq[[1]][1],"%Y%m%d"),format(date_seq[[1]][length(date_seq[[1]])],"%Y%m%d"),sep="_"),"/lonlat.feather",sep=""))
names(lonlat) <- c("long","lat")
write_feather(lonlat,paste(dirmerra,"/lonlat.feather",sep=""))

MerraDate <- seq(date_seq[[1]][1],date_seq[[length(date_seq)]][length(date_seq[[length(date_seq)]])],by="h")
hours <- as.POSIXct(rep(MerraDate[length(MerraDate)],23),tz="UTC")
hours <- hours + (1:23)*3600
MerraDate <- c(MerraDate,hours)
write_feather(as.data.frame(MerraDate),paste(dirmerra,"/MerraDate.feather",sep=""))

MerraDate <- read_feather(paste(dirmerra,"/MerraDate.feather",sep=""))
LonLat <- read_feather(paste(dirmerra,"/lonlat.feather",sep=""))

pnames <- c("U10M","U50M","V10M","V50M","DISPH")

# change format from daily to point-wise files
invisible(apply(lonlat[1:500,],1,saveMerraPoint,pnames,lon1,lat1,lon2,lat2,date_seq))
invisible(apply(lonlat[501:1000,],1,saveMerraPoint,pnames,lon1,lat1,lon2,lat2,date_seq))
invisible(apply(lonlat[1001:1500,],1,saveMerraPoint,pnames,lon1,lat1,lon2,lat2,date_seq))
invisible(apply(lonlat[1501:2000,],1,saveMerraPoint,pnames,lon1,lat1,lon2,lat2,date_seq))
invisible(apply(lonlat[2001:2500,],1,saveMerraPoint,pnames,lon1,lat1,lon2,lat2,date_seq))
invisible(apply(lonlat[2501:3000,],1,saveMerraPoint,pnames,lon1,lat1,lon2,lat2,date_seq))
invisible(apply(lonlat[3001:3500,],1,saveMerraPoint,pnames,lon1,lat1,lon2,lat2,date_seq))
invisible(apply(lonlat[3501:4000,],1,saveMerraPoint,pnames,lon1,lat1,lon2,lat2,date_seq))
invisible(apply(lonlat[4001:4500,],1,saveMerraPoint,pnames,lon1,lat1,lon2,lat2,date_seq))
invisible(apply(lonlat[4501:(dim(lonlat)[1]),],1,saveMerraPoint,pnames,lon1,lat1,lon2,lat2,date_seq))








#################################################
##### CALCULATE CAPACITY CORRECTION FACTORS #####
#################################################

# read wind park data (locations, installed capacities, commissioning dates)
load(paste(dirwindparks,"/windparks_complete.RData",sep=""))
# extract only commissiongs and capacities of NE and S
NEwp <- windparks[windparks$state %in% c("Bahia","Ceará","Paraíba","Pernambuco","Piaui","Rio Grande do Norte","Sergipe"),c(3,9,10,11)]
Swp <- windparks[windparks$state %in% c("Paraná","Santa Catarina","Rio Grande do Sul"),c(3,9,10,11)]
# create datetime commissioning dates from year month and day
Bwp_cd <- data.frame(cap=windparks$cap,comdate=as.POSIXct(paste(windparks$year,"-",windparks$month,"-",windparks$day," 00:00:00",sep=""),tz="UTC"))
NEwp_cd <- data.frame(cap=NEwp$cap,comdate=as.POSIXct(paste(NEwp$year,"-",NEwp$month,"-",NEwp$day," 00:00:00",sep=""),tz="UTC"))
Swp_cd <- data.frame(cap=Swp$cap,comdate=as.POSIXct(paste(Swp$year,"-",Swp$month,"-",Swp$day," 00:00:00",sep=""),tz="UTC"))
# aggregate by date to avoid multiple same date stamps
Bwp_ag <- aggregate(Bwp_cd$cap,by=list(Bwp_cd$comdate),sum)
NEwp_ag <- aggregate(NEwp_cd$cap,by=list(NEwp_cd$comdate),sum)
Swp_ag <- aggregate(Swp_cd$cap,by=list(Swp_cd$comdate),sum)
names(Bwp_ag) <- c("comdate","cap")
names(NEwp_ag) <- c("comdate","cap")
names(Swp_ag) <- c("comdate","cap")
# calculate cumulative installed capacities and divide by 1000 to get from kW to MW
Bcap_WP <- data.frame(commissioning=Bwp_ag$comdate,cap=cumsum(Bwp_ag$cap)/1000)
NEcap_WP <- data.frame(commissioning=NEwp_ag$comdate,cap=cumsum(NEwp_ag$cap)/1000)
Scap_WP <- data.frame(commissioning=Swp_ag$comdate,cap=cumsum(Swp_ag$cap)/1000)
# cut at 2006
bef06_B <- Bcap_WP[which(Bcap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_NE <- NEcap_WP[which(NEcap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_S <- Scap_WP[which(Scap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
Bcap_WP <- Bcap_WP[which(Bcap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
NEcap_WP <- NEcap_WP[which(NEcap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
Scap_WP <- Scap_WP[which(Scap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
# add capacity on 1.1.2006
Bcap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_B$cap,1)),Bcap_WP)
NEcap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_NE$cap,1)),NEcap_WP)
Scap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_S$cap,1)),Scap_WP)

# read capacities from ONS
Bcap_ONS <- read.table(paste(dircaps,"/Brasil.csv",sep=""),sep=";",header=T,stringsAsFactors=F)
NEcap_ONS <- read.table(paste(dircaps,"/Nordeste.csv",sep=""),sep=";",header=T,stringsAsFactors=F)
Scap_ONS <- read.table(paste(dircaps,"/Sul.csv",sep=""),sep=";",header=T,stringsAsFactors=F)
# extract yearmonth and generation in MW
Bcap_ONS <- data.frame(comdate=as.POSIXct(as.vector(paste(substr(Bcap_ONS[,2],7,10),"-",substr(Bcap_ONS[,2],4,5),"-01",sep="")),tz="UTC"),cap_MW=as.numeric(gsub(",",".",Bcap_ONS[,7],fixed=T)))
NEcap_ONS <- data.frame(comdate=as.POSIXct(as.vector(paste(substr(NEcap_ONS[,2],7,10),"-",substr(NEcap_ONS[,2],4,5),"-01",sep="")),tz="UTC"),cap_MW=as.numeric(gsub(",",".",NEcap_ONS[,7],fixed=T)))
Scap_ONS <- data.frame(comdate=as.POSIXct(as.vector(paste(substr(Scap_ONS[,2],7,10),"-",substr(Scap_ONS[,2],4,5),"-01",sep="")),tz="UTC"),cap_MW=as.numeric(gsub(",",".",Scap_ONS[,7],fixed=T)))
# reverse to have earliest at top and most recent at bottom
Bcap_ONS <- Bcap_ONS[c(length(Bcap_ONS[,1]):1),]
NEcap_ONS <- NEcap_ONS[c(length(NEcap_ONS[,1]):1),]
Scap_ONS <- Scap_ONS[c(length(Scap_ONS[,1]):1),]
# add starting capacity
Bcap_ONS <- rbind(data.frame(comdate=as.POSIXct("2006-01-01",tz="UTC"),cap_MW=0),Bcap_ONS)
NEcap_ONS <- rbind(data.frame(comdate=as.POSIXct("2006-01-01",tz="UTC"),cap_MW=0),NEcap_ONS)
Scap_ONS <- rbind(data.frame(comdate=as.POSIXct("2006-01-01",tz="UTC"),cap_MW=0),Scap_ONS)

# create data frame for all capacities to compare them
caps_df <- data.frame(time=seq(as.POSIXct("2006-01-01",tz="UTC"),as.POSIXct("2018-08-31",tz="UTC"),by="day"),Bons=NA,Bwp=NA,NEons=NA,NEwp=NA,Sons=NA,Swp=NA)
caps_df[match(Bcap_ONS[,1],caps_df[,1]),2] <- Bcap_ONS[,2]
caps_df[match(Bcap_WP[,1],caps_df[,1]),3] <- Bcap_WP[,2]
caps_df[match(NEcap_ONS[,1],caps_df[,1]),4] <- NEcap_ONS[,2]
caps_df[match(NEcap_WP[,1],caps_df[,1]),5] <- NEcap_WP[,2]
caps_df[match(Scap_ONS[,1],caps_df[,1]),6] <- Scap_ONS[,2]
caps_df[match(Scap_WP[,1],caps_df[,1]),7] <- Scap_WP[,2]
# fill NAs
caps_df[,2] <- na.locf(caps_df[,2])
caps_df[,3] <- na.locf(caps_df[,3])
caps_df[,4] <- na.locf(caps_df[,4])
caps_df[,5] <- na.locf(caps_df[,5])
caps_df[,6] <- na.locf(caps_df[,6])
caps_df[,7] <- na.locf(caps_df[,7])
# proportions
cfB <- sum(caps_df$Bons)/sum(caps_df$Bwp)
cfNE <- sum(caps_df$NEons)/sum(caps_df$NEwp)
cfS <- sum(caps_df$Sons)/sum(caps_df$Swp)

# save capacity correction factors for later use
save(cfB,cfNE,cfS,file=paste(dircaps,"/cap_cfs.RData",sep=""))


###################################################################################################################
############################### STEP 1: FIND BEST INTERPOLATION METHOD ############################################
###################################################################################################################
### ENERCON E-82
# datasheet (p7-10):http://www.enercon.de/fileadmin/Redakteur/Medien-Portal/broschueren/pdf/en/ENERCON_Produkt_en_06_2015.pdf
# power 0 at 0 wind speed is added
# selected rated power: 2000kW, selected height: 108m
# power curve: windspeed [m/s] and power output [kW]
ratedpower <- 2000
height <- 108
windspeed <- c(0:25)
powercurve <- c(0,0,3,25,82,174,312,532,815,1180,1580,1810,1980,rep(2050,13))

date.start <- as.POSIXct("2006-01-01",tz="UTC")
rad <- pi/180
LonLat <- read_feather(paste(dirmerra,"/lonlat.feather",sep=""))

# basic simulation returns wind power per wind park
# Nearest Neighbour
intmethod=1
statpowlist <- calcstatpower(ratedpower,height,windspeed,powercurve,intmethod)
setwd(dirresults)
save(statpowlist,file="statpowlist_NN.RData")
# Bilinear Interpolation
intmethod=2
statpowlist <- calcstatpower(ratedpower,height,windspeed,powercurve,intmethod)
setwd(dirresults)
save(statpowlist,file="statpowlist_BLI.RData")
# Inverse Distance Weighting
intmethod=4
statpowlist <- calcstatpower(ratedpower,height,windspeed,powercurve,intmethod)
setwd(dirresults)
save(statpowlist,file="statpowlist_IDW.RData")

# sum up power generation per state
makeSTATEpowlist("NN")
makeSTATEpowlist("BLI")
makeSTATEpowlist("IDW")



# do extra for particular wind parks
# load windparkdata
load(paste(dirwindparks,"/windparks_complete.RData",sep=""))
# select only windparks for comparison (usually windparks with largest installed capacity, or sufficiently long period of generation or avaiable)
sel_windparks = windparks[c(218,274,331:338,425:428,4,150,57),]
# save
save(sel_windparks,file=paste(dirwindparks_sel,"/selected_windparks.RData",sep=""))

# calculate wind power generation for selected wind parks
# Nearest Neighbour
intmethod=1
statpowlist <- calcstatpower(ratedpower,height,windspeed,powercurve,intmethod,selection="sel")
names(statpowlist) <- c("RS","SC","BA","CE","PI","PE","RN")
setwd(dirresults)
save(statpowlist,file="statpowlist_NN_sel.RData")
# Bilinear Interpolation
intmethod=2
statpowlist <- calcstatpower(ratedpower,height,windspeed,powercurve,intmethod,selection="sel")
names(statpowlist) <- c("RS","SC","BA","CE","PI","PE","RN")
setwd(dirresults)
save(statpowlist,file="statpowlist_BLI_sel.RData")
# Inverse Distance Weighting
intmethod=4
statpowlist <- calcstatpower(ratedpower,height,windspeed,powercurve,intmethod,selection="sel")
names(statpowlist) <- c("RS","SC","BA","CE","PI","PE","RN")
setwd(dirresults)
save(statpowlist,file="statpowlist_IDW_sel.RData")








##### prepare results for Brazil, subsystems, states and wind parks #####
load(paste(dirresults,"/STATEpowlist_NN.RData",sep=""))
STATEpowlist_NN <- STATEpowlist
load(paste(dirresults,"/statpowlist_NN_sel.RData",sep=""))
statpowlist_NN = statpowlist
load(paste(dirresults,"/STATEpowlist_BLI.RData",sep=""))
STATEpowlist_BLI <- STATEpowlist
load(paste(dirresults,"/statpowlist_BLI_sel.RData",sep=""))
statpowlist_BLI = statpowlist
load(paste(dirresults,"/STATEpowlist_IDW.RData",sep=""))
STATEpowlist_IDW <- STATEpowlist
load(paste(dirresults,"/statpowlist_IDW_sel.RData",sep=""))
statpowlist_IDW = statpowlist
# sum for NE and S
SUBpowlist_NN <- sum_subsystem(STATEpowlist_NN)
SUBpowlist_BLI <- sum_subsystem(STATEpowlist_BLI)
SUBpowlist_IDW <- sum_subsystem(STATEpowlist_IDW)
# sum for Brazil
Bpowlist_NN <- sum_brasil(STATEpowlist_NN)
Bpowlist_BLI <- sum_brasil(STATEpowlist_BLI)
Bpowlist_IDW <- sum_brasil(STATEpowlist_IDW)
# aggregate daily
statpowlist_NNd <- dailyaggregate(statpowlist_NN)
statpowlist_BLId <- dailyaggregate(statpowlist_BLI)
statpowlist_IDWd <- dailyaggregate(statpowlist_IDW)
STATEpowlist_NNd <- dailyaggregate(STATEpowlist_NN)
STATEpowlist_BLId <- dailyaggregate(STATEpowlist_BLI)
STATEpowlist_IDWd <- dailyaggregate(STATEpowlist_IDW)
SUBpowlist_NNd <- dailyaggregate(SUBpowlist_NN)
SUBpowlist_BLId <- dailyaggregate(SUBpowlist_BLI)
SUBpowlist_IDWd <- dailyaggregate(SUBpowlist_IDW)
Bpowlist_NNd <- aggregate(Bpowlist_NN[,2],by=list(format(Bpowlist_NN[,1],"%Y%m%d")),sum)
Bpowlist_BLId <- aggregate(Bpowlist_BLI[,2],by=list(format(Bpowlist_BLI[,1],"%Y%m%d")),sum)
Bpowlist_IDWd <- aggregate(Bpowlist_IDW[,2],by=list(format(Bpowlist_IDW[,1],"%Y%m%d")),sum)
# cut to startdate (first day with wind power > 0)
for(i in c(1:length(statpowlist_NNd))){
  statpowlist_NNd[[i]] <- statpowlist_NNd[[i]][which(statpowlist_NNd[[i]][,2]>0)[1]:length(statpowlist_NNd[[i]][,2]),]
  statpowlist_BLId[[i]] <- statpowlist_BLId[[i]][which(statpowlist_BLId[[i]][,2]>0)[1]:length(statpowlist_BLId[[i]][,2]),]
  statpowlist_IDWd[[i]] <- statpowlist_IDWd[[i]][which(statpowlist_IDWd[[i]][,2]>0)[1]:length(statpowlist_IDWd[[i]][,2]),]
}
for(i in c(1:length(STATEpowlist_NNd))){
  STATEpowlist_NNd[[i]] <- STATEpowlist_NNd[[i]][which(STATEpowlist_NNd[[i]][,2]>0)[1]:length(STATEpowlist_NNd[[i]][,2]),]
  STATEpowlist_BLId[[i]] <- STATEpowlist_BLId[[i]][which(STATEpowlist_BLId[[i]][,2]>0)[1]:length(STATEpowlist_BLId[[i]][,2]),]
  STATEpowlist_IDWd[[i]] <- STATEpowlist_IDWd[[i]][which(STATEpowlist_IDWd[[i]][,2]>0)[1]:length(STATEpowlist_IDWd[[i]][,2]),]
}
# add names of states
names(statpowlist_NNd) <- names(statpowlist_NN)
names(statpowlist_BLId) <- names(statpowlist_BLI)
names(statpowlist_IDWd) <- names(statpowlist_IDW)
names(STATEpowlist_NNd) <- c("Bahia","Ceará","Maranhão","MinasGerais","Paraíba","Paraná","Pernambuco","Piaui","RiodeJaneiro","RioGrandedoNorte","RioGrandedoSul","SantaCatarina","Sergipe")
names(STATEpowlist_BLId) <- c("Bahia","Ceará","Maranhão","MinasGerais","Paraíba","Paraná","Pernambuco","Piaui","RiodeJaneiro","RioGrandedoNorte","RioGrandedoSul","SantaCatarina","Sergipe")
names(STATEpowlist_IDWd) <- c("Bahia","Ceará","Maranhão","MinasGerais","Paraíba","Paraná","Pernambuco","Piaui","RiodeJaneiro","RioGrandedoNorte","RioGrandedoSul","SantaCatarina","Sergipe")

# load measured wind power
# stations
statprod <- list()
for(i in c(1:length(statpowlist_NNd))){
  statprod[[i]] <- getstatproddaily(names(statpowlist_NNd)[i])
}
names(statprod) <- names(statpowlist_NNd)
# states
STATEprod <- list()
ct <- NULL
for(i in c(1:length(STATEpowlist_NNd))){
  sp <- getSTATEproddaily(names(STATEpowlist_NNd)[i])
  if(length(sp)>0){
    STATEprod[[length(STATEprod)+1]] <- sp
    ct <- c(ct,i)
  }
}
names(STATEprod) <- names(STATEpowlist_NNd)[ct]
# subsystems
SUBprod <- list()
SUBprod[[1]] <- getprodSUBBRA("NE")
SUBprod[[2]] <- getprodSUBBRA("S")
# brazil
Bprod <- getprodSUBBRA("BRASIL")
save(statpowlist_NNd,statpowlist_BLId,statpowlist_IDWd,STATEpowlist_NNd,STATEpowlist_BLId,STATEpowlist_IDWd,SUBpowlist_NNd,SUBpowlist_BLId,SUBpowlist_IDWd,Bpowlist_NNd,Bpowlist_BLId,Bpowlist_IDWd,statprod,STATEprod,SUBprod,Bprod,file=paste(dirresults,"/comp_interpolationmethods.RData",sep=""))


load(paste(dirresults,"/comp_interpolationmethods.RData",sep=""))
# convert calculated wind power from kWh to GWh
for(i in c(1:length(statpowlist_NNd))){
  statpowlist_NNd[[i]][,2] <- statpowlist_NNd[[i]][,2]/10^6
  statpowlist_BLId[[i]][,2] <- statpowlist_BLId[[i]][,2]/10^6
  statpowlist_IDWd[[i]][,2] <- statpowlist_IDWd[[i]][,2]/10^6
}
for(i in c(1:length(STATEpowlist_NNd))){
  STATEpowlist_NNd[[i]][,2] <- STATEpowlist_NNd[[i]][,2]/10^6
  STATEpowlist_BLId[[i]][,2] <- STATEpowlist_BLId[[i]][,2]/10^6
  STATEpowlist_IDWd[[i]][,2] <- STATEpowlist_IDWd[[i]][,2]/10^6
}
for(i in c(1:length(SUBpowlist_NNd))){
  SUBpowlist_NNd[[i]][,2] <- SUBpowlist_NNd[[i]][,2]/10^6
  SUBpowlist_BLId[[i]][,2] <- SUBpowlist_BLId[[i]][,2]/10^6
  SUBpowlist_IDWd[[i]][,2] <- SUBpowlist_IDWd[[i]][,2]/10^6
}
Bpowlist_NNd[,2] <- Bpowlist_NNd[,2]/10^6
Bpowlist_BLId[,2] <- Bpowlist_BLId[,2]/10^6
Bpowlist_IDWd[,2] <- Bpowlist_IDWd[,2]/10^6


# correct with capacity correction factors
load(paste(dircaps,"/cap_cfs.RData",sep=""))
# of stations first two are in South, rest in Northeast
for(i in c(1:2)){
  statpowlist_NNd[[i]][,2] <- statpowlist_NNd[[i]][,2]*cfS
  statpowlist_BLId[[i]][,2] <- statpowlist_BLId[[i]][,2]*cfS
  statpowlist_IDWd[[i]][,2] <- statpowlist_IDWd[[i]][,2]*cfS
}
for(i in c(3:length(statpowlist_NNd))){
  statpowlist_NNd[[i]][,2] <- statpowlist_NNd[[i]][,2]*cfNE
  statpowlist_BLId[[i]][,2] <- statpowlist_BLId[[i]][,2]*cfNE
  statpowlist_IDWd[[i]][,2] <- statpowlist_IDWd[[i]][,2]*cfNE
}
# NE: 1,2,3,5,7,8,10,13
# S: 6,11,12
# SE: 4,9
for(i in c(1,2,3,5,7,8,10,13)){
  STATEpowlist_NNd[[i]][,2] <- STATEpowlist_NNd[[i]][,2]*cfNE
  STATEpowlist_BLId[[i]][,2] <- STATEpowlist_BLId[[i]][,2]*cfNE
  STATEpowlist_IDWd[[i]][,2] <- STATEpowlist_IDWd[[i]][,2]*cfNE
}
for(i in c(6,11,12)){
  STATEpowlist_NNd[[i]][,2] <- STATEpowlist_NNd[[i]][,2]*cfS
  STATEpowlist_BLId[[i]][,2] <- STATEpowlist_BLId[[i]][,2]*cfS
  STATEpowlist_IDWd[[i]][,2] <- STATEpowlist_IDWd[[i]][,2]*cfS
}
for(i in c(4,9)){
  STATEpowlist_NNd[[i]][,2] <- STATEpowlist_NNd[[i]][,2]*cfB
  STATEpowlist_BLId[[i]][,2] <- STATEpowlist_BLId[[i]][,2]*cfB
  STATEpowlist_IDWd[[i]][,2] <- STATEpowlist_IDWd[[i]][,2]*cfB
}
#subsystems
SUBpowlist_NNd[[1]][,2] <- SUBpowlist_NNd[[1]][,2]*cfNE
SUBpowlist_BLId[[1]][,2] <- SUBpowlist_BLId[[1]][,2]*cfNE
SUBpowlist_IDWd[[1]][,2] <- SUBpowlist_IDWd[[1]][,2]*cfNE
SUBpowlist_NNd[[2]][,2] <- SUBpowlist_NNd[[2]][,2]*cfS
SUBpowlist_BLId[[2]][,2] <- SUBpowlist_BLId[[2]][,2]*cfS
SUBpowlist_IDWd[[2]][,2] <- SUBpowlist_IDWd[[2]][,2]*cfS
# brazil
Bpowlist_NNd[,2] <- Bpowlist_NNd[,2]*cfB
Bpowlist_BLId[,2] <- Bpowlist_BLId[,2]*cfB
Bpowlist_IDWd[,2] <- Bpowlist_IDWd[,2]*cfB


# cut to same lengths
statcomp_NNd <- list()
statcomp_BLId <- list()
statcomp_IDWd <- list()
for(i in c(1:length(statpowlist_NNd))){
  statcomp_NNd[[i]] <- csl(statpowlist_NNd[[i]],statprod[[i]])
  statcomp_BLId[[i]] <- csl(statpowlist_BLId[[i]],statprod[[i]])
  statcomp_IDWd[[i]] <- csl(statpowlist_IDWd[[i]],statprod[[i]])
}
STATEcomp_NNd <- list()
STATEcomp_BLId <- list()
STATEcomp_IDWd <- list()
for(i in c(1:length(STATEprod))){
  ind = match(names(STATEprod)[i],names(STATEpowlist_NNd))
  STATEcomp_NNd[[i]] <- csl(STATEpowlist_NNd[[ind]],STATEprod[[i]])
  STATEcomp_BLId[[i]] <- csl(STATEpowlist_BLId[[ind]],STATEprod[[i]])
  STATEcomp_IDWd[[i]] <- csl(STATEpowlist_IDWd[[ind]],STATEprod[[i]])
}
SUBcomp_NNd <- list()
SUBcomp_BLId <- list()
SUBcomp_IDWd <- list()
for(i in c(1:length(SUBpowlist_NNd))){
  SUBcomp_NNd[[i]] <- csl(SUBpowlist_NNd[[i]],SUBprod[[i]])
  SUBcomp_BLId[[i]] <- csl(SUBpowlist_BLId[[i]],SUBprod[[i]])
  SUBcomp_IDWd[[i]] <- csl(SUBpowlist_IDWd[[i]],SUBprod[[i]])
}
Bcomp_NNd <- csl(Bpowlist_NNd,Bprod)
Bcomp_BLId <- csl(Bpowlist_BLId,Bprod)
Bcomp_IDWd <- csl(Bpowlist_IDWd,Bprod)







##### analyse results for Brazil, subsystems, states and wind parks #####
# Brazil
Bcors <- round(c(cor(Bcomp_NNd[,2],Bcomp_NNd[,3]),cor(Bcomp_BLId[,2],Bcomp_BLId[,3]),cor(Bcomp_IDWd[,2],Bcomp_IDWd[,3])),3)
Brmse <- round(c(rmse(Bcomp_NNd[,2],Bcomp_NNd[,3]),rmse(Bcomp_BLId[,2],Bcomp_BLId[,3]),rmse(Bcomp_IDWd[,2],Bcomp_IDWd[,3])),3)
Bmbe <- round(c(mean(Bcomp_NNd[,2]-Bcomp_NNd[,3]),mean(Bcomp_BLId[,2]-Bcomp_BLId[,3]),mean(Bcomp_IDWd[,2]-Bcomp_IDWd[,3])),3)
Bmeans <- round(c(mean(Bcomp_NNd[,2]),mean(Bcomp_BLId[,2]),mean(Bcomp_IDWd[,2]),mean(Bcomp_NNd[,3])),3)
Bstats <- data.frame(int=c("NN","BLI","IDW","obs"),cor=c(Bcors,NA),rmse=c(Brmse,NA),mbe=c(Bmbe,NA),mean=Bmeans)
save(Bstats,file=paste0(dirresultscapc,"/Bstats_noc.RData"))
write.table(Bstats,file=paste(dirresultscapc,"/Bstats_noc.csv",sep=""),sep=";")
# merge all data to a tibble
dat <- melt(data.frame(NN=Bcomp_NNd[,2]-Bcomp_NNd[,3],BLI=Bcomp_BLId[,2]-Bcomp_NNd[,3],IDW=Bcomp_IDWd[,2]-Bcomp_NNd[,3]))
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  xlab("Interpolation method") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirresultscapc,"/diff_B_noc.png",sep=""), width = 6, height = 4.125)
# Subsystems
SUBstats <- list()
for(i in c(1:length(SUBcomp_NNd))){
  SUBcors <- round(c(c(cor(SUBcomp_NNd[[i]][,2],SUBcomp_NNd[[i]][,3]),cor(SUBcomp_BLId[[i]][,2],SUBcomp_BLId[[i]][,3]),cor(SUBcomp_IDWd[[i]][,2],SUBcomp_IDWd[[i]][,3]))),3)
  SUBrmse <- round(c(rmse(SUBcomp_NNd[[i]][,2],SUBcomp_NNd[[i]][,3]),rmse(SUBcomp_BLId[[i]][,2],SUBcomp_BLId[[i]][,3]),rmse(SUBcomp_IDWd[[i]][,2],SUBcomp_IDWd[[i]][,3])),3)
  SUBmbe <- round(c(mean(SUBcomp_NNd[[i]][,2]-SUBcomp_NNd[[i]][,3]),mean(SUBcomp_BLId[[i]][,2]-SUBcomp_BLId[[i]][,3]),mean(SUBcomp_IDWd[[i]][,2]-SUBcomp_IDWd[[i]][,3])),3)
  SUBmeans <- round(c(mean(SUBcomp_NNd[[i]][,2]),mean(SUBcomp_BLId[[i]][,2]),mean(SUBcomp_IDWd[[i]][,2]),mean(SUBcomp_NNd[[i]][,3])),3)
  stat <- data.frame(int=c("NN","BLI","IDW","obs"),cor=c(SUBcors,NA),rmse=c(SUBrmse,NA),mbe=c(SUBmbe,NA),mean=SUBmeans)
  SUBstats[[i]] <- stat
}
names(SUBstats) <- c("NE","S")
save(SUBstats,file=paste0(dirresultscapc,"/SUBstats_noc.RData"))
write.list2(SUBstats,paste(dirresultscapc,"/SUBstats_noc.csv",sep=""))
dat <- NULL
for(i in c(1:length(SUBcomp_NNd))){
  # merge all data to a tibble
  dat1 <- melt(data.frame(NN=SUBcomp_NNd[[i]][,2]-SUBcomp_NNd[[i]][,3],BLI=SUBcomp_BLId[[i]][,2]-SUBcomp_NNd[[i]][,3],IDW=SUBcomp_IDWd[[i]][,2]-SUBcomp_NNd[[i]][,3]))
  dat1 <- data.frame(subsystem=names(SUBstats)[i],dat1)
  if(length(dat)>1){
    dat <- rbind(dat,dat1)
  }else{
    dat <- dat1
  }
}
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  facet_wrap(~subsystem) +
  xlab("Interpolation method") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirresultscapc,"/diff_SUB_noc.png",sep=""), width = 6, height = 4.125)
# States
STATEstats <- list()
for(i in c(1:length(STATEcomp_NNd))){
  STATEcors <- round(c(c(cor(STATEcomp_NNd[[i]][,2],STATEcomp_NNd[[i]][,3]),cor(STATEcomp_BLId[[i]][,2],STATEcomp_BLId[[i]][,3]),cor(STATEcomp_IDWd[[i]][,2],STATEcomp_IDWd[[i]][,3]))),3)
  STATErmse <- round(c(rmse(STATEcomp_NNd[[i]][,2],STATEcomp_NNd[[i]][,3]),rmse(STATEcomp_BLId[[i]][,2],STATEcomp_BLId[[i]][,3]),rmse(STATEcomp_IDWd[[i]][,2],STATEcomp_IDWd[[i]][,3])),3)
  STATEmbe <- round(c(mean(STATEcomp_NNd[[i]][,2]-STATEcomp_NNd[[i]][,3]),mean(STATEcomp_BLId[[i]][,2]-STATEcomp_BLId[[i]][,3]),mean(STATEcomp_IDWd[[i]][,2]-STATEcomp_IDWd[[i]][,3])),3)
  STATEmeans <- round(c(mean(STATEcomp_NNd[[i]][,2]),mean(STATEcomp_BLId[[i]][,2]),mean(STATEcomp_IDWd[[i]][,2]),mean(STATEcomp_NNd[[i]][,3])),3)
  stat <- data.frame(int=c("NN","BLI","IDW","obs"),cor=c(STATEcors,NA),rmse=c(STATErmse,NA),mbe=c(STATEmbe,NA),mean=STATEmeans)
  STATEstats[[i]] <- stat
}
names(STATEstats) <- names(STATEprod)
save(STATEstats,file=paste0(dirresultscapc,"/STATEstats_noc.RData"))
write.list2(STATEstats,paste(dirresultscapc,"/STATEstats_noc.csv",sep=""))
dat <- NULL
for(i in c(1:length(STATEcomp_NNd))){
  # merge all data to a tibble
  dat1 <- melt(data.frame(NN=STATEcomp_NNd[[i]][,2]-STATEcomp_NNd[[i]][,3],BLI=STATEcomp_BLId[[i]][,2]-STATEcomp_NNd[[i]][,3],IDW=STATEcomp_IDWd[[i]][,2]-STATEcomp_NNd[[i]][,3]))
  dat1 <- data.frame(state=names(STATEstats)[i],dat1)
  if(length(dat)>1){
    dat <- rbind(dat,dat1)
  }else{
    dat <- dat1
  }
}
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  facet_wrap(~state) +
  xlab("Interpolation method") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirresultscapc,"/diff_STATE_noc.png",sep=""), width = 8, height = 8.25)
# Wind power plants
statstats <- list()
for(i in c(1:length(statcomp_NNd))){
  statcors <- round(c(c(cor(statcomp_NNd[[i]][,2],statcomp_NNd[[i]][,3]),cor(statcomp_BLId[[i]][,2],statcomp_BLId[[i]][,3]),cor(statcomp_IDWd[[i]][,2],statcomp_IDWd[[i]][,3]))),3)
  statrmse <- round(c(rmse(statcomp_NNd[[i]][,2],statcomp_NNd[[i]][,3]),rmse(statcomp_BLId[[i]][,2],statcomp_BLId[[i]][,3]),rmse(statcomp_IDWd[[i]][,2],statcomp_IDWd[[i]][,3])),3)
  statmbe <- round(c(mean(statcomp_NNd[[i]][,2]-statcomp_NNd[[i]][,3]),mean(statcomp_BLId[[i]][,2]-statcomp_BLId[[i]][,3]),mean(statcomp_IDWd[[i]][,2]-statcomp_IDWd[[i]][,3])),3)
  statmeans <- round(c(mean(statcomp_NNd[[i]][,2]),mean(statcomp_BLId[[i]][,2]),mean(statcomp_IDWd[[i]][,2]),mean(statcomp_NNd[[i]][,3])),3)
  stat <- data.frame(int=c("NN","BLI","IDW","obs"),cor=c(statcors,NA),rmse=c(statrmse,NA),mbe=c(statmbe,NA),mean=statmeans)
  statstats[[i]] <- stat
}
names(statstats) <- c("RS-ElebrasCidreira1","SC-BomJardim","BA-Macaubas","CE-PraiaFormosa","PI-Araripe","PE-SaoClemente","RN-AlegriaII")
save(statstats,file=paste0(dirresultscapc,"/statstats_noc.RData"))
write.list2(statstats,paste(dirresultscapc,"/statstats_noc.csv",sep=""))
dat <- NULL
for(i in c(1:length(statcomp_NNd))){
  # merge all data to a tibble
  dat1 <- melt(data.frame(NN=statcomp_NNd[[i]][,2]-statcomp_NNd[[i]][,3],BLI=statcomp_BLId[[i]][,2]-statcomp_NNd[[i]][,3],IDW=statcomp_IDWd[[i]][,2]-statcomp_NNd[[i]][,3]))
  dat1 <- data.frame(state=names(statstats)[i],dat1)
  if(length(dat)>1){
    dat <- rbind(dat,dat1)
  }else{
    dat <- dat1
  }
}
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  facet_wrap(~state,nrow=2) +
  xlab("Interpolation method") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirresultscapc,"/diff_stat_noc.png",sep=""), width = 9, height = 4.125)



















#################################################################################################
############## STEP 2: CHOOSE DATA SOURCE FOR WIND SPEED MEAN CORRECTION ########################
############## INMET WIND SPEED MEASUREMENTS OR MEAN WIND SPEEDS GLOBAL WIND ATLAS ##############
#################################################################################################
# general data
LonLat <- read_feather(paste(dirmerra,"/lonlat.feather",sep=""))
# start date 1999 because of INMET data
date.start <- as.POSIXct("1999-01-01",tz="UTC")
rad <- pi/180
# Enercon E-82
ratedpower <- 2000
height <- 108
windspeed <- c(0:25)
powercurve <- c(0,0,3,25,82,174,312,532,815,1180,1580,1810,1980,rep(2050,13))


##### INMET #####
# determining factor whether mean approximation is carried out:
# limit for max distance to INMET station: 80 km
INmaxdist <- 80
# Nearest Neighbour
method=1
wscdata="INMET"
statpowlist <- calcstatpower_meanAPT(ratedpower,height,windspeed,powercurve,method,selection="all",wscdata)
setwd(dirresults)
save(statpowlist,cfs_mean,file="statpowlist_wsmaIN.RData") # wsmaIN = wind speed mean approximation with INMET data

# sum up power generation per state
makeSTATEpowlist("wsmaIN")


# calculate wind power generation for selected wind parks
# Nearest Neighbour
method=1
wscdata = "INMET"
statpowlist <- calcstatpower_meanAPT(ratedpower,height,windspeed,powercurve,method,selection="sel",wscdata)
names(statpowlist) <- c("RS","SC","BA","CE","PI","PE","RN")
setwd(dirresults)
save(statpowlist,cfs_mean,file="statpowlist_wsmaIN_sel.RData")


##### WIND ATLAS #####
# prepare wind atlas data (for faster loading)
tif = raster(paste(dirwindatlas,"/wind_atlas_all_clip.tif",sep=""))
windatlas <- rasterToPoints(tif)
save(windatlas,file=paste(dirwindatlas,"/wind_atlas.RData",sep=""))

# do mean approximation with Wind Atlas Data and Nearest Neighbour method
method=1
wscdata="WINDATLAS"
statpowlist <- calcstatpower_meanAPT(ratedpower,height,windspeed,powercurve,method,selection="all",wscdata)
setwd(dirresults)
save(statpowlist,file="statpowlist_wsmaWA.RData") # wsmaWA = wind speed mean approximation with Wind Atlas data
save(cfs_mean,file="cfs_WA.RData")

# sum up power generation per state
makeSTATEpowlist("wsmaWA")


# calculate wind power generation for selected wind parks
# Nearest Neighbour
method=1
wscdata = "WINDATLAS"
statpowlist <- calcstatpower_meanAPT(ratedpower,height,windspeed,powercurve,method,selection="sel",wscdata)
names(statpowlist) <- c("RS","SC","BA","CE","PI","PE","RN")
setwd(dirresults)
save(statpowlist,file="statpowlist_wsmaWA_sel.RData")
save(cfs_mean,file="cfs_WA_stats.RData")



##### prepare results for brazil, subsystems, states and wind parks #####
load(paste(dirresults,"/STATEpowlist_NN.RData",sep=""))
STATEpowlist_NN <- STATEpowlist
load(paste(dirresults,"/statpowlist_NN_sel.RData",sep=""))
statpowlist_NN = statpowlist
load(paste(dirresults,"/statpowlist_wsmaIN_sel.RData",sep=""))
statpowlist_IN <- statpowlist
load(paste(dirresults,"/statpowlist_wsmaWA_sel.RData",sep=""))
statpowlist_WA <- statpowlist
load(paste(dirresults,"/STATEpowlist_wsmaIN.RData",sep=""))
STATEpowlist_IN <- STATEpowlist
load(paste(dirresults,"/STATEpowlist_wsmaWA.RData",sep=""))
STATEpowlist_WA <- STATEpowlist

# sum for NE and S
SUBpowlist_NN <- sum_subsystem(STATEpowlist_NN)
SUBpowlist_IN <- sum_subsystem(STATEpowlist_IN)
SUBpowlist_WA <- sum_subsystem(STATEpowlist_WA)
# sum for Brazil
Bpowlist_NN <- sum_brasil(STATEpowlist_NN)
Bpowlist_IN <- sum_brasil(STATEpowlist_IN)
Bpowlist_WA <- sum_brasil(STATEpowlist_WA)
# aggregate daily
statpowlist_NNd <- dailyaggregate(statpowlist_NN)
statpowlist_INd <- dailyaggregate(statpowlist_IN)
statpowlist_WAd <- dailyaggregate(statpowlist_WA)
STATEpowlist_NNd <- dailyaggregate(STATEpowlist_NN)
STATEpowlist_INd <- dailyaggregate(STATEpowlist_IN)
STATEpowlist_WAd <- dailyaggregate(STATEpowlist_WA)
SUBpowlist_NNd <- dailyaggregate(SUBpowlist_NN)
SUBpowlist_INd <- dailyaggregate(SUBpowlist_IN)
SUBpowlist_WAd <- dailyaggregate(SUBpowlist_WA)
Bpowlist_NNd <- aggregate(Bpowlist_NN[,2],by=list(format(Bpowlist_NN[,1],"%Y%m%d")),sum)
Bpowlist_INd <- aggregate(Bpowlist_IN[,2],by=list(format(Bpowlist_IN[,1],"%Y%m%d")),sum)
Bpowlist_WAd <- aggregate(Bpowlist_WA[,2],by=list(format(Bpowlist_WA[,1],"%Y%m%d")),sum)
# cut to startdate (first day with wind power > 0)
for(i in c(1:length(statpowlist_INd))){
  statpowlist_NNd[[i]] <- statpowlist_NNd[[i]][which(statpowlist_NNd[[i]][,2]>0)[1]:length(statpowlist_NNd[[i]][,2]),]
  statpowlist_INd[[i]] <- statpowlist_INd[[i]][which(statpowlist_INd[[i]][,2]>0)[1]:length(statpowlist_INd[[i]][,2]),]
  statpowlist_WAd[[i]] <- statpowlist_WAd[[i]][which(statpowlist_WAd[[i]][,2]>0)[1]:length(statpowlist_WAd[[i]][,2]),]
}
for(i in c(1:length(STATEpowlist_INd))){
  STATEpowlist_NNd[[i]] <- STATEpowlist_NNd[[i]][which(STATEpowlist_NNd[[i]][,2]>0)[1]:length(STATEpowlist_NNd[[i]][,2]),]
  STATEpowlist_INd[[i]] <- STATEpowlist_INd[[i]][which(STATEpowlist_INd[[i]][,2]>0)[1]:length(STATEpowlist_INd[[i]][,2]),]
  STATEpowlist_WAd[[i]] <- STATEpowlist_WAd[[i]][which(STATEpowlist_WAd[[i]][,2]>0)[1]:length(STATEpowlist_WAd[[i]][,2]),]
}
# add names of states
names(statpowlist_NNd) <- names(statpowlist_NN)
names(statpowlist_INd) <- names(statpowlist_IN)
names(statpowlist_WAd) <- names(statpowlist_WA)
names(STATEpowlist_NNd) <- c("Bahia","Ceará","Maranhão","MinasGerais","Paraíba","Paraná","Pernambuco","Piaui","RiodeJaneiro","RioGrandedoNorte","RioGrandedoSul","SantaCatarina","Sergipe")
names(STATEpowlist_INd) <- c("Bahia","Ceará","Maranhão","MinasGerais","Paraíba","Paraná","Pernambuco","Piaui","RiodeJaneiro","RioGrandedoNorte","RioGrandedoSul","SantaCatarina","Sergipe")
names(STATEpowlist_WAd) <- c("Bahia","Ceará","Maranhão","MinasGerais","Paraíba","Paraná","Pernambuco","Piaui","RiodeJaneiro","RioGrandedoNorte","RioGrandedoSul","SantaCatarina","Sergipe")

# load measured wind power
# stations
statprod <- list()
for(i in c(1:length(statpowlist_INd))){
  statprod[[i]] <- getstatproddaily(names(statpowlist_INd)[i])
}
names(statprod) <- names(statpowlist_INd)
# states
STATEprod <- list()
ct <- NULL
for(i in c(1:length(STATEpowlist_INd))){
  sp <- getSTATEproddaily(names(STATEpowlist_INd)[i])
  if(length(sp)>0){
    STATEprod[[length(STATEprod)+1]] <- sp
    ct <- c(ct,i)
  }
}
names(STATEprod) <- names(STATEpowlist_INd)[ct]
# subsystems
SUBprod <- list()
SUBprod[[1]] <- getprodSUBBRA("NE")
SUBprod[[2]] <- getprodSUBBRA("S")
# brazil
Bprod <- getprodSUBBRA("BRASIL")
save(statpowlist_NNd,statpowlist_INd,statpowlist_WAd,STATEpowlist_NNd,STATEpowlist_INd,STATEpowlist_WAd,SUBpowlist_NNd,SUBpowlist_INd,SUBpowlist_WAd,Bpowlist_NNd,Bpowlist_INd,Bpowlist_WAd,statprod,STATEprod,SUBprod,Bprod,file=paste(dirresults,"/comp_wsma.RData",sep=""))



load(paste(dirresults,"/comp_wsma.RData",sep=""))
# convert calculated wind power from kWh to GWh
for(i in c(1:length(statpowlist_INd))){
  statpowlist_NNd[[i]][,2] <- statpowlist_NNd[[i]][,2]/10^6
  statpowlist_INd[[i]][,2] <- statpowlist_INd[[i]][,2]/10^6
  statpowlist_WAd[[i]][,2] <- statpowlist_WAd[[i]][,2]/10^6
}
for(i in c(1:length(STATEpowlist_INd))){
  STATEpowlist_NNd[[i]][,2] <- STATEpowlist_NNd[[i]][,2]/10^6
  STATEpowlist_INd[[i]][,2] <- STATEpowlist_INd[[i]][,2]/10^6
  STATEpowlist_WAd[[i]][,2] <- STATEpowlist_WAd[[i]][,2]/10^6
}
for(i in c(1:length(SUBpowlist_INd))){
  SUBpowlist_NNd[[i]][,2] <- SUBpowlist_NNd[[i]][,2]/10^6
  SUBpowlist_INd[[i]][,2] <- SUBpowlist_INd[[i]][,2]/10^6
  SUBpowlist_WAd[[i]][,2] <- SUBpowlist_WAd[[i]][,2]/10^6
}
Bpowlist_NNd[,2] <- Bpowlist_NNd[,2]/10^6
Bpowlist_INd[,2] <- Bpowlist_INd[,2]/10^6
Bpowlist_WAd[,2] <- Bpowlist_WAd[,2]/10^6



# correct with capacity correction factors
load(paste(dircaps,"/cap_cfs.RData",sep=""))
# of stations first two are in South, rest in Northeast
for(i in c(1:2)){
  statpowlist_NNd[[i]][,2] <- statpowlist_NNd[[i]][,2]*cfS
  statpowlist_INd[[i]][,2] <- statpowlist_INd[[i]][,2]*cfS
  statpowlist_WAd[[i]][,2] <- statpowlist_WAd[[i]][,2]*cfS
}
for(i in c(3:length(statpowlist_NNd))){
  statpowlist_NNd[[i]][,2] <- statpowlist_NNd[[i]][,2]*cfNE
  statpowlist_INd[[i]][,2] <- statpowlist_INd[[i]][,2]*cfNE
  statpowlist_WAd[[i]][,2] <- statpowlist_WAd[[i]][,2]*cfNE
}
# NE: 1,2,3,5,7,8,10,13
# S: 6,11,12
# SE: 4,9
for(i in c(1,2,3,5,7,8,10,13)){
  STATEpowlist_NNd[[i]][,2] <- STATEpowlist_NNd[[i]][,2]*cfNE
  STATEpowlist_INd[[i]][,2] <- STATEpowlist_INd[[i]][,2]*cfNE
  STATEpowlist_WAd[[i]][,2] <- STATEpowlist_WAd[[i]][,2]*cfNE
}
for(i in c(6,11,12)){
  STATEpowlist_NNd[[i]][,2] <- STATEpowlist_NNd[[i]][,2]*cfS
  STATEpowlist_INd[[i]][,2] <- STATEpowlist_INd[[i]][,2]*cfS
  STATEpowlist_WAd[[i]][,2] <- STATEpowlist_WAd[[i]][,2]*cfS
}
for(i in c(4,9)){
  STATEpowlist_NNd[[i]][,2] <- STATEpowlist_NNd[[i]][,2]*cfB
  STATEpowlist_INd[[i]][,2] <- STATEpowlist_INd[[i]][,2]*cfB
  STATEpowlist_WAd[[i]][,2] <- STATEpowlist_WAd[[i]][,2]*cfB
}
#subsystems
SUBpowlist_NNd[[1]][,2] <- SUBpowlist_NNd[[1]][,2]*cfNE
SUBpowlist_INd[[1]][,2] <- SUBpowlist_INd[[1]][,2]*cfNE
SUBpowlist_WAd[[1]][,2] <- SUBpowlist_WAd[[1]][,2]*cfNE
SUBpowlist_NNd[[2]][,2] <- SUBpowlist_NNd[[2]][,2]*cfS
SUBpowlist_INd[[2]][,2] <- SUBpowlist_INd[[2]][,2]*cfS
SUBpowlist_WAd[[2]][,2] <- SUBpowlist_WAd[[2]][,2]*cfS
# brazil
Bpowlist_NNd[,2] <- Bpowlist_NNd[,2]*cfB
Bpowlist_INd[,2] <- Bpowlist_INd[,2]*cfB
Bpowlist_WAd[,2] <- Bpowlist_WAd[,2]*cfB


# cut to same lengths
statcomp_NNd <- list()
statcomp_INd <- list()
statcomp_WAd <- list()
for(i in c(1:length(statpowlist_INd))){
  statcomp_NNd[[i]] <- csl(statpowlist_NNd[[i]],statprod[[i]])
  statcomp_INd[[i]] <- csl(statpowlist_INd[[i]],statprod[[i]])
  statcomp_WAd[[i]] <- csl(statpowlist_WAd[[i]],statprod[[i]])
}
STATEcomp_NNd <- list()
STATEcomp_INd <- list()
STATEcomp_WAd <- list()
for(i in c(1:length(STATEprod))){
  ind = match(names(STATEprod)[i],names(STATEpowlist_INd))
  STATEcomp_NNd[[i]] <- csl(STATEpowlist_NNd[[ind]],STATEprod[[i]])
  STATEcomp_INd[[i]] <- csl(STATEpowlist_INd[[ind]],STATEprod[[i]])
  STATEcomp_WAd[[i]] <- csl(STATEpowlist_WAd[[ind]],STATEprod[[i]])
}
SUBcomp_NNd <- list()
SUBcomp_INd <- list()
SUBcomp_WAd <- list()
for(i in c(1:length(SUBpowlist_INd))){
  SUBcomp_NNd[[i]] <- csl(SUBpowlist_NNd[[i]],SUBprod[[i]])
  SUBcomp_INd[[i]] <- csl(SUBpowlist_INd[[i]],SUBprod[[i]])
  SUBcomp_WAd[[i]] <- csl(SUBpowlist_WAd[[i]],SUBprod[[i]])
}
Bcomp_NNd <- csl(Bpowlist_NNd,Bprod)
Bcomp_INd <- csl(Bpowlist_INd,Bprod)
Bcomp_WAd <- csl(Bpowlist_WAd,Bprod)

save(statcomp_NNd,statcomp_INd,statcomp_WAd,STATEcomp_NNd,STATEcomp_INd,STATEcomp_WAd,SUBcomp_NNd,SUBcomp_INd,SUBcomp_WAd,Bcomp_NNd,Bcomp_INd,Bcomp_WAd,file=paste0(dirresultscapc,"/comp_step2.RData"))

##### analyse results for brazil, subsystems, states and wind parks #####
# Brasil
Bcors <- round(c(cor(Bcomp_NNd[,2],Bcomp_NNd[,3]),cor(Bcomp_INd[,2],Bcomp_INd[,3]),cor(Bcomp_WAd[,2],Bcomp_WAd[,3])),3)
Brmse <- round(c(rmse(Bcomp_NNd[,2],Bcomp_NNd[,3]),rmse(Bcomp_INd[,2],Bcomp_INd[,3]),rmse(Bcomp_WAd[,2],Bcomp_WAd[,3])),3)
# considered R² but not useful as values >1 are obtained
Bmbe <- round(c(mean(Bcomp_NNd[,2]-Bcomp_NNd[,3]),mean(Bcomp_INd[,2]-Bcomp_INd[,3]),mean(Bcomp_WAd[,2]-Bcomp_WAd[,3])),3)
Bmeans <- round(c(mean(Bcomp_NNd[,2]),mean(Bcomp_INd[,2]),mean(Bcomp_WAd[,2]),mean(Bcomp_INd[,3])),3)
Bstats <- data.frame(int=c("NN","INMET","GWA","obs"),cor=c(Bcors,NA),rmse=c(Brmse,NA),mbe=c(Bmbe,NA),mean=Bmeans)
save(Bstats,file=paste0(dirresultscapc,"/Bstats_wsma.RData"))
write.table(Bstats,paste(dirresultscapc,"/Bstats_wsma.csv",sep=""),sep=";")
# merge all data to a tibble
dat <- melt(data.frame(NN=Bcomp_NNd[,2]-Bcomp_INd[,3],INMET=Bcomp_INd[,2]-Bcomp_INd[,3],GWA=Bcomp_WAd[,2]-Bcomp_INd[,3]))
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  xlab("Data source for wind speed mean approximation") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirresultscapc,"/diff_B_wsma.png",sep=""), width = 6, height = 4.125)
# Subsystems
SUBstats <- list()
for(i in c(1:length(SUBcomp_INd))){
  SUBcors <- round(c(c(cor(SUBcomp_NNd[[i]][,2],SUBcomp_NNd[[i]][,3]),cor(SUBcomp_INd[[i]][,2],SUBcomp_INd[[i]][,3]),cor(SUBcomp_WAd[[i]][,2],SUBcomp_WAd[[i]][,3]))),3)
  SUBrmse <- round(c(rmse(SUBcomp_NNd[[i]][,2],SUBcomp_NNd[[i]][,3]),rmse(SUBcomp_INd[[i]][,2],SUBcomp_INd[[i]][,3]),rmse(SUBcomp_WAd[[i]][,2],SUBcomp_WAd[[i]][,3])),3)
  SUBmbe <- round(c(mean(SUBcomp_NNd[[i]][,2]-SUBcomp_NNd[[i]][,3]),mean(SUBcomp_INd[[i]][,2]-SUBcomp_INd[[i]][,3]),mean(SUBcomp_WAd[[i]][,2]-SUBcomp_WAd[[i]][,3])),3)
  SUBmeans <- round(c(mean(SUBcomp_NNd[[i]][,2]),mean(SUBcomp_INd[[i]][,2]),mean(SUBcomp_WAd[[i]][,2]),mean(SUBcomp_INd[[i]][,3])),3)
  stat <- data.frame(int=c("NN","INMET","GWA","obs"),cor=c(SUBcors,NA),rmse=c(SUBrmse,NA),mbe=c(SUBmbe,NA),mean=SUBmeans)
  SUBstats[[i]] <- stat
}
names(SUBstats) <- c("NE","S")
save(SUBstats,file=paste0(dirresultscapc,"/SUBstats_wsma.RData"))
write.list2(SUBstats,paste(dirresultscapc,"/SUBstats_wsma.csv",sep=""))
dat <- NULL
for(i in c(1:length(SUBcomp_INd))){
  # merge all data to a tibble
  dat1 <- melt(data.frame(NN=SUBcomp_NNd[[i]][,2]-SUBcomp_INd[[i]][,3],INMET=SUBcomp_INd[[i]][,2]-SUBcomp_INd[[i]][,3],GWA=SUBcomp_WAd[[i]][,2]-SUBcomp_INd[[i]][,3]))
  dat1 <- data.frame(subsystem=names(SUBstats)[i],dat1)
  if(length(dat)>1){
    dat <- rbind(dat,dat1)
  }else{
    dat <- dat1
  }
}
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  facet_wrap(~subsystem) +
  xlab("Data source for wind speed mean approximation") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirresultscapc,"/diffs_SUB_wsma.png",sep=""), width = 6, height = 4.125)
# States
STATEstats <- list()
for(i in c(1:length(STATEcomp_INd))){
  STATEcors <- round(c(c(cor(STATEcomp_NNd[[i]][,2],STATEcomp_NNd[[i]][,3]),cor(STATEcomp_INd[[i]][,2],STATEcomp_INd[[i]][,3]),cor(STATEcomp_WAd[[i]][,2],STATEcomp_WAd[[i]][,3]))),3)
  STATErmse <- round(c(rmse(STATEcomp_NNd[[i]][,2],STATEcomp_NNd[[i]][,3]),rmse(STATEcomp_INd[[i]][,2],STATEcomp_INd[[i]][,3]),rmse(STATEcomp_WAd[[i]][,2],STATEcomp_WAd[[i]][,3])),3)
  STATEmbe <- round(c(mean(STATEcomp_NNd[[i]][,2]-STATEcomp_NNd[[i]][,3]),mean(STATEcomp_INd[[i]][,2]-STATEcomp_INd[[i]][,3]),mean(STATEcomp_WAd[[i]][,2]-STATEcomp_WAd[[i]][,3])),3)
  STATEmeans <- round(c(mean(STATEcomp_NNd[[i]][,2]),mean(STATEcomp_INd[[i]][,2]),mean(STATEcomp_WAd[[i]][,2]),mean(STATEcomp_INd[[i]][,3])),3)
  stat <- data.frame(int=c("NN","INMET","GWA","obs"),cor=c(STATEcors,NA),rmse=c(STATErmse,NA),mbe=c(STATEmbe,NA),mean=STATEmeans)
  STATEstats[[i]] <- stat
}
names(STATEstats) <- names(STATEprod)
save(STATEstats,file=paste0(dirresultscapc,"/STATEstats_wsma.RData"))
write.list2(STATEstats,paste(dirresultscapc,"/STATEstats_wsma.csv",sep=""))
dat <- NULL
for(i in c(1:length(STATEcomp_INd))){
  # merge all data to a tibble
  dat1 <- melt(data.frame(NN=STATEcomp_NNd[[i]][,2]-STATEcomp_INd[[i]][,3],INMET=STATEcomp_INd[[i]][,2]-STATEcomp_INd[[i]][,3],GWA=STATEcomp_WAd[[i]][,2]-STATEcomp_INd[[i]][,3]))
  dat1 <- data.frame(state=names(STATEstats)[i],dat1)
  if(length(dat)>1){
    dat <- rbind(dat,dat1)
  }else{
    dat <- dat1
  }
}
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  facet_wrap(~state,nrow=2) +
  xlab("Data source for wind speed mean approximation") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirresultscapc,"/diff_STATE_wsma.png",sep=""), width = 9, height = 4.125)
# Wind power plants
statstats <- list()
for(i in c(1:length(statcomp_INd))){
  statcors <- round(c(c(cor(statcomp_NNd[[i]][,2],statcomp_NNd[[i]][,3]),cor(statcomp_INd[[i]][,2],statcomp_INd[[i]][,3]),cor(statcomp_WAd[[i]][,2],statcomp_WAd[[i]][,3]))),3)
  statrmse <- round(c(rmse(statcomp_NNd[[i]][,2],statcomp_NNd[[i]][,3]),rmse(statcomp_INd[[i]][,2],statcomp_INd[[i]][,3]),rmse(statcomp_WAd[[i]][,2],statcomp_WAd[[i]][,3])),3)
  statmbe <- round(c(mean(statcomp_NNd[[i]][,2]-statcomp_NNd[[i]][,3]),mean(statcomp_INd[[i]][,2]-statcomp_INd[[i]][,3]),mean(statcomp_WAd[[i]][,2]-statcomp_WAd[[i]][,3])),3)
  statmeans <- round(c(mean(statcomp_NNd[[i]][,2]),mean(statcomp_INd[[i]][,2]),mean(statcomp_WAd[[i]][,2]),mean(statcomp_INd[[i]][,3])),3)
  stat <- data.frame(int=c("NN","IN","WA","obs"),cor=c(statcors,NA),rmse=c(statrmse,NA),mbe=c(statmbe,NA),mean=statmeans)
  statstats[[i]] <- stat
}
names(statstats) <- c("RS-ElebrasCidreira1","SC-BomJardim","BA-Macaubas","CE-PraiaFormosa","PI-Araripe","PE-SaoClemente","RN-AlegriaII")
save(statstats,file=paste0(dirresultscapc,"/statstats_wsma.RData"))
write.list2(statstats,paste(dirresultscapc,"/statstats_wsma.csv",sep=""))
dat <- NULL
for(i in c(1:length(statcomp_INd))){
  # merge all data to a tibble
  dat1 <- melt(data.frame(NN=statcomp_NNd[[i]][,2]-statcomp_INd[[i]][,3],INMET=statcomp_INd[[i]][,2]-statcomp_INd[[i]][,3],GWA=statcomp_WAd[[i]][,2]-statcomp_INd[[i]][,3]))
  dat1 <- data.frame(state=names(statstats)[i],dat1)
  if(length(dat)>1){
    dat <- rbind(dat,dat1)
  }else{
    dat <- dat1
  }
}
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  facet_wrap(~state,nrow=2) +
  xlab("Data source for wind speed mean approximation") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirresultscapc,"/diff_stat_wsma.png",sep=""), width = 9, height = 4.125)





















##########################################################################################
############ STEP 3: COMPARISON OF DIFFERENT WIND SPEED CORRECTION METHODS: ##############
############ MONTHLY, MONTHLY + HOURLY, ONLY MEAN APPROXIMATION ##########################
##########################################################################################
# determining factor whether wind speed correction is carried out:
# limit for max distance to INMET station: 80 km
INmaxdist <- 80
# limit for minimum correlation for wind speed correction: 50%
corrlimit <- 0.5
# Nearest Neighbour
method=1

# prepare data frame for removal of data with insufficient quality
# How many months per month (Jan,Feb,...) need to be complete?
minmonth = 4
# how many days is a month required to contain in order to be "complete"?
mindaynum = 30
# how many months are allowed to have less than mindaynum days?
monthlim = 1
# how many days does a month need to be long enough that its data are respected?
shortmonths = 10
remove_months(minmonth,mindaynum,monthlim,shortmonths,rmrows=1)

# calculate correction factors
LonLat <- read_feather(paste(dirmerra,"/lonlat.feather",sep=""))
date.start <- as.POSIXct("1999-01-01",tz="UTC")
rad <- pi/180
hubheight <- 10
calccfs_r()

save(cfhm_r,cfm_r,corhm_r,corm_r,file=paste(dirresults,"/cfscors_r.RData",sep=""))




# calculate wind power generation with wind speed correction
### ENERCON E-82
# datasheet (p7-10):http://www.enercon.de/fileadmin/Redakteur/Medien-Portal/broschueren/pdf/en/ENERCON_Produkt_en_06_2015.pdf
# power 0 at 0 wind speed is added
# selected rated power: 2000kW, selected height: 108m
# power curve: windspeed [m/s] and power output [kW]
ratedpower <- 2000
height <- 108
windspeed <- c(0:25)
powercurve <- c(0,0,3,25,82,174,312,532,815,1180,1580,1810,1980,rep(2050,13))

# hourly and monthly
statpowlist <- calcstatpower_windcor(ratedpower,height,windspeed,powercurve,INmaxdist,corrlimit,method,selection="all",mhm="hm")
setwd(dirresults)
save(statpowlist,file="statpowlist_wschm.RData") # wschm = wind speed correction hourly and monthly
# sum up power generation per state
makeSTATEpowlist("wschm")

# monthly
statpowlist <- calcstatpower_windcor(ratedpower,height,windspeed,powercurve,INmaxdist,corrlimit,method,selection="all",mhm="m")
setwd(dirresults)
save(statpowlist,file="statpowlist_wscm.RData") # wscm = wind speed correction monthly
# sum up power generation per state
makeSTATEpowlist("wscm")








##### prepare results for brazil, subsystems and states #####
load(paste(dirresults,"/statpowlist_wsmaWA_sel.RData",sep=""))
statpowlist_wsma <- statpowlist
load(paste(dirresults,"/STATEpowlist_wschm.RData",sep=""))
STATEpowlist_wschm <- STATEpowlist
load(paste(dirresults,"/STATEpowlist_wscm.RData",sep=""))
STATEpowlist_wscm <- STATEpowlist
load(paste(dirresults,"/STATEpowlist_wsmaWA.RData",sep=""))
STATEpowlist_wsma <- STATEpowlist
rm(statpowlist,STATEpowlist)

# sum for NE and S
SUBpowlist_wschm <- sum_subsystem(STATEpowlist_wschm)
SUBpowlist_wscm <- sum_subsystem(STATEpowlist_wscm)
SUBpowlist_wsma <- sum_subsystem(STATEpowlist_wsma)
# sum for Brazil
Bpowlist_wschm <- sum_brasil(STATEpowlist_wschm)
Bpowlist_wscm <- sum_brasil(STATEpowlist_wscm)
Bpowlist_wsma <- sum_brasil(STATEpowlist_wsma)
# aggregate daily
STATEpowlist_wschm_d <- dailyaggregate(STATEpowlist_wschm)
STATEpowlist_wscm_d <- dailyaggregate(STATEpowlist_wscm)
STATEpowlist_wsma_d <- dailyaggregate(STATEpowlist_wsma)
SUBpowlist_wschm_d <- dailyaggregate(SUBpowlist_wschm)
SUBpowlist_wscm_d <- dailyaggregate(SUBpowlist_wscm)
SUBpowlist_wsma_d <- dailyaggregate(SUBpowlist_wsma)
Bpowlist_wschm_d <- aggregate(Bpowlist_wschm[,2],by=list(format(Bpowlist_wschm[,1],"%Y%m%d")),sum)
Bpowlist_wscm_d <- aggregate(Bpowlist_wscm[,2],by=list(format(Bpowlist_wscm[,1],"%Y%m%d")),sum)
Bpowlist_wsma_d <- aggregate(Bpowlist_wsma[,2],by=list(format(Bpowlist_wsma[,1],"%Y%m%d")),sum)
# cut to startdate (first day with wind power > 0)
for(i in c(1:length(STATEpowlist_wschm_d))){
  STATEpowlist_wschm_d[[i]] <- STATEpowlist_wschm_d[[i]][which(STATEpowlist_wschm_d[[i]][,2]>0)[1]:length(STATEpowlist_wschm_d[[i]][,2]),]
  STATEpowlist_wscm_d[[i]] <- STATEpowlist_wscm_d[[i]][which(STATEpowlist_wscm_d[[i]][,2]>0)[1]:length(STATEpowlist_wscm_d[[i]][,2]),]
  STATEpowlist_wsma_d[[i]] <- STATEpowlist_wsma_d[[i]][which(STATEpowlist_wsma_d[[i]][,2]>0)[1]:length(STATEpowlist_wsma_d[[i]][,2]),]
}
# add names of states
names(STATEpowlist_wschm_d) <- c("Bahia","Ceará","Maranhão","MinasGerais","Paraíba","Paraná","Pernambuco","Piaui","RiodeJaneiro","RioGrandedoNorte","RioGrandedoSul","SantaCatarina","Sergipe")
names(STATEpowlist_wscm_d) <- c("Bahia","Ceará","Maranhão","MinasGerais","Paraíba","Paraná","Pernambuco","Piaui","RiodeJaneiro","RioGrandedoNorte","RioGrandedoSul","SantaCatarina","Sergipe")
names(STATEpowlist_wsma_d) <- c("Bahia","Ceará","Maranhão","MinasGerais","Paraíba","Paraná","Pernambuco","Piaui","RiodeJaneiro","RioGrandedoNorte","RioGrandedoSul","SantaCatarina","Sergipe")

# load measured wind power
# states
STATEprod <- list()
ct <- NULL
for(i in c(1:length(STATEpowlist_wschm_d))){
  sp <- getSTATEproddaily(names(STATEpowlist_wschm_d)[i])
  if(length(sp)>0){
    STATEprod[[length(STATEprod)+1]] <- sp
    ct <- c(ct,i)
  }
}
names(STATEprod) <- names(STATEpowlist_wschm_d)[ct]
# subsystems
SUBprod <- list()
SUBprod[[1]] <- getprodSUBBRA("NE")
SUBprod[[2]] <- getprodSUBBRA("S")
# Brazil
Bprod <- getprodSUBBRA("BRASIL")
save(STATEpowlist_wschm_d,STATEpowlist_wscm_d,STATEpowlist_wsma_d,SUBpowlist_wschm_d,SUBpowlist_wscm_d,SUBpowlist_wsma_d,Bpowlist_wschm_d,Bpowlist_wscm_d,Bpowlist_wsma_d,statprod,STATEprod,SUBprod,Bprod,file=paste(dirresults,"/comp_wsc.RData",sep=""))


load(paste(dirresults,"/comp_wsc.RData",sep=""))
# convert calculated wind power from kWh to GWh
for(i in c(1:length(STATEpowlist_wschm_d))){
  STATEpowlist_wschm_d[[i]][,2] <- STATEpowlist_wschm_d[[i]][,2]/10^6
  STATEpowlist_wscm_d[[i]][,2] <- STATEpowlist_wscm_d[[i]][,2]/10^6
  STATEpowlist_wsma_d[[i]][,2] <- STATEpowlist_wsma_d[[i]][,2]/10^6
}
for(i in c(1:length(SUBpowlist_wschm_d))){
  SUBpowlist_wschm_d[[i]][,2] <- SUBpowlist_wschm_d[[i]][,2]/10^6
  SUBpowlist_wscm_d[[i]][,2] <- SUBpowlist_wscm_d[[i]][,2]/10^6
  SUBpowlist_wsma_d[[i]][,2] <- SUBpowlist_wsma_d[[i]][,2]/10^6
}
Bpowlist_wschm_d[,2] <- Bpowlist_wschm_d[,2]/10^6
Bpowlist_wscm_d[,2] <- Bpowlist_wscm_d[,2]/10^6
Bpowlist_wsma_d[,2] <- Bpowlist_wsma_d[,2]/10^6


# correct with capacity correction factors
load(paste(dircaps,"/cap_cfs.RData",sep=""))
# NE: 1,2,3,5,7,8,10,13
# S: 6,11,12
# SE: 4,9
for(i in c(1,2,3,5,7,8,10,13)){
  STATEpowlist_wschm_d[[i]][,2] <- STATEpowlist_wschm_d[[i]][,2]*cfNE
  STATEpowlist_wscm_d[[i]][,2] <- STATEpowlist_wscm_d[[i]][,2]*cfNE
  STATEpowlist_wsma_d[[i]][,2] <- STATEpowlist_wsma_d[[i]][,2]*cfNE
}
for(i in c(6,11,12)){
  STATEpowlist_wschm_d[[i]][,2] <- STATEpowlist_wschm_d[[i]][,2]*cfS
  STATEpowlist_wscm_d[[i]][,2] <- STATEpowlist_wscm_d[[i]][,2]*cfS
  STATEpowlist_wsma_d[[i]][,2] <- STATEpowlist_wsma_d[[i]][,2]*cfS
}
for(i in c(4,9)){
  STATEpowlist_wschm_d[[i]][,2] <- STATEpowlist_wschm_d[[i]][,2]*cfB
  STATEpowlist_wscm_d[[i]][,2] <- STATEpowlist_wscm_d[[i]][,2]*cfB
  STATEpowlist_wsma_d[[i]][,2] <- STATEpowlist_wsma_d[[i]][,2]*cfB
}
#subsystems
SUBpowlist_wschm_d[[1]][,2] <- SUBpowlist_wschm_d[[1]][,2]*cfNE
SUBpowlist_wscm_d[[1]][,2] <- SUBpowlist_wscm_d[[1]][,2]*cfNE
SUBpowlist_wsma_d[[1]][,2] <- SUBpowlist_wsma_d[[1]][,2]*cfNE
SUBpowlist_wschm_d[[2]][,2] <- SUBpowlist_wschm_d[[2]][,2]*cfS
SUBpowlist_wscm_d[[2]][,2] <- SUBpowlist_wscm_d[[2]][,2]*cfS
SUBpowlist_wsma_d[[2]][,2] <- SUBpowlist_wsma_d[[2]][,2]*cfS
# brazil
Bpowlist_wschm_d[,2] <- Bpowlist_wschm_d[,2]*cfB
Bpowlist_wscm_d[,2] <- Bpowlist_wscm_d[,2]*cfB
Bpowlist_wsma_d[,2] <- Bpowlist_wsma_d[,2]*cfB


# cut to same lengths
STATEcomp_wschm_d <- list()
STATEcomp_wscm_d <- list()
STATEcomp_wsma_d <- list()
for(i in c(1:length(STATEprod))){
  ind = match(names(STATEprod)[i],names(STATEpowlist_wschm_d))
  STATEcomp_wschm_d[[i]] <- csl(STATEpowlist_wschm_d[[ind]],STATEprod[[i]])
  STATEcomp_wscm_d[[i]] <- csl(STATEpowlist_wscm_d[[ind]],STATEprod[[i]])
  STATEcomp_wsma_d[[i]] <- csl(STATEpowlist_wsma_d[[ind]],STATEprod[[i]])
}
SUBcomp_wschm_d <- list()
SUBcomp_wscm_d <- list()
SUBcomp_wsma_d <- list()
for(i in c(1:length(SUBpowlist_wschm_d))){
  SUBcomp_wschm_d[[i]] <- csl(SUBpowlist_wschm_d[[i]],SUBprod[[i]])
  SUBcomp_wscm_d[[i]] <- csl(SUBpowlist_wscm_d[[i]],SUBprod[[i]])
  SUBcomp_wsma_d[[i]] <- csl(SUBpowlist_wsma_d[[i]],SUBprod[[i]])
}
Bcomp_wschm_d <- csl(Bpowlist_wschm_d,Bprod)
Bcomp_wscm_d <- csl(Bpowlist_wscm_d,Bprod)
Bcomp_wsma_d <- csl(Bpowlist_wsma_d,Bprod)



save(STATEcomp_wschm_d,STATEcomp_wscm_d,STATEcomp_wsma_d,SUBcomp_wschm_d,SUBcomp_wscm_d,SUBcomp_wsma_d,Bcomp_wschm_d,Bcomp_wscm_d,Bcomp_wsma_d,file=paste(dirresultscapc,"/comp_csl_wsc.RData",sep=""))




load(paste(dirresultscapc,"/comp_csl_wsc.RData",sep=""))

##### analyse results for brazil, subsystems and states #####
# Brasil
Bcors <- round(c(cor(Bcomp_wschm_d[,2],Bcomp_wschm_d[,3]),cor(Bcomp_wscm_d[,2],Bcomp_wscm_d[,3]),cor(Bcomp_wsma_d[,2],Bcomp_wsma_d[,3])),3)
Brmse <- round(c(rmse(Bcomp_wschm_d[,2],Bcomp_wschm_d[,3]),rmse(Bcomp_wscm_d[,2],Bcomp_wscm_d[,3]),rmse(Bcomp_wsma_d[,2],Bcomp_wsma_d[,3])),3)
# considered R² but not useful as values >1 are obtained
Bmbe <- round(c(mean(Bcomp_wschm_d[,2]-Bcomp_wschm_d[,3]),mean(Bcomp_wscm_d[,2]-Bcomp_wscm_d[,3]),mean(Bcomp_wsma_d[,2]-Bcomp_wsma_d[,3])),3)
Bmeans <- round(c(mean(Bcomp_wschm_d[,2]),mean(Bcomp_wscm_d[,2]),mean(Bcomp_wsma_d[,2]),mean(Bcomp_wschm_d[,3])),3)
Bstats <- data.frame(int=c("wsc_hm","wsc_m","wsma","obs"),cor=c(Bcors,NA),rmse=c(Brmse,NA),mbe=c(Bmbe,NA),mean=Bmeans)
save(Bstats,file=paste0(dirresultscapc,"/Bstats_wsc.RData"))
write.table(Bstats,paste(dirresultscapc,"/Bstats_wsc.csv",sep=""),sep=";")
# merge all data to a tibble
dat <- melt(data.frame(GWA_hm=Bcomp_wschm_d[,2]-Bcomp_wschm_d[,3],GWA_m=Bcomp_wscm_d[,2]-Bcomp_wschm_d[,3],GWA=Bcomp_wsma_d[,2]-Bcomp_wschm_d[,3]))
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  xlab("Wind speed correction method") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirresultscapc,"/diff_B_wsc.png",sep=""), width = 6, height = 4.125)
# Subsystems
SUBstats <- list()
for(i in c(1:length(SUBcomp_wschm_d))){
  SUBcors <- round(c(c(cor(SUBcomp_wschm_d[[i]][,2],SUBcomp_wschm_d[[i]][,3]),cor(SUBcomp_wscm_d[[i]][,2],SUBcomp_wscm_d[[i]][,3]),cor(SUBcomp_wsma_d[[i]][,2],SUBcomp_wsma_d[[i]][,3]))),3)
  SUBrmse <- round(c(rmse(SUBcomp_wschm_d[[i]][,2],SUBcomp_wschm_d[[i]][,3]),rmse(SUBcomp_wscm_d[[i]][,2],SUBcomp_wscm_d[[i]][,3]),rmse(SUBcomp_wsma_d[[i]][,2],SUBcomp_wsma_d[[i]][,3])),3)
  SUBmbe <- round(c(mean(SUBcomp_wschm_d[[i]][,2]-SUBcomp_wscm_d[[i]][,3]),mean(SUBcomp_wscm_d[[i]][,2]-SUBcomp_wscm_d[[i]][,3]),mean(SUBcomp_wsma_d[[i]][,2]-SUBcomp_wsma_d[[i]][,3])),3)
  SUBmeans <- round(c(mean(SUBcomp_wschm_d[[i]][,2]),mean(SUBcomp_wscm_d[[i]][,2]),mean(SUBcomp_wsma_d[[i]][,2]),mean(SUBcomp_wschm_d[[i]][,3])),3)
  stat <- data.frame(int=c("wsc_hm","wsc_m","wsma","obs"),cor=c(SUBcors,NA),rmse=c(SUBrmse,NA),mbe=c(SUBmbe,NA),mean=SUBmeans)
  SUBstats[[i]] <- stat
}
names(SUBstats) <- c("NE","S")
save(SUBstats,file=paste0(dirresultscapc,"/SUBstats_wsc.RData"))
write.list2(SUBstats,paste(dirresultscapc,"/SUBstats_wsc.csv",sep=""))
dat <- NULL
for(i in c(1:length(SUBcomp_wschm_d))){
  # merge all data to a tibble
  dat1 <- melt(data.frame(GWA_hm=SUBcomp_wschm_d[[i]][,2]-SUBcomp_wschm_d[[i]][,3],GWA_m=SUBcomp_wscm_d[[i]][,2]-SUBcomp_wschm_d[[i]][,3],GWA=SUBcomp_wsma_d[[i]][,2]-SUBcomp_wschm_d[[i]][,3]))
  dat1 <- data.frame(subsystem=names(SUBstats)[i],dat1)
  if(length(dat)>1){
    dat <- rbind(dat,dat1)
  }else{
    dat <- dat1
  }
}
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  facet_wrap(~subsystem) +
  xlab("Wind speed correction method") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirresultscapc,"/diff_SUB_wsc.png",sep=""), width = 6, height = 4.125)
# States
STATEstats <- list()
for(i in c(1:length(STATEcomp_wschm_d))){
  STATEcors <- round(c(c(cor(STATEcomp_wschm_d[[i]][,2],STATEcomp_wschm_d[[i]][,3]),cor(STATEcomp_wscm_d[[i]][,2],STATEcomp_wscm_d[[i]][,3]),cor(STATEcomp_wsma_d[[i]][,2],STATEcomp_wsma_d[[i]][,3]))),3)
  STATErmse <- round(c(rmse(STATEcomp_wschm_d[[i]][,2],STATEcomp_wschm_d[[i]][,3]),rmse(STATEcomp_wscm_d[[i]][,2],STATEcomp_wscm_d[[i]][,3]),rmse(STATEcomp_wsma_d[[i]][,2],STATEcomp_wsma_d[[i]][,3])),3)
  STATEmbe <- round(c(mean(STATEcomp_wschm_d[[i]][,2]-STATEcomp_wschm_d[[i]][,3]),mean(STATEcomp_wscm_d[[i]][,2]-STATEcomp_wscm_d[[i]][,3]),mean(STATEcomp_wsma_d[[i]][,2]-STATEcomp_wsma_d[[i]][,3])),3)
  STATEmeans <- round(c(mean(STATEcomp_wschm_d[[i]][,2]),mean(STATEcomp_wscm_d[[i]][,2]),mean(STATEcomp_wsma_d[[i]][,2]),mean(STATEcomp_wschm_d[[i]][,3])),3)
  stat <- data.frame(int=c("wsc_hm","wsc_m","wsma","obs"),cor=c(STATEcors,NA),rmse=c(STATErmse,NA),mbe=c(STATEmbe,NA),mean=STATEmeans)
  STATEstats[[i]] <- stat
}
names(STATEstats) <- names(STATEprod)
save(STATEstats,file=paste0(dirresultscapc,"/STATEstats_wsc.RData"))
write.list2(STATEstats,paste(dirresultscapc,"/STATEstats_wsc.csv",sep=""))
dat <- NULL
for(i in c(1:length(STATEcomp_wschm_d))){
  # merge all data to a tibble
  dat1 <- melt(data.frame(GWA_hm=STATEcomp_wschm_d[[i]][,2]-STATEcomp_wschm_d[[i]][,3],GWA_m=STATEcomp_wscm_d[[i]][,2]-STATEcomp_wschm_d[[i]][,3],GWA=STATEcomp_wsma_d[[i]][,2]-STATEcomp_wschm_d[[i]][,3]))
  dat1 <- data.frame(state=names(STATEstats)[i],dat1)
  if(length(dat)>1){
    dat <- rbind(dat,dat1)
  }else{
    dat <- dat1
  }
}
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  facet_wrap(~state,nrow=2) +
  xlab("Wind speed correction method") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirresultscapc,"/diff_STATE_wsc.png",sep=""), width = 9, height = 4.125)






###################### ALWAYS APPLY CORRECTION FOR SINGLE WIND PARKS ######################
###################### BECAUSE OTHERWISE HARDLY WIND SPEED CORRECTION #####################
method=1
INmaxdist <- 80
corrlimit <- 0.5
minmonth = 4
mindaynum = 30
monthlim = 1
shortmonths = 10
LonLat <- read_feather(paste(dirmerra,"/lonlat.feather",sep=""))
date.start <- as.POSIXct("1999-01-01",tz="UTC")
rad <- pi/180
hubheight <- 10
ratedpower <- 2000
height <- 108
windspeed <- c(0:25)
powercurve <- c(0,0,3,25,82,174,312,532,815,1180,1580,1810,1980,rep(2050,13))


# first calculate mean correction factors again (for all, so that none are 1)

# hourly and monthly
statpowlist <- calcstatpower_windcor(ratedpower,height,windspeed,powercurve,INmaxdist,corrlimit,method,selection="sel",mhm="hm",applylim=0)
names(statpowlist) <- c("RS","SC","BA","CE","PI","PE","RN")
setwd(dirresults)
save(statpowlist,file="statpowlist_wschm_sel_allc.RData")

# monthly
statpowlist <- calcstatpower_windcor(ratedpower,height,windspeed,powercurve,INmaxdist,corrlimit,method,selection="sel",mhm="m",applylim=0)
names(statpowlist) <- c("RS","SC","BA","CE","PI","PE","RN")
setwd(dirresults)
save(statpowlist,file="statpowlist_wscm_sel_allc.RData")

##### prepare results for wind parks #####
load(paste(dirresults,"/statpowlist_wschm_sel_allc.RData",sep=""))
statpowlist_wschm = statpowlist
load(paste(dirresults,"/statpowlist_wscm_sel_allc.RData",sep=""))
statpowlist_wscm <- statpowlist
load(paste(dirresults,"/statpowlist_wsmaWA_sel.RData",sep=""))
statpowlist_wsma <- statpowlist
rm(statpowlist)

# aggregate daily
statpowlist_wschm_d <- dailyaggregate(statpowlist_wschm)
statpowlist_wscm_d <- dailyaggregate(statpowlist_wscm)
statpowlist_wsma_d <- dailyaggregate(statpowlist_wsma)
# cut to startdate (first day with wind power > 0)
for(i in c(1:length(statpowlist_wschm_d))){
  statpowlist_wschm_d[[i]] <- statpowlist_wschm_d[[i]][which(statpowlist_wschm_d[[i]][,2]>0)[1]:length(statpowlist_wschm_d[[i]][,2]),]
  statpowlist_wscm_d[[i]] <- statpowlist_wscm_d[[i]][which(statpowlist_wscm_d[[i]][,2]>0)[1]:length(statpowlist_wscm_d[[i]][,2]),]
  statpowlist_wsma_d[[i]] <- statpowlist_wsma_d[[i]][which(statpowlist_wsma_d[[i]][,2]>0)[1]:length(statpowlist_wsma_d[[i]][,2]),]
}
# add names of states
names(statpowlist_wschm_d) <- names(statpowlist_wschm)
names(statpowlist_wscm_d) <- names(statpowlist_wscm)
names(statpowlist_wsma_d) <- names(statpowlist_wsma)

# load measured wind power
# stations
statprod <- list()
for(i in c(1:length(statpowlist_wschm_d))){
  statprod[[i]] <- getstatproddaily(names(statpowlist_wschm_d)[i])
}
names(statprod) <- names(statpowlist_wschm_d)

# convert calculated wind power from kWh to GWh
for(i in c(1:length(statpowlist_wschm_d))){
  statpowlist_wschm_d[[i]][,2] <- statpowlist_wschm_d[[i]][,2]/10^6
  statpowlist_wscm_d[[i]][,2] <- statpowlist_wscm_d[[i]][,2]/10^6
  statpowlist_wsma_d[[i]][,2] <- statpowlist_wsma_d[[i]][,2]/10^6
}

# cut to same lengths
statcomp_wschm_d <- list()
statcomp_wscm_d <- list()
statcomp_wsma_d <- list()
for(i in c(1:length(statpowlist_wschm_d))){
  statcomp_wschm_d[[i]] <- csl(statpowlist_wschm_d[[i]],statprod[[i]])
  statcomp_wscm_d[[i]] <- csl(statpowlist_wscm_d[[i]],statprod[[i]])
  statcomp_wsma_d[[i]] <- csl(statpowlist_wsma_d[[i]],statprod[[i]])
}

save(statcomp_wschm_d,statcomp_wscm_d,statcomp_wsma_d,file=paste(dirresults,"/comp_csl_wsc_sel_allc.RData",sep=""))

load(paste(dirresults,"/comp_csl_wsc_sel_allc.RData",sep=""))


# correct with capacity correction factors
load(paste(dircaps,"/cap_cfs.RData",sep=""))
# South
for(i in c(1,2)){
  statcomp_wschm_d[[i]][,2] <- statcomp_wschm_d[[i]][,2]*cfS
  statcomp_wscm_d[[i]][,2] <- statcomp_wscm_d[[i]][,2]*cfS
  statcomp_wsma_d[[i]][,2] <- statcomp_wsma_d[[i]][,2]*cfS
}
# Northeast
for(i in c(3:7)){
  statcomp_wschm_d[[i]][,2] <- statcomp_wschm_d[[i]][,2]*cfNE
  statcomp_wscm_d[[i]][,2] <- statcomp_wscm_d[[i]][,2]*cfNE
  statcomp_wsma_d[[i]][,2] <- statcomp_wsma_d[[i]][,2]*cfNE
}

##### analyse results for wind parks #####

# Wind power plants
statstats <- list()
for(i in c(1:length(statcomp_wschm_d))){
  statcors <- round(c(c(cor(statcomp_wschm_d[[i]][,2],statcomp_wschm_d[[i]][,3]),cor(statcomp_wscm_d[[i]][,2],statcomp_wscm_d[[i]][,3]),cor(statcomp_wsma_d[[i]][,2],statcomp_wsma_d[[i]][,3]))),3)
  statrmse <- round(c(rmse(statcomp_wschm_d[[i]][,2],statcomp_wschm_d[[i]][,3]),rmse(statcomp_wscm_d[[i]][,2],statcomp_wscm_d[[i]][,3]),rmse(statcomp_wsma_d[[i]][,2],statcomp_wsma_d[[i]][,3])),3)
  statmbe <- round(c(mean(statcomp_wschm_d[[i]][,2]-statcomp_wschm_d[[i]][,3]),mean(statcomp_wscm_d[[i]][,2]-statcomp_wscm_d[[i]][,3]),mean(statcomp_wsma_d[[i]][,2]-statcomp_wsma_d[[i]][,3])),3)
  statmeans <- round(c(mean(statcomp_wschm_d[[i]][,2]),mean(statcomp_wscm_d[[i]][,2]),mean(statcomp_wsma_d[[i]][,2]),mean(statcomp_wschm_d[[i]][,3])),3)
  stat <- data.frame(int=c("wsc_hm","wsc_m","wsma","obs"),cor=c(statcors,NA),rmse=c(statrmse,NA),mbe=c(statmbe,NA),mean=statmeans)
  statstats[[i]] <- stat
}
names(statstats) <- c("RS-ElebrasCidreira1","SC-BomJardim","BA-Macaubas","CE-PraiaFormosa","PI-Araripe","PE-SaoClemente","RN-AlegriaII")
save(statstats,file=paste0(dirresultscapc,"/statstats_wsc_sel_allc.RData"))
write.list2(statstats,paste(dirresultscapc,"/statstats_wsc_sel_allc.csv",sep=""))
dat <- NULL
for(i in c(1:length(statcomp_wschm_d))){
  # merge all data to a tibble
  dat1 <- melt(data.frame(GWA_hm=statcomp_wschm_d[[i]][,2]-statcomp_wschm_d[[i]][,3],GWA_m=statcomp_wscm_d[[i]][,2]-statcomp_wschm_d[[i]][,3],GWA=statcomp_wsma_d[[i]][,2]-statcomp_wschm_d[[i]][,3]))
  dat1 <- data.frame(state=names(statstats)[i],dat1)
  if(length(dat)>1){
    dat <- rbind(dat,dat1)
  }else{
    dat <- dat1
  }
}
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  facet_wrap(~state,nrow=2) +
  xlab("Wind speed correction method") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirresultscapc,"/diff_stat_wsc_sel_allc.png",sep=""), width = 9, height = 4.125)








#####################################################################
####### CALCULATION OF MEAN CAPACITIES #############################
#####################################################################

# read wind park data
load(paste0(dirwindparks,"/windparks_complete.RData"))
load(paste0(dirwindparks_sel,"/selected_windparks.RData"))
Bwp <- windparks[,c(3,9,10,11)]
# extract only commissionings and capacities of NE and S
NEwp <- windparks[windparks$state %in% c("Bahia","Ceará","Paraíba","Pernambuco","Piaui","Rio Grande do Norte","Sergipe"),c(3,9,10,11)]
Swp <- windparks[windparks$state %in% c("Paraná","Santa Catarina","Rio Grande do Sul"),c(3,9,10,11)]
# extract commissioning dates per state
BAwp <- windparks[which(windparks$state=="Bahia"),c(3,9,10,11)]
CEwp <- windparks[which(windparks$state=="Ceará"),c(3,9,10,11)]
PEwp <- windparks[which(windparks$state=="Pernambuco"),c(3,9,10,11)]
PIwp <- windparks[which(windparks$state=="Piaui"),c(3,9,10,11)]
RNwp <- windparks[which(windparks$state=="Rio Grande do Norte"),c(3,9,10,11)]
RSwp <- windparks[which(windparks$state=="Rio Grande do Sul"),c(3,9,10,11)]
SCwp <- windparks[which(windparks$state=="Santa Catarina"),c(3,9,10,11)]
# extract commissioning dates per station
Mawp <- sel_windparks[grep("Macaúbas",sel_windparks$name),c(3,9,10,11)]
Prwp <- sel_windparks[grep("Praia Formosa",sel_windparks$name),c(3,9,10,11)]
Sawp <- sel_windparks[grep("São Clemente",sel_windparks$name),c(3,9,10,11)]
Arwp <- sel_windparks[grep("Araripe",sel_windparks$name),c(3,9,10,11)]
Alwp <- sel_windparks[grep("Alegria",sel_windparks$name),c(3,9,10,11)]
Elwp <- sel_windparks[grep("Elebrás Cidreira",sel_windparks$name),c(3,9,10,11)]
Bowp <- sel_windparks[grep("Bom Jardim",sel_windparks$name),c(3,9,10,11)]

# create datetime commissioning dates from year month and day
Bwp_cd <- data.frame(cap=Bwp$cap,comdate=as.POSIXct(paste0(Bwp$year,"-",Bwp$month,"-",Bwp$day," 00:00:00"),tz="UTC"))
NEwp_cd <- data.frame(cap=NEwp$cap,comdate=as.POSIXct(paste0(NEwp$year,"-",NEwp$month,"-",NEwp$day," 00:00:00"),tz="UTC"))
Swp_cd <- data.frame(cap=Swp$cap,comdate=as.POSIXct(paste0(Swp$year,"-",Swp$month,"-",Swp$day," 00:00:00"),tz="UTC"))
BAwp_cd <- data.frame(cap=BAwp$cap,comdate=as.POSIXct(paste0(BAwp$year,"-",BAwp$month,"-",BAwp$day," 00:00:00"),tz="UTC"))
CEwp_cd <- data.frame(cap=CEwp$cap,comdate=as.POSIXct(paste0(CEwp$year,"-",CEwp$month,"-",CEwp$day," 00:00:00"),tz="UTC"))
PEwp_cd <- data.frame(cap=PEwp$cap,comdate=as.POSIXct(paste0(PEwp$year,"-",PEwp$month,"-",PEwp$day," 00:00:00"),tz="UTC"))
PIwp_cd <- data.frame(cap=PIwp$cap,comdate=as.POSIXct(paste0(PIwp$year,"-",PIwp$month,"-",PIwp$day," 00:00:00"),tz="UTC"))
RNwp_cd <- data.frame(cap=RNwp$cap,comdate=as.POSIXct(paste0(RNwp$year,"-",RNwp$month,"-",RNwp$day," 00:00:00"),tz="UTC"))
RSwp_cd <- data.frame(cap=RSwp$cap,comdate=as.POSIXct(paste0(RSwp$year,"-",RSwp$month,"-",RSwp$day," 00:00:00"),tz="UTC"))
SCwp_cd <- data.frame(cap=SCwp$cap,comdate=as.POSIXct(paste0(SCwp$year,"-",SCwp$month,"-",SCwp$day," 00:00:00"),tz="UTC"))
Mawp_cd <- data.frame(cap=Mawp$cap,comdate=as.POSIXct(paste0(Mawp$year,"-",Mawp$month,"-",Mawp$day," 00:00:00"),tz="UTC"))
Prwp_cd <- data.frame(cap=Prwp$cap,comdate=as.POSIXct(paste0(Prwp$year,"-",Prwp$month,"-",Prwp$day," 00:00:00"),tz="UTC"))
Sawp_cd <- data.frame(cap=Sawp$cap,comdate=as.POSIXct(paste0(Sawp$year,"-",Sawp$month,"-",Sawp$day," 00:00:00"),tz="UTC"))
Arwp_cd <- data.frame(cap=Arwp$cap,comdate=as.POSIXct(paste0(Arwp$year,"-",Arwp$month,"-",Arwp$day," 00:00:00"),tz="UTC"))
Alwp_cd <- data.frame(cap=Alwp$cap,comdate=as.POSIXct(paste0(Alwp$year,"-",Alwp$month,"-",Alwp$day," 00:00:00"),tz="UTC"))
Elwp_cd <- data.frame(cap=Elwp$cap,comdate=as.POSIXct(paste0(Elwp$year,"-",Elwp$month,"-",Elwp$day," 00:00:00"),tz="UTC"))
Bowp_cd <- data.frame(cap=Bowp$cap,comdate=as.POSIXct(paste0(Bowp$year,"-",Bowp$month,"-",Bowp$day," 00:00:00"),tz="UTC"))

# agregate by date to avoid multiple same date stamps
Bwp_ag <- aggregate(Bwp_cd$cap,by=list(Bwp_cd$comdate),sum)
NEwp_ag <- aggregate(NEwp_cd$cap,by=list(NEwp_cd$comdate),sum)
Swp_ag <- aggregate(Swp_cd$cap,by=list(Swp_cd$comdate),sum)
BAwp_ag <- aggregate(BAwp_cd$cap,by=list(BAwp_cd$comdate),sum)
CEwp_ag <- aggregate(CEwp_cd$cap,by=list(CEwp_cd$comdate),sum)
PEwp_ag <- aggregate(PEwp_cd$cap,by=list(PEwp_cd$comdate),sum)
PIwp_ag <- aggregate(PIwp_cd$cap,by=list(PIwp_cd$comdate),sum)
RNwp_ag <- aggregate(RNwp_cd$cap,by=list(RNwp_cd$comdate),sum)
RSwp_ag <- aggregate(RSwp_cd$cap,by=list(RSwp_cd$comdate),sum)
SCwp_ag <- aggregate(SCwp_cd$cap,by=list(SCwp_cd$comdate),sum)
Mawp_ag <- aggregate(Mawp_cd$cap,by=list(Mawp_cd$comdate),sum)
Prwp_ag <- aggregate(Prwp_cd$cap,by=list(Prwp_cd$comdate),sum)
Sawp_ag <- aggregate(Sawp_cd$cap,by=list(Sawp_cd$comdate),sum)
Arwp_ag <- aggregate(Arwp_cd$cap,by=list(Arwp_cd$comdate),sum)
Alwp_ag <- aggregate(Alwp_cd$cap,by=list(Alwp_cd$comdate),sum)
Elwp_ag <- aggregate(Elwp_cd$cap,by=list(Elwp_cd$comdate),sum)
Bowp_ag <- aggregate(Bowp_cd$cap,by=list(Bowp_cd$comdate),sum)

names(Bwp_ag) <- names(NEwp_ag) <- names(Swp_ag) <- names(BAwp_ag) <- names(CEwp_ag) <- names(PEwp_ag) <- names(PIwp_ag) <- names(RNwp_ag) <- names(RSwp_ag) <- names(SCwp_ag) <- names(Mawp_ag) <- names(Prwp_ag) <- names(Sawp_ag) <- names(Arwp_ag) <- names(Alwp_ag) <- names(Elwp_ag) <- names(Bowp_ag) <- c("comdate","cap")

# calculate cumulative installed capacities and divide by 1000 to get from kW to MW
Bcap_WP <- data.frame(commissioning=Bwp_ag$comdate,cap=cumsum(Bwp_ag$cap)/1000)
NEcap_WP <- data.frame(commissioning=NEwp_ag$comdate,cap=cumsum(NEwp_ag$cap)/1000)
Scap_WP <- data.frame(commissioning=Swp_ag$comdate,cap=cumsum(Swp_ag$cap)/1000)
BAcap_WP <- data.frame(commissioning=BAwp_ag$comdate,cap=cumsum(BAwp_ag$cap)/1000)
CEcap_WP <- data.frame(commissioning=CEwp_ag$comdate,cap=cumsum(CEwp_ag$cap)/1000)
PEcap_WP <- data.frame(commissioning=PEwp_ag$comdate,cap=cumsum(PEwp_ag$cap)/1000)
PIcap_WP <- data.frame(commissioning=PIwp_ag$comdate,cap=cumsum(PIwp_ag$cap)/1000)
RNcap_WP <- data.frame(commissioning=RNwp_ag$comdate,cap=cumsum(RNwp_ag$cap)/1000)
RScap_WP <- data.frame(commissioning=RSwp_ag$comdate,cap=cumsum(RSwp_ag$cap)/1000)
SCcap_WP <- data.frame(commissioning=SCwp_ag$comdate,cap=cumsum(SCwp_ag$cap)/1000)
Macap_WP <- data.frame(commissioning=Mawp_ag$comdate,cap=cumsum(Mawp_ag$cap)/1000)
Prcap_WP <- data.frame(commissioning=Prwp_ag$comdate,cap=cumsum(Prwp_ag$cap)/1000)
Sacap_WP <- data.frame(commissioning=Sawp_ag$comdate,cap=cumsum(Sawp_ag$cap)/1000)
Arcap_WP <- data.frame(commissioning=Arwp_ag$comdate,cap=cumsum(Arwp_ag$cap)/1000)
Alcap_WP <- data.frame(commissioning=Alwp_ag$comdate,cap=cumsum(Alwp_ag$cap)/1000)
Elcap_WP <- data.frame(commissioning=Elwp_ag$comdate,cap=cumsum(Elwp_ag$cap)/1000)
Bocap_WP <- data.frame(commissioning=Bowp_ag$comdate,cap=cumsum(Bowp_ag$cap)/1000)

# split at 2006
bef06_B <- Bcap_WP[which(Bcap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_NE <- NEcap_WP[which(NEcap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_S <- Scap_WP[which(Scap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_BA <- BAcap_WP[which(BAcap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_CE <- CEcap_WP[which(CEcap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_PE <- PEcap_WP[which(PEcap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_PI <- PIcap_WP[which(PIcap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_RN <- RNcap_WP[which(RNcap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_RS <- RScap_WP[which(RScap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_SC <- SCcap_WP[which(SCcap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_Ma <- Macap_WP[which(Macap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_Pr <- Prcap_WP[which(Prcap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_Sa <- Sacap_WP[which(Sacap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_Ar <- Arcap_WP[which(Arcap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_Al <- Alcap_WP[which(Alcap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_El <- Elcap_WP[which(Elcap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]
bef06_Bo <- Bocap_WP[which(Bocap_WP$commissioning<as.POSIXct("2006-01-01",tz="UTC")),]

Bcap_WP <- Bcap_WP[which(Bcap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
NEcap_WP <- NEcap_WP[which(NEcap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
Scap_WP <- Scap_WP[which(Scap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
BAcap_WP <- BAcap_WP[which(BAcap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
CEcap_WP <- CEcap_WP[which(CEcap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
PEcap_WP <- PEcap_WP[which(PEcap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
PIcap_WP <- PIcap_WP[which(PIcap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
RNcap_WP <- RNcap_WP[which(RNcap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
RScap_WP <- RScap_WP[which(RScap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
SCcap_WP <- SCcap_WP[which(SCcap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
Macap_WP <- Macap_WP[which(Macap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
Prcap_WP <- Prcap_WP[which(Prcap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
Sacap_WP <- Sacap_WP[which(Sacap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
Arcap_WP <- Arcap_WP[which(Arcap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
Alcap_WP <- Alcap_WP[which(Alcap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
Elcap_WP <- Elcap_WP[which(Elcap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
Bocap_WP <- Bocap_WP[which(Bocap_WP$commissioning>=as.POSIXct("2006-01-01",tz="UTC")),]
# cut at 2017-08-31
Bcap_WP <- Bcap_WP[which(Bcap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]
NEcap_WP <- NEcap_WP[which(NEcap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]
Scap_WP <- Scap_WP[which(Scap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]
BAcap_WP <- BAcap_WP[which(BAcap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]
CEcap_WP <- CEcap_WP[which(CEcap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]
PEcap_WP <- PEcap_WP[which(PEcap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]
PIcap_WP <- PIcap_WP[which(PIcap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]
RNcap_WP <- RNcap_WP[which(RNcap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]
RScap_WP <- RScap_WP[which(RScap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]
SCcap_WP <- SCcap_WP[which(SCcap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]
Macap_WP <- Macap_WP[which(Macap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]
Prcap_WP <- Prcap_WP[which(Prcap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]
Sacap_WP <- Sacap_WP[which(Sacap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]
Arcap_WP <- Arcap_WP[which(Arcap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]
Alcap_WP <- Alcap_WP[which(Alcap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]
Elcap_WP <- Elcap_WP[which(Elcap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]
Bocap_WP <- Bocap_WP[which(Bocap_WP$commissioning<=as.POSIXct("2017-08-31",tz="UTC")),]

# add capacity on 1.1.2006
if(length(bef06_B$commissioning>0)){Bcap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_B$cap,1)),Bcap_WP)}
if(length(bef06_NE$commissioning>0)){NEcap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_NE$cap,1)),NEcap_WP)}
if(length(bef06_S$commissioning>0)){Scap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_S$cap,1)),Scap_WP)}
if(length(bef06_BA$commissioning>0)){BAcap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_BA$cap,1)),BAcap_WP)}
if(length(bef06_CE$commissioning>0)){CEcap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_CE$cap,1)),CEcap_WP)}
if(length(bef06_PE$commissioning>0)){PEcap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_PE$cap,1)),PEcap_WP)}
if(length(bef06_PI$commissioning>0)){PIcap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_PI$cap,1)),PIcap_WP)}
if(length(bef06_RN$commissioning>0)){RNcap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_RN$cap,1)),RNcap_WP)}
if(length(bef06_RS$commissioning>0)){RScap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_RS$cap,1)),RScap_WP)}
if(length(bef06_SC$commissioning>0)){SCcap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_SC$cap,1)),SCcap_WP)}
if(length(bef06_Ma$commissioning>0)){Macap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_Ma$cap,1)),Macap_WP)}
if(length(bef06_Pr$commissioning>0)){Prcap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_Pr$cap,1)),Prcap_WP)}
if(length(bef06_Sa$commissioning>0)){Sacap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_Sa$cap,1)),Sacap_WP)}
if(length(bef06_Ar$commissioning>0)){Arcap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_Ar$cap,1)),Arcap_WP)}
if(length(bef06_Al$commissioning>0)){Alcap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_Al$cap,1)),Alcap_WP)}
if(length(bef06_El$commissioning>0)){Elcap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_El$cap,1)),Elcap_WP)}
if(length(bef06_Bo$commissioning>0)){Bocap_WP <- rbind(data.frame(commissioning=as.POSIXct("2006-01-01",tz="UTC"),cap=tail(bef06_Bo$cap,1)),Bocap_WP)}

# put into sequence

Bcaps <- data.frame(date=seq(Bcap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
Bcaps$caps[match(Bcap_WP$commissioning,Bcaps$date)] <- Bcap_WP$cap
Bcaps$caps <- na.locf(Bcaps$caps)
NEcaps <- data.frame(date=seq(NEcap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
NEcaps$caps[match(NEcap_WP$commissioning,NEcaps$date)] <- NEcap_WP$cap
NEcaps$caps <- na.locf(NEcaps$caps)
Scaps <- data.frame(date=seq(Scap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
Scaps$caps[match(Scap_WP$commissioning,Scaps$date)] <- Scap_WP$cap
Scaps$caps <- na.locf(Scaps$caps)
BAcaps <- data.frame(date=seq(BAcap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
BAcaps$caps[match(BAcap_WP$commissioning,BAcaps$date)] <- BAcap_WP$cap
BAcaps$caps <- na.locf(BAcaps$caps)
CEcaps <- data.frame(date=seq(CEcap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
CEcaps$caps[match(CEcap_WP$commissioning,CEcaps$date)] <- CEcap_WP$cap
CEcaps$caps <- na.locf(CEcaps$caps)
PEcaps <- data.frame(date=seq(PEcap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
PEcaps$caps[match(PEcap_WP$commissioning,PEcaps$date)] <- PEcap_WP$cap
PEcaps$caps <- na.locf(PEcaps$caps)
PIcaps <- data.frame(date=seq(PIcap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
PIcaps$caps[match(PIcap_WP$commissioning,PIcaps$date)] <- PIcap_WP$cap
PIcaps$caps <- na.locf(PIcaps$caps)
RNcaps <- data.frame(date=seq(RNcap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
RNcaps$caps[match(RNcap_WP$commissioning,RNcaps$date)] <- RNcap_WP$cap
RNcaps$caps <- na.locf(RNcaps$caps)
RScaps <- data.frame(date=seq(RScap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
RScaps$caps[match(RScap_WP$commissioning,RScaps$date)] <- RScap_WP$cap
RScaps$caps <- na.locf(RScaps$caps)
SCcaps <- data.frame(date=seq(SCcap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
SCcaps$caps[match(SCcap_WP$commissioning,SCcaps$date)] <- SCcap_WP$cap
SCcaps$caps <- na.locf(SCcaps$caps)
Macaps <- data.frame(date=seq(Macap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
Macaps$caps[match(Macap_WP$commissioning,Macaps$date)] <- Macap_WP$cap
Macaps$caps <- na.locf(Macaps$caps)
Prcaps <- data.frame(date=seq(Prcap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
Prcaps$caps[match(Prcap_WP$commissioning,Prcaps$date)] <- Prcap_WP$cap
Prcaps$caps <- na.locf(Prcaps$caps)
Sacaps <- data.frame(date=seq(Sacap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
Sacaps$caps[match(Sacap_WP$commissioning,Sacaps$date)] <- Sacap_WP$cap
Sacaps$caps <- na.locf(Sacaps$caps)
Arcaps <- data.frame(date=seq(Arcap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
Arcaps$caps[match(Arcap_WP$commissioning,Arcaps$date)] <- Arcap_WP$cap
Arcaps$caps <- na.locf(Arcaps$caps)
Alcaps <- data.frame(date=seq(Alcap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
Alcaps$caps[match(Alcap_WP$commissioning,Alcaps$date)] <- Alcap_WP$cap
Alcaps$caps <- na.locf(Alcaps$caps)
Elcaps <- data.frame(date=seq(Elcap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
Elcaps$caps[match(Elcap_WP$commissioning,Elcaps$date)] <- Elcap_WP$cap
Elcaps$caps <- na.locf(Elcaps$caps)
Bocaps <- data.frame(date=seq(Bocap_WP$commissioning[1],as.POSIXct("2017-08-31",tz="UTC"),by="d"),caps=NA)
Bocaps$caps[match(Bocap_WP$commissioning,Bocaps$date)] <- Bocap_WP$cap
Bocaps$caps <- na.locf(Bocaps$caps)

tab <- data.frame(region=c("Brazil","Northeast","South","Bahia","Ceará","Pernambuco","Piaui","Rio Grande do Norte","Rio Grande do Sul","Santa Catarina","Macaubas","Praia Formosa","Sao Clemente","Araripe","Alegria II","Elebras Cidreira 1","Bom Jardim"),startsim=NA,startval=NA,startmax=NA,mean_cap_MW=NA)
# get start date of simulation
tab$startsim[1] <- Bcaps$date[1]
tab$startsim[2] <- NEcaps$date[1]
tab$startsim[3] <- Scaps$date[1]
tab$startsim[4] <- BAcaps$date[1]
tab$startsim[5] <- CEcaps$date[1]
tab$startsim[6] <- PEcaps$date[1]
tab$startsim[7] <- PIcaps$date[1]
tab$startsim[8] <- RNcaps$date[1]
tab$startsim[9] <- RScaps$date[1]
tab$startsim[10] <- SCcaps$date[1]
tab$startsim[11] <- Macaps$date[1]
tab$startsim[12] <- Prcaps$date[1]
tab$startsim[13] <- Sacaps$date[1]
tab$startsim[14] <- Arcaps$date[1]
tab$startsim[15] <- Alcaps$date[1]
tab$startsim[16] <- Elcaps$date[1]
tab$startsim[17] <- Bocaps$date[1]

# load validation data to find out starting date
Bval <- read.csv2(paste0(dirwindprodsubbra,"/brasil_dia.csv"))
NEval <- read.csv2(paste0(dirwindprodsubbra,"/nordeste_dia.csv"))
Sval <- read.csv2(paste0(dirwindprodsubbra,"/sul_dia.csv"))
BAval <- read.csv2(paste0(dirwindproddaily,"/Bahia.csv"))
CEval <- read.csv2(paste0(dirwindproddaily,"/Ceará.csv"))
PEval <- read.csv2(paste0(dirwindproddaily,"/Pernambuco.csv"))
PIval <- read.csv2(paste0(dirwindproddaily,"/Piaui.csv"))
RNval <- read.csv2(paste0(dirwindproddaily,"/RioGrandedoNorte.csv"))
RSval <- read.csv2(paste0(dirwindproddaily,"/RioGrandedoSul.csv"))
SCval <- read.csv2(paste0(dirwindproddaily,"/SantaCatarina.csv"))
Maval <- read.csv2(paste0(dirwindparks_sel,"/BA_Macaubas.csv"))
Prval <- read.csv2(paste0(dirwindparks_sel,"/CE_PraiaFormosa.csv"))
Saval <- read.csv2(paste0(dirwindparks_sel,"/PE_SaoClemente.csv"))
Arval <- read.csv2(paste0(dirwindparks_sel,"/PI_Araripe.csv"))
Alval <- read.csv2(paste0(dirwindparks_sel,"/RN_AlegriaII.csv"))
Elval <- read.csv2(paste0(dirwindparks_sel,"/RS_ElebrasCidreira1.csv"))
Boval <- read.csv2(paste0(dirwindparks_sel,"/SC_BomJardim.csv"))

# get start dates of validation data
tab$startval[1] <- as.POSIXct(paste(substr(Bval[2,1],7,10),substr(Bval[2,1],4,5),substr(Bval[2,1],1,2),sep="-"),tz="UTC")
tab$startval[2] <- as.POSIXct(paste(substr(NEval[2,1],7,10),substr(NEval[2,1],4,5),substr(NEval[2,1],1,2),sep="-"),tz="UTC")
tab$startval[3] <- as.POSIXct(paste(substr(Sval[2,1],7,10),substr(Sval[2,1],4,5),substr(Sval[2,1],1,2),sep="-"),tz="UTC")
tab$startval[4] <- as.POSIXct(paste(substr(BAval[2,1],7,10),substr(BAval[2,1],4,5),substr(BAval[2,1],1,2),sep="-"),tz="UTC")
tab$startval[5] <- as.POSIXct(paste(substr(CEval[2,1],7,10),substr(CEval[2,1],4,5),substr(CEval[2,1],1,2),sep="-"),tz="UTC")
tab$startval[6] <- as.POSIXct(paste(substr(PEval[2,1],7,10),substr(PEval[2,1],4,5),substr(PEval[2,1],1,2),sep="-"),tz="UTC")
tab$startval[7] <- as.POSIXct(paste(substr(PIval[2,1],7,10),substr(PIval[2,1],4,5),substr(PIval[2,1],1,2),sep="-"),tz="UTC")
tab$startval[8] <- as.POSIXct(paste(substr(RNval[2,1],7,10),substr(RNval[2,1],4,5),substr(RNval[2,1],1,2),sep="-"),tz="UTC")
tab$startval[9] <- as.POSIXct(paste(substr(RSval[2,1],7,10),substr(RSval[2,1],4,5),substr(RSval[2,1],1,2),sep="-"),tz="UTC")
tab$startval[10] <- as.POSIXct(paste(substr(SCval[2,1],7,10),substr(SCval[2,1],4,5),substr(SCval[2,1],1,2),sep="-"),tz="UTC")
tab$startval[11] <- as.POSIXct(paste(substr(Maval[2,1],7,10),substr(Maval[2,1],4,5),substr(Maval[2,1],1,2),sep="-"),tz="UTC")
tab$startval[12] <- as.POSIXct(paste(substr(Prval[2,1],7,10),substr(Prval[2,1],4,5),substr(Prval[2,1],1,2),sep="-"),tz="UTC")
tab$startval[13] <- as.POSIXct(paste(substr(Saval[2,1],7,10),substr(Saval[2,1],4,5),substr(Saval[2,1],1,2),sep="-"),tz="UTC")
tab$startval[14] <- as.POSIXct(paste(substr(Arval[2,1],7,10),substr(Arval[2,1],4,5),substr(Arval[2,1],1,2),sep="-"),tz="UTC")
tab$startval[15] <- as.POSIXct(paste(substr(Alval[2,1],7,10),substr(Alval[2,1],4,5),substr(Alval[2,1],1,2),sep="-"),tz="UTC")
tab$startval[16] <- as.POSIXct(paste(substr(Elval[2,1],7,10),substr(Elval[2,1],4,5),substr(Elval[2,1],1,2),sep="-"),tz="UTC")
tab$startval[17] <- as.POSIXct(paste(substr(Boval[2,1],7,10),substr(Boval[2,1],4,5),substr(Boval[2,1],1,2),sep="-"),tz="UTC")

# find maximum of starting dates per location
for(i in c(1:17)){
  tab$startmax[i] <- max(tab$startsim[i],tab$startval[i])
}


# get mean capacities for overlapping timespans
tab$mean_cap_MW[1] <- round(mean(Bcaps$caps[which(Bcaps$date>=tab$startmax[1])]),2)
tab$mean_cap_MW[2] <- round(mean(NEcaps$caps[which(Bcaps$date>=tab$startmax[2])]),2)
tab$mean_cap_MW[3] <- round(mean(Scaps$caps[which(Scaps$date>=tab$startmax[3])]),2)
tab$mean_cap_MW[4] <- round(mean(BAcaps$caps[which(BAcaps$date>=tab$startmax[4])]),2)
tab$mean_cap_MW[5] <- round(mean(CEcaps$caps[which(CEcaps$date>=tab$startmax[5])]),2)
tab$mean_cap_MW[6] <- round(mean(PEcaps$caps[which(PEcaps$date>=tab$startmax[6])]),2)
tab$mean_cap_MW[7] <- round(mean(PIcaps$caps[which(PIcaps$date>=tab$startmax[7])]),2)
tab$mean_cap_MW[8] <- round(mean(RNcaps$caps[which(RNcaps$date>=tab$startmax[8])]),2)
tab$mean_cap_MW[9] <- round(mean(RScaps$caps[which(RScaps$date>=tab$startmax[9])]),2)
tab$mean_cap_MW[10] <- round(mean(SCcaps$caps[which(SCcaps$date>=tab$startmax[10])]),2)
tab$mean_cap_MW[11] <- round(mean(Macaps$caps[which(Macaps$date>=tab$startmax[11])]),2)
tab$mean_cap_MW[12] <- round(mean(Prcaps$caps[which(Prcaps$date>=tab$startmax[12])]),2)
tab$mean_cap_MW[13] <- round(mean(Sacaps$caps[which(Sacaps$date>=tab$startmax[13])]),2)
tab$mean_cap_MW[14] <- round(mean(Arcaps$caps[which(Arcaps$date>=tab$startmax[14])]),2)
tab$mean_cap_MW[15] <- round(mean(Alcaps$caps[which(Alcaps$date>=tab$startmax[15])]),2)
tab$mean_cap_MW[16] <- round(mean(Elcaps$caps[which(Elcaps$date>=tab$startmax[16])]),2)
tab$mean_cap_MW[17] <- round(mean(Bocaps$caps[which(Bocaps$date>=tab$startmax[17])]),2)

save(tab,file=paste0(dirbase,"/table_mean_caps_validation.RData"))









####################################################################################################################################################
# PLOTS STATISTICAL ANALYSIS #######################################################################################################################
####################################################################################################################################################

# width and height of images
w = 3.375
h = 5
# rows in legend
rw = 4


# for normalising RMSEs and MBEs we need mean capacities per area (B,SUBs,STATEs,stats)
load(paste0(dirbase,"/table_mean_caps_validation.RData"))
tab$region <- c("Brazil","North-East","South","Bahia","Ceará","Pernambuco","Piaui","RioGrandedoNorte","RioGrandedoSul","SantaCatarina","BA-Macaubas","CE-PraiaFormosa","PE-SaoClemente","PI-Araripe","RN-AlegriaII","RS-ElebrasCidreira1","SC-BomJardim")


################
# STEP 1 #######
################

# correlations and RMSE

### correlations

# Brazil + subsystems
load(paste0(dirresultscapc,"/Bstats_noc.RData"))
Bstats <- melt(Bstats)
names(Bstats) <- c("sim","measure","val")
Bstats$area <- "Brazil"
load(paste0(dirresultscapc,"/SUBstats_noc.RData"))
SUBstats <- melt(SUBstats)
names(SUBstats) <- c("sim","measure","val","area")
BSUBstats <- rbind(Bstats,SUBstats)
# replace NE with North-East and S with South
BSUBstats$area[which(BSUBstats$area=="NE")] <- "North-East"
BSUBstats$area[which(BSUBstats$area=="S")] <- "South"
ggplot(data=BSUBstats %>% filter(measure=="cor",sim!="obs"), aes(x=sim,y=val,group=sim,color=area)) +
  coord_cartesian(ylim=c(0.45,1)) +
  geom_jitter(lwd=3, width=0.2,height=0) +
  scale_colour_manual(values=c("#c72321", "#0d8085", "#efc220")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Interpolation method") + 
  ylab("Daily correlations")
ggsave(paste0(dirresultscapc,"/pointplots/BSUBcor_noc.png"), width = w, height = h)


# states
load(paste0(dirresultscapc,"/STATEstats_noc.RData"))
STATEstats <- melt(STATEstats)
names(STATEstats) <- c("sim","measure","val","state")
ggplot(data=STATEstats %>% filter(measure=="cor",sim!="obs"), aes(x=sim,y=val,group=sim,color=state)) +
  coord_cartesian(ylim=c(0.45,1)) +
  geom_boxplot() +
  geom_jitter(lwd=3, width=0.2,height=0) +
  scale_colour_manual(values=c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Interpolation method") + 
  ylab("")
ggsave(paste0(dirresultscapc,"/pointplots/STATEcor_noc.png"), width = w, height = h)




# windparks
load(paste0(dirresultscapc,"/statstats_noc.RData"))
statstats <- melt(statstats)
names(statstats) <- c("sim","measure","val","windpark")
ggplot(data=statstats %>% filter(measure=="cor",sim!="obs"), aes(x=sim,y=val,group=sim,color=windpark)) +
  coord_cartesian(ylim=c(0.45,1)) +
  geom_boxplot() +
  geom_jitter(lwd=3, width=0.2,height=0) +
  scale_colour_manual(values=c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Interpolation method") + 
  ylab("")
ggsave(paste0(dirresultscapc,"/pointplots/statcor_noc.png"), width = w, height = h)



### RMSES

# Brazil and subsystems
# divide capacity by 1000 to get from MW to GW and then multiply by 24 for hours of the day
BSUBstats_r <- BSUBstats
BSUBstats_r$val[which(BSUBstats$measure!="cor")] <- BSUBstats_r$val[which(BSUBstats$measure!="cor")]/(tab$mean_cap_MW[match(BSUBstats$area[which(BSUBstats$measure!="cor")],tab$region)]/1000*24)
ggplot(data=BSUBstats_r %>% filter(measure=="rmse",sim!="obs"), aes(x=sim,y=val,group=sim,color=area)) +
  coord_cartesian(ylim=c(0.1,0.35)) +
  geom_jitter(lwd=3, width=0.2,height=0) +
  scale_colour_manual(values=c("#c72321", "#0d8085", "#efc220")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Interpolation method") + 
  ylab("Daily relative RMSE")
ggsave(paste0(dirresultscapc,"/pointplots/BSUBrrmse_noc.png"), width = w, height = h)



# states
STATEstats_r <- STATEstats
STATEstats_r$val[which(STATEstats$measure!="cor")] <- STATEstats_r$val[which(STATEstats$measure!="cor")]/(tab$mean_cap_MW[match(STATEstats$state[which(STATEstats$measure!="cor")],tab$region)]/1000*24)
ggplot(data=STATEstats_r %>% filter(measure=="rmse",sim!="obs"), aes(x=sim,y=val,color=state,group=sim)) +
  coord_cartesian(ylim=c(0.1,0.35)) +
  geom_boxplot() +
  geom_jitter(lwd=3, width=0.2,height=0) +
  scale_colour_manual(values=c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Interpolation method") + 
  ylab("")
ggsave(paste0(dirresultscapc,"/pointplots/STATErrmse_noc.png"), width = w, height = h)

# windparks
statstats_r <- statstats
statstats_r$val[which(statstats$measure!="cor")] <- statstats_r$val[which(statstats$measure!="cor")]/(tab$mean_cap_MW[match(statstats$windpark[which(statstats$measure!="cor")],tab$region)]/1000*24)
ggplot(data=statstats_r %>% filter(measure=="rmse",sim!="obs"), aes(x=sim,y=val,group=sim,color=windpark)) +
  coord_cartesian(ylim=c(0.1,0.35)) +
  geom_boxplot() +
  geom_jitter(lwd=3, width=0.2,height=0) +
  scale_colour_manual(values=c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Interpolation method") + 
  ylab("")
ggsave(paste0(dirresultscapc,"/pointplots/statrrmse_noc.png"), width = w, height = h)





################
# STEP 2 #######
################

# RMSEs and MBEs

### RMSEs

# Brazil and subsystems
load(paste0(dirresultscapc,"/Bstats_wsma.RData"))
Bstats <- melt(Bstats)
names(Bstats) <- c("sim","measure","val")
Bstats$area <- "Brazil"
load(paste0(dirresultscapc,"/SUBstats_wsma.RData"))
SUBstats <- melt(SUBstats)
names(SUBstats) <- c("sim","measure","val","area")
BSUBstats <- rbind(Bstats,SUBstats)
# replace NE with North-East and S with South
BSUBstats$area[which(BSUBstats$area=="NE")] <- "North-East"
BSUBstats$area[which(BSUBstats$area=="S")] <- "South"
BSUBstats_r <- BSUBstats
# divide capacity by 1000 to get from MW to GW and then multiply by 24 for hours of the day
BSUBstats_r$val[which(BSUBstats$measure!="cor")] <- BSUBstats_r$val[which(BSUBstats$measure!="cor")]/(tab$mean_cap_MW[match(BSUBstats$area[which(BSUBstats$measure!="cor")],tab$region)]/1000*24)
ggplot(data=BSUBstats_r %>% filter(measure=="rmse",sim!="obs"), aes(x=sim,y=val,group=sim,color=area)) +
  coord_cartesian(ylim=c(0.1,0.5)) +
  geom_jitter(lwd=3, width=0.2,height=0) +
  scale_colour_manual(values=c("#c72321", "#0d8085", "#efc220")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Wind speed mean approximation source") + 
  ylab("Daily relative RMSE")
ggsave(paste0(dirresultscapc,"/pointplots/BSUBrrmse_wsma.png"), width = w, height = h)


# states
load(paste0(dirresultscapc,"/STATEstats_wsma.RData"))
STATEstats <- melt(STATEstats)
names(STATEstats) <- c("sim","measure","val","state")
STATEstats_r <- STATEstats
STATEstats_r$val[which(STATEstats$measure!="cor")] <- STATEstats_r$val[which(STATEstats$measure!="cor")]/(tab$mean_cap_MW[match(STATEstats$state[which(STATEstats$measure!="cor")],tab$region)]/1000*24)
ggplot(data=STATEstats_r %>% filter(measure=="rmse",sim!="obs"), aes(x=sim,y=val,color=state,group=sim)) +
  coord_cartesian(ylim=c(0.1,0.5)) +
  geom_boxplot() +
  geom_jitter(lwd=3, width=0.2,height=0) +
  scale_colour_manual(values=c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Wind speed mean approximation source") + 
  ylab("")
ggsave(paste0(dirresultscapc,"/pointplots/STATErrmse_wsma.png"), width = w, height = h)


# windparks
load(paste0(dirresultscapc,"/statstats_wsma.RData"))
for(i in c(1:7)){statstats[[i]][,1] <- as.factor(c("NN","INMET","GWA","obs"))}
statstats <- melt(statstats)
names(statstats) <- c("sim","measure","val","windpark")
statstats_r <- statstats
statstats_r$val[which(statstats$measure!="cor")] <- statstats_r$val[which(statstats$measure!="cor")]/(tab$mean_cap_MW[match(statstats$windpark[which(statstats$measure!="cor")],tab$region)]/1000*24)

statstats_r1 <- filter(statstats_r,measure=="rmse",windpark%in%(c("BA-Macaubas","CE-PraiaFormosa")),sim=="INMET")
statstats_r2 <- filter(statstats_r,measure=="rmse",!((windpark%in%(c("BA-Macaubas","CE-PraiaFormosa")))&(sim=="INMET")),sim!="obs")
ggplot(data=statstats_r %>% filter(measure=="rmse",sim!="obs"), aes(x=sim,y=val,group=sim,color=windpark)) +
  coord_cartesian(ylim=c(0.1,0.5)) +
  geom_boxplot() +
  geom_jitter(data = statstats_r2, lwd=3, width=0.2,height=0,shape=19) +
  geom_jitter(data = statstats_r1, lwd=3.5, width=0.2,height=0,shape=15) +
  scale_colour_manual(values=c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Wind speed mean approximation source") + 
  ylab("") 
ggsave(paste0(dirresultscapc,"/pointplots/statrrmse_wsma.png"), width = w, height = h)


### MBEs

# Brazil and subsystems
ggplot(data=BSUBstats_r %>% filter(measure=="mbe",sim!="obs"), aes(x=sim,y=val,group=sim,color=area)) +
  coord_cartesian(ylim=c(-0.5,0.45)) +
  geom_jitter(lwd=3, width=0.2,height=0) +
  scale_colour_manual(values=c("#c72321", "#0d8085", "#efc220")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Wind speed mean approximation source") + 
  ylab("Daily relative MBE")
ggsave(paste0(dirresultscapc,"/pointplots/BSUBrmbe_wsma.png"), width = w, height = h)


# states
ggplot(data=STATEstats_r %>% filter(measure=="mbe",sim!="obs"), aes(x=sim,y=val,color=state,group=sim)) +
  coord_cartesian(ylim=c(-0.5,0.45)) +
  geom_boxplot() +
  geom_jitter(lwd=3, width=0.2,height=0) +
  scale_colour_manual(values=c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Wind speed mean approximation source") + 
  ylab("")
ggsave(paste0(dirresultscapc,"/pointplots/STATErmbe_wsma.png"), width = w, height = h)


# windparks
statstats_r1 <- filter(statstats_r,measure=="mbe",windpark%in%(c("BA-Macaubas","CE-PraiaFormosa")),sim=="INMET")
statstats_r2 <- filter(statstats_r,measure=="mbe",!((windpark%in%(c("BA-Macaubas","CE-PraiaFormosa")))&(sim=="INMET")),sim!="obs")
ggplot(data=statstats_r %>% filter(measure=="mbe",sim!="obs"), aes(x=sim,y=val,group=sim,color=windpark)) +
  coord_cartesian(ylim=c(-0.5,0.45)) +
  geom_boxplot() +
  geom_jitter(data = statstats_r2, lwd=3, width=0.2,height=0,shape=19) +
  geom_jitter(data = statstats_r1, lwd=3.5, width=0.2,height=0,shape=15) +
  scale_colour_manual(values=c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Wind speed mean approximation source") + 
  ylab("") 
ggsave(paste0(dirresultscapc,"/pointplots/statrmbe_wsma.png"), width = w, height = h)



################
# STEP 3 #######
################

# RMSEs and MBEs

### RMSES

# Brazil and subsystems
load(paste0(dirresultscapc,"/Bstats_wsc.RData"))
Bstats <- melt(Bstats)
names(Bstats) <- c("sim","measure","val")
Bstats$area <- "Brazil"
load(paste0(dirresultscapc,"/SUBstats_wsc.RData"))
SUBstats <- melt(SUBstats)
names(SUBstats) <- c("sim","measure","val","area")
BSUBstats <- rbind(Bstats,SUBstats)
# replace NE with North-East and S with South
BSUBstats$area[which(BSUBstats$area=="NE")] <- "North-East"
BSUBstats$area[which(BSUBstats$area=="S")] <- "South"
BSUBstats_r <- BSUBstats
BSUBstats_r$val[which(BSUBstats$measure!="cor")] <- BSUBstats_r$val[which(BSUBstats$measure!="cor")]/(tab$mean_cap_MW[match(BSUBstats$area[which(BSUBstats$measure!="cor")],tab$region)]/1000*24)
BSUBstats_r1 <- filter(BSUBstats_r,measure=="rmse",area=="South",sim=="wsc_m")
BSUBstats_r2 <- filter(BSUBstats_r,measure=="rmse",!((area=="South")&(sim=="wsc_m")),sim!="obs")
ggplot(data=BSUBstats_r %>% filter(measure=="rmse",sim!="obs"), aes(x=sim,y=val,group=sim,color=area)) +
  coord_cartesian(ylim=c(0.1,0.5)) +
  geom_jitter(data = BSUBstats_r1, lwd=3, width=0.2,height=0,shape=15) +
  geom_jitter(data = BSUBstats_r2, lwd=3.5, width=0.2,height=0,shape=19) +
  scale_colour_manual(values=c("#c72321", "#0d8085", "#efc220")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Wind speed correction method") + 
  ylab("Daily relative RMSE")
ggsave(paste0(dirresultscapc,"/pointplots/BSUBrrmse_wsc.png"), width = w, height = h)


# states
load(paste0(dirresultscapc,"/STATEstats_wsc.RData"))
STATEstats <- melt(STATEstats)
names(STATEstats) <- c("sim","measure","val","state")
STATEstats_r <- STATEstats
STATEstats_r$val[which(STATEstats$measure!="cor")] <- STATEstats_r$val[which(STATEstats$measure!="cor")]/(tab$mean_cap_MW[match(STATEstats$state[which(STATEstats$measure!="cor")],tab$region)]/1000*24)
STATEstats_r1 <- filter(STATEstats_r,measure=="rmse",((state!="Bahia")&(sim=="wsc_m"))|((state%in%c("Piaui","SantaCatarina"))&(sim=="wsc_hm")))
STATEstats_r2 <- filter(STATEstats_r,measure=="rmse",!((state!="Bahia")&(sim=="wsc_m"))&!((state%in%c("Piaui","SantaCatarina"))&(sim=="wsc_hm")),sim!="obs")
ggplot(data=STATEstats_r %>% filter(measure=="rmse",sim!="obs"), aes(x=sim,y=val,color=state,group=sim)) +
  coord_cartesian(ylim=c(0.1,0.5)) +
  geom_boxplot() +
  geom_jitter(data = STATEstats_r1, lwd=3, width=0.2,height=0,shape=15) +
  geom_jitter(data = STATEstats_r2, lwd=3.5, width=0.2,height=0,shape=19) +
  scale_colour_manual(values=c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Wind speed correction method") + 
  ylab("")
ggsave(paste0(dirresultscapc,"/pointplots/STATErrmse_wsc.png"), width = w, height = h)



# windparks
load(paste0(dirresultscapc,"/statstats_wsc_sel_allc.RData"))
statstats <- melt(statstats)
names(statstats) <- c("sim","measure","val","windpark")
statstats_r <- statstats
statstats_r$val[which(statstats$measure!="cor")] <- statstats_r$val[which(statstats$measure!="cor")]/(tab$mean_cap_MW[match(statstats$windpark[which(statstats$measure!="cor")],tab$region)]/1000*24)
statstats_r1 <- filter(statstats_r,measure=="rmse",(windpark%in%(c("RS-ElebrasCidreira1","RN-AlegriaII"))&sim=="wsc_hm")|(sim=="wsma"))
statstats_r2 <- filter(statstats_r,measure=="rmse",!((windpark%in%(c("RS-ElebrasCidreira1","RN-AlegriaII")))&(sim=="wsc_hm"))&(sim!="wsma"),sim!="obs")
ggplot(data=statstats_r %>% filter(measure=="rmse",sim!="obs"), aes(x=sim,y=val,group=sim,color=windpark)) +
  coord_cartesian(ylim=c(0.1,0.5)) +
  geom_boxplot() +
  geom_jitter(data = statstats_r2, lwd=3, width=0.2,height=0,shape=15) +
  geom_jitter(data = statstats_r1, lwd=3.5, width=0.2,height=0,shape=19) +
  scale_colour_manual(values=c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Wind speed correction method") + 
  ylab("") 
ggsave(paste0(dirresultscapc,"/pointplots/statrrmse_wsc.png"), width = w, height = h)




### MBEs

# Brazil and subsystems
BSUBstats_r1 <- filter(BSUBstats_r,measure=="mbe",area=="South",sim=="wsc_m")
BSUBstats_r2 <- filter(BSUBstats_r,measure=="mbe",!((area=="South")&(sim=="wsc_m")),sim!="obs")
ggplot(data=BSUBstats_r %>% filter(measure=="mbe",sim!="obs"), aes(x=sim,y=val,group=sim,color=area)) +
  coord_cartesian(ylim=c(-0.4,0.6)) +
  geom_jitter(data = BSUBstats_r1, lwd=3, width=0.2,height=0,shape=15) +
  geom_jitter(data = BSUBstats_r2, lwd=3.5, width=0.2,height=0,shape=19) +
  scale_colour_manual(values=c("#c72321", "#0d8085", "#efc220")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Wind speed correction method") + 
  ylab("Daily relative MBE")
ggsave(paste0(dirresultscapc,"/pointplots/BSUBrmbe_wsc.png"), width = w, height = h)


# states
STATEstats_r1 <- filter(STATEstats_r,measure=="mbe",((state!="Bahia")&(sim=="wsc_m"))|((state%in%c("Piaui","SantaCatarina"))&(sim=="wsc_hm")))
STATEstats_r2 <- filter(STATEstats_r,measure=="mbe",!((state!="Bahia")&(sim=="wsc_m"))&!((state%in%c("Piaui","SantaCatarina"))&(sim=="wsc_hm")),sim!="obs")
ggplot(data=STATEstats_r %>% filter(measure=="mbe",sim!="obs"), aes(x=sim,y=val,color=state,group=sim)) +
  coord_cartesian(ylim=c(-0.4,0.6)) +
  geom_boxplot() +
  geom_jitter(data = STATEstats_r1, lwd=3, width=0.2,height=0,shape=15) +
  geom_jitter(data = STATEstats_r2, lwd=3.5, width=0.2,height=0,shape=19) +
  scale_colour_manual(values=c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Wind speed correction method") + 
  ylab("")
ggsave(paste0(dirresultscapc,"/pointplots/STATErmbe_wsc.png"), width = w, height = h)



# windparks
statstats_r1 <- filter(statstats_r,measure=="mbe",(windpark%in%(c("RS-ElebrasCidreira1","RN-AlegriaII"))&sim=="wsc_hm")|(sim=="wsma"))
statstats_r2 <- filter(statstats_r,measure=="mbe",!((windpark%in%(c("RS-ElebrasCidreira1","RN-AlegriaII")))&(sim=="wsc_hm"))&(sim!="wsma"),sim!="obs")
ggplot(data=statstats_r %>% filter(measure=="mbe",sim!="obs"), aes(x=sim,y=val,group=sim,color=windpark)) +
  coord_cartesian(ylim=c(-0.4,0.6)) +
  geom_boxplot() +
  geom_jitter(data = statstats_r2, lwd=3, width=0.2,height=0,shape=15) +
  geom_jitter(data = statstats_r1, lwd=3.5, width=0.2,height=0,shape=19) +
  scale_colour_manual(values=c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c")) +
  theme(legend.position = "bottom") +
  guides(color=guide_legend(nrow=rw,title="")) +
  xlab("Wind speed correction method") + 
  ylab("") 
ggsave(paste0(dirresultscapc,"/pointplots/statrmbe_wsc.png"), width = w, height = h)



























