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







library(dplyr)
library(lubridate)
library(tibble)
library(feather)
library(tidyverse)
library(Metrics)
library(reshape2)
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
library(cowplot)


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

# installed capacities per subsystem needed for capacity correction can be downloaded here:
# http://ons.org.br/Paginas/resultados-da-operacao/historico-da-operacao/capacidade_instalada.aspx
# select each subsystem and Tipo de Usina "Eólica"
# and then data since 2006
# then click on graph and then download as csv

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
                date_seq,c("U2M"),
                name,
                password,
                TRUE)
getMERRADataBox(lon1,lat1,lon2,lat2,
                date_seq,c("V2M"),
                name,
                password,
                TRUE)
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
lapply(date_seq,convertMerraFeather,lon1,lat1,lon2,lat2,"U2M","U2m")
lapply(date_seq,convertMerraFeather,lon1,lat1,lon2,lat2,"V2M","V2m")
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
lonlat <- as.data.frame(LonLat)

pnames <- c("U2M","V2M","U10M","U50M","V10M","V50M","DISPH")

# change format from daily to point-wise files
# split into chunks due to memory restrictions
divs <- divisors(length(lonlat[,1])) 
binsize <- max(divs[which(divs<400)])
lll <- split(lonlat,f=rep(1:(length(lonlat[,1])/binsize),each=binsize))
for(ll in lll){
  invisible(apply(ll,1,saveMerraPointU2M,pnames2,lon1,lat1,lon2,lat2,date_seq))
}







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




###################################################################################################################
############################### STEP 1: FIND BEST INTERPOLATION METHOD ############################################
###################################################################################################################
date.start <- as.POSIXct("2006-01-01",tz="UTC")
rad <- pi/180
LonLat <- read_feather(paste(dirmerra,"/lonlat.feather",sep=""))

# basic simulation returns wind power per wind park
# Nearest Neighbour
intmethod=1
statpowlist <- calcstatpower(intmethod) # 33 sec
setwd(dirresults)
save(statpowlist,file="statpowlist_NN.RData")
statpowlistNN <- statpowlist
rm(statpowlist)
# Bilinear Interpolation
intmethod=2
statpowlist <- calcstatpower(intmethod)
setwd(dirresults)
save(statpowlist,file="statpowlist_BLI.RData")
statpowlistBLI <- statpowlist
rm(statpowlist)
# Inverse Distance Weighting
intmethod=4
statpowlist <- calcstatpower(intmethod)
setwd(dirresults)
save(statpowlist,file="statpowlist_IDW.RData")
statpowlistIDW <- statpowlist
rm(statpowlist)



##### prepare results for Brazil, subsystems, states and wind parks #####

# sum up: Brazil
BRA_NN <- sum_brasil(statpowlistNN)
BRA_BLI <- sum_brasil(statpowlistBLI)
BRA_IDW <- sum_brasil(statpowlistIDW)
# states:
STA_NN <- makeSTATEpowlist(statpowlistNN)
STA_BLI <- makeSTATEpowlist(statpowlistBLI)
STA_IDW <- makeSTATEpowlist(statpowlistIDW)
# subsystems:
SUB_NN <- sum_subsystem(STA_NN)
SUB_BLI <- sum_subsystem(STA_BLI)
SUB_IDW <- sum_subsystem(STA_IDW)
# windparks:
WPS_NN <- makeWPpowlist(statpowlistNN)
WPS_BLI <- makeWPpowlist(statpowlistBLI)
WPS_IDW <- makeWPpowlist(statpowlistIDW)

# aggregate daily
# Brazil:
BRA_NNd <- dailyaggregate(list(BRA_NN))[[1]]
BRA_BLId <- dailyaggregate(list(BRA_BLI))[[1]]
BRA_IDWd <- dailyaggregate(list(BRA_IDW))[[1]]
# states:
STA_NNd <- dailyaggregate(STA_NN)
names(STA_NNd) <- gsub(" ","",names(STA_NN))
STA_BLId <- dailyaggregate(STA_BLI)
names(STA_BLId) <- gsub(" ","",names(STA_BLI))
STA_IDWd <- dailyaggregate(STA_IDW)
names(STA_IDWd) <- gsub(" ","",names(STA_IDW))
# subsystems:
SUB_NNd <- dailyaggregate(SUB_NN)
names(SUB_NNd) <- names(SUB_NN)
SUB_BLId <- dailyaggregate(SUB_BLI)
names(SUB_BLId) <- names(SUB_BLI)
SUB_IDWd <- dailyaggregate(SUB_IDW)
names(SUB_IDWd) <- names(SUB_IDW)
# windparks:
WPS_NNd <- dailyaggregate(WPS_NN)
names(WPS_NNd) <- names(WPS_NN)
WPS_BLId <- dailyaggregate(WPS_BLI)
names(WPS_BLId) <- names(WPS_BLI)
WPS_IDWd <- dailyaggregate(WPS_IDW)
names(WPS_IDWd) <- names(WPS_IDW)


# get production data
prodBRA <- getprodSUBBRA("BRASIL")
prodNE <- getprodSUBBRA("NE")
prodS <- getprodSUBBRA("S")
prodSTA <- list()
for(state in gsub(" ","",names(STA_NN))){
  prodSTA[[state]] <- getSTATEproddaily(state)
}
prodWPS <- list()
for(wps in names(WPS_NN)){
  prodWPS[[wps]] <- getstatproddaily(substr(wps,1,2))
}

# cut to same lengths
# Brazil:
BRA_NNdc <- csl(prodBRA,BRA_NNd)
BRA_BLIdc <- csl(prodBRA,BRA_BLId)
BRA_IDWdc <- csl(prodBRA,BRA_IDWd)
# states:
STA_NNdc <- list()
STA_BLIdc <- list()
STA_IDWdc <- list()
for(state in names(STA_NNd)){
  if(!is.null(prodSTA[[state]])){
    STA_NNdc[[state]] <- csl(prodSTA[[state]],STA_NNd[[state]])
    STA_BLIdc[[state]] <- csl(prodSTA[[state]],STA_BLId[[state]])
    STA_IDWdc[[state]] <- csl(prodSTA[[state]],STA_IDWd[[state]])
  }
}
# subsystems:
SUB_NNdc <- list()
SUB_NNdc[["NE"]] <- csl(prodNE,SUB_NNd$NE)
SUB_NNdc[["S"]] <- csl(prodS,SUB_NNd$S)
SUB_BLIdc <- list()
SUB_BLIdc[["NE"]] <- csl(prodNE,SUB_BLId$NE)
SUB_BLIdc[["S"]] <- csl(prodS,SUB_BLId$S)
SUB_IDWdc <- list()
SUB_IDWdc[["NE"]] <- csl(prodNE,SUB_IDWd$NE)
SUB_IDWdc[["S"]] <- csl(prodS,SUB_IDWd$S)
# windparks:
WPS_NNdc <- list()
WPS_BLIdc <- list()
WPS_IDWdc <- list()
for(wps in names(WPS_NNd)){
  WPS_NNdc[[wps]] <- csl(prodWPS[[wps]],WPS_NNd[[wps]])
  WPS_BLIdc[[wps]] <- csl(prodWPS[[wps]],WPS_BLId[[wps]])
  WPS_IDWdc[[wps]] <- csl(prodWPS[[wps]],WPS_IDWd[[wps]])
}

# load capacity correction factors
load(paste(dircaps,"/cap_cfs.RData",sep=""))
# apply capacity correction factors and kWh -> GWh
BRA_NNdc[,3] <- BRA_NNdc[,3]*cfB/10^6
BRA_BLIdc[,3] <- BRA_BLIdc[,3]*cfB/10^6
BRA_IDWdc[,3] <- BRA_IDWdc[,3]*cfB/10^6
SUB_NNdc[['NE']][,3] <- SUB_NNdc[['NE']][,3]*cfNE/10^6
SUB_NNdc[['S']][,3] <- SUB_NNdc[['S']][,3]*cfS/10^6
SUB_BLIdc[['NE']][,3] <- SUB_BLIdc[['NE']][,3]*cfNE/10^6
SUB_BLIdc[['S']][,3] <- SUB_BLIdc[['S']][,3]*cfS/10^6
SUB_IDWdc[['NE']][,3] <- SUB_IDWdc[['NE']][,3]*cfNE/10^6
SUB_IDWdc[['S']][,3] <- SUB_IDWdc[['S']][,3]*cfS/10^6
# of states and stations last two are in S, others in NE
for(i in c(1:(length(STA_NNdc)-2))){
  STA_NNdc[[i]][,3] <- STA_NNdc[[i]][,3]*cfNE/10^6
  WPS_NNdc[[i]][,3] <- WPS_NNdc[[i]][,3]*cfNE/10^6
  STA_BLIdc[[i]][,3] <- STA_BLIdc[[i]][,3]*cfNE/10^6
  WPS_BLIdc[[i]][,3] <- WPS_BLIdc[[i]][,3]*cfNE/10^6
  STA_IDWdc[[i]][,3] <- STA_IDWdc[[i]][,3]*cfNE/10^6
  WPS_IDWdc[[i]][,3] <- WPS_IDWdc[[i]][,3]*cfNE/10^6
}
for(i in c((length(STA_NNdc)-1):length(STA_NNdc))){
  STA_NNdc[[i]][,3] <- STA_NNdc[[i]][,3]*cfS/10^6
  WPS_NNdc[[i]][,3] <- WPS_NNdc[[i]][,3]*cfS/10^6
  STA_BLIdc[[i]][,3] <- STA_BLIdc[[i]][,3]*cfS/10^6
  WPS_BLIdc[[i]][,3] <- WPS_BLIdc[[i]][,3]*cfS/10^6
  STA_IDWdc[[i]][,3] <- STA_IDWdc[[i]][,3]*cfS/10^6
  WPS_IDWdc[[i]][,3] <- WPS_IDWdc[[i]][,3]*cfS/10^6
}
# name columns
names(BRA_NNdc) <- c("time","obs","sim")
names(SUB_NNdc[['NE']]) <- c("time","obs","sim")
names(SUB_NNdc[['S']]) <- c("time","obs","sim")
names(BRA_BLIdc) <- c("time","obs","sim")
names(SUB_BLIdc[['NE']]) <- c("time","obs","sim")
names(SUB_BLIdc[['S']]) <- c("time","obs","sim")
names(BRA_IDWdc) <- c("time","obs","sim")
names(SUB_IDWdc[['NE']]) <- c("time","obs","sim")
names(SUB_IDWdc[['S']]) <- c("time","obs","sim")
for(i in c(1:7)){
  names(STA_NNdc[[i]]) <- c("time","obs","sim")
  names(WPS_NNdc[[i]]) <- c("time","obs","sim")
  names(STA_BLIdc[[i]]) <- c("time","obs","sim")
  names(WPS_BLIdc[[i]]) <- c("time","obs","sim")
  names(STA_IDWdc[[i]]) <- c("time","obs","sim")
  names(WPS_IDWdc[[i]]) <- c("time","obs","sim")
}


# put all in one list
comp_NNd <- list()
comp_NNd[["Brazil"]] <- BRA_NNdc
comp_NNd[["North-East"]] <- SUB_NNdc[["NE"]]
comp_NNd[["South"]] <- SUB_NNdc[["S"]]
comp_BLId <- list()
comp_BLId[["Brazil"]] <- BRA_BLIdc
comp_BLId[["North-East"]] <- SUB_BLIdc[["NE"]]
comp_BLId[["South"]] <- SUB_BLIdc[["S"]]
comp_IDWd <- list()
comp_IDWd[["Brazil"]] <- BRA_IDWdc
comp_IDWd[["North-East"]] <- SUB_IDWdc[["NE"]]
comp_IDWd[["South"]] <- SUB_IDWdc[["S"]]
for(i in c(1:7)){
  comp_NNd[[names(STA_NNdc)[i]]] <- STA_NNdc[[i]]
  comp_NNd[[names(WPS_NNdc)[i]]] <- WPS_NNdc[[i]]
  comp_BLId[[names(STA_BLIdc)[i]]] <- STA_BLIdc[[i]]
  comp_BLId[[names(WPS_BLIdc)[i]]] <- WPS_BLIdc[[i]]
  comp_IDWd[[names(STA_IDWdc)[i]]] <- STA_IDWdc[[i]]
  comp_IDWd[[names(WPS_IDWdc)[i]]] <- WPS_IDWdc[[i]]
}

save(comp_NNd,comp_BLId,comp_IDWd,file=paste0(dirresults,"/comp_noc.RData"))


##### analyse results for Brazil, subsystems, states and wind parks #####
statsNN <- NULL
statsBLI <- NULL
statsIDW <- NULL
types <- data.frame(region=names(comp_NNd),type=c("Brazil+Subs",rep("Brazil+Subs",2),rep(c("State","Windpark"),7)))
for(i in c(1:length(comp_NNd))){
  
  stat <- data.frame(region = names(comp_NNd)[i],
                     area = types$type[which(names(comp_NNd)[i]==types$region)],
                     cor = cor(comp_NNd[[i]]$sim,comp_NNd[[i]]$obs),
                     RMSE = rmse(comp_NNd[[i]]$sim,comp_NNd[[i]]$obs),
                     MBE = mean(comp_NNd[[i]]$sim-comp_NNd[[i]]$obs),
                     mean = mean(comp_NNd[[i]]$sim),
                     obs = mean(comp_NNd[[i]]$obs))
  statsNN <- rbind(statsNN,stat)
  stat <- data.frame(region = names(comp_BLId)[i],
                     area = types$type[which(names(comp_BLId)[i]==types$region)],
                     cor = cor(comp_BLId[[i]]$sim,comp_BLId[[i]]$obs),
                     RMSE = rmse(comp_BLId[[i]]$sim,comp_BLId[[i]]$obs),
                     MBE = mean(comp_BLId[[i]]$sim-comp_BLId[[i]]$obs),
                     mean = mean(comp_BLId[[i]]$sim),
                     obs = mean(comp_BLId[[i]]$obs))
  statsBLI <- rbind(statsBLI,stat)
  stat <- data.frame(region = names(comp_IDWd)[i],
                     area = types$type[which(names(comp_IDWd)[i]==types$region)],
                     cor = cor(comp_IDWd[[i]]$sim,comp_IDWd[[i]]$obs),
                     RMSE = rmse(comp_IDWd[[i]]$sim,comp_IDWd[[i]]$obs),
                     MBE = mean(comp_IDWd[[i]]$sim-comp_IDWd[[i]]$obs),
                     mean = mean(comp_IDWd[[i]]$sim),
                     obs = mean(comp_IDWd[[i]]$obs))
  statsIDW <- rbind(statsIDW,stat)
}

stats <- rbind(data.frame(statsNN,int="NN"),data.frame(statsBLI,int="BLI"),data.frame(statsIDW,int="IDW"))

# prepare stats for table
stats_tidy <- gather(stats,key="type",value="val",-region,-area,-int)

save(stats,stats_tidy,file=paste0(dirresults,"/stats_noc.RData"))

par <- rep(c("cor","RMSE","MBE","mean"),each=3)
sim <- c("NN_","BLI_","IDW_")
vars <- c("region", paste0(sim,par),"NN_obs")
stats_table <- stats_tidy %>% 
                  mutate(val=round(val,3)) %>%
                  arrange(area) %>%
                  mutate(type=paste0(int,"_",type)) %>%
                  dplyr::select(-int) %>%
                  spread(key=type,value=val) %>%
                  arrange(area) %>%
                  dplyr::select(vars) %>%
                  rename(obs = NN_obs)

write.table(stats_table,file=paste(dirresults,"/stats_noc.csv",sep=""),sep=";")





##### relative results #####

load(paste0(dirbase,"/table_mean_caps_validation.RData"))
tab$region <- as.vector(tab$region)
# adapt names of regions
names <- data.frame(old = as.vector(c("Northeast", "Macaubas",   "Praia Formosa",  "Sao Clemente",  "Araripe",   "Alegria II",  "Elebras Cidreira 1", "Bom Jardim",  "Rio Grande do Norte","Rio Grande do Sul","Santa Catarina")),
                    new = as.vector(c("North-East","BA-Macaubas","CE-PraiaFormosa","PE-SaoClemente","PI-Araripe","RN-AlegriaII","RS-ElebrasCidreira1","SC-BomJardim","RioGrandedoNorte",   "RioGrandedoSul",   "SantaCatarina")),stringsAsFactors = FALSE)
tab$region[match(names$old,tab$region)] <- names$new

stats_r <- subset(stats,select=-c(mean,obs))
stats_r$RMSE <- stats$RMSE/(tab$mean_cap_MW[match(stats$region,tab$region)]/1000*24) #/1000 MW->GW, *24 24 hours of day
stats_r$MBE <- stats$MBE/(tab$mean_cap_MW[match(stats$region,tab$region)]/1000*24) #/1000 MW->GW, *24 24 hours of day



##### plot differences for Brazil, subsystems, states and wind parks #####
# Brazil
# merge all data to a tibble
reg <- "Brazil"
dat <- melt(data.frame(NN=comp_NNd[[reg]][,3]-comp_NNd[[reg]][,2],BLI=comp_BLId[[reg]][,3]-comp_NNd[[reg]][,2],IDW=comp_IDWd[[reg]][,3]-comp_NNd[[reg]][,2]))
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  xlab("Interpolation method") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirims,"/diff_B_noc.png",sep=""), width = 6, height = 4.125)
# Subsystems
dat <- NULL
reg <- c("North-East","South")
for(i in reg){
  # merge all data to a tibble
  dat1 <- melt(data.frame(NN=comp_NNd[[i]][,3]-comp_NNd[[i]][,2],BLI=comp_BLId[[i]][,3]-comp_NNd[[i]][,2],IDW=comp_IDWd[[i]][,3]-comp_NNd[[i]][,2]))
  dat1 <- data.frame(subsystem=i,dat1)
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
  ggsave(paste(dirims,"/diff_SUB_noc.png",sep=""), width = 6, height = 4.125)
# States
dat <- NULL
for(i in names(STA_NNdc)){
  # merge all data to a tibble
  dat1 <- melt(data.frame(NN=comp_NNd[[i]][,3]-comp_NNd[[i]][,2],BLI=comp_BLId[[i]][,3]-comp_NNd[[i]][,2],IDW=comp_IDWd[[i]][,3]-comp_NNd[[i]][,2]))
  dat1 <- data.frame(state=i,dat1)
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
  ggsave(paste(dirims,"/diff_STATE_noc.png",sep=""), width = 8, height = 8.25)
# Wind power plants
dat <- NULL
for(i in names(WPS_NNd)){
  # merge all data to a tibble
  dat1 <- melt(data.frame(NN=comp_NNd[[i]][,3]-comp_NNd[[i]][,2],BLI=comp_BLId[[i]][,3]-comp_NNd[[i]][,2],IDW=comp_IDWd[[i]][,3]-comp_NNd[[i]][,2]))
  dat1 <- data.frame(state=i,dat1)
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
  ggsave(paste(dirims,"/diff_stat_noc.png",sep=""), width = 9, height = 4.125)



##### plot statistical analysis #####
# width and height of images
w = 3.375
h = 5
# rows in legend
rw = 4

# correlations and RMSE
stats_rl <- split(stats_r,stats_r$area)
ys <- list(element_text(),element_blank(),element_blank())
col <- list(c("#c72321", "#0d8085", "#efc220"),c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c"),c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c"))
mar <- c(0,-0.3,-0.3)

### correlations
plist <- list()
yl <- c("Daily correlations","","")
for(i in 1:3){
  plot <- 
    ggplot(data=stats_rl[[i]], aes(x=int,y=cor,group=int,color=region)) +
      coord_cartesian(ylim=c(0.45,1)) +
      scale_colour_manual(values=col[[i]]) +
      theme(legend.position = "bottom", axis.text.y = ys[[i]], plot.margin=margin(l=mar[i],unit="cm")) +
      guides(color=guide_legend(nrow=rw,title="")) +
      xlab("Interpolation method") + 
      ylab(yl[i])
  if(i>1){
    plist[[i]] <- plot + 
                    geom_boxplot() + 
                    geom_jitter(lwd=3, width=0.2,height=0)
  }else{
    plist[[i]] <- plot + 
                    geom_jitter(lwd=3, width=0.2,height=0)
  }
}
cowplot::plot_grid(plotlist = plist, nrow=1, align="h")
ggsave(paste0(dirresults,"/pointplots/cor_noc.png"), width = w*2.9, height = h)


### RMSES
plist <- list()
yl <- c("Daily relative RMSE","","")
for(i in 1:3){
  plot <- 
    ggplot(data=stats_rl[[i]], aes(x=int,y=RMSE,group=int,color=region)) +
      coord_cartesian(ylim=c(0.1,0.35)) +
      scale_colour_manual(values=col[[i]]) +
      theme(legend.position = "bottom", axis.text.y = ys[[i]], plot.margin=margin(l=mar[i],unit="cm")) +
      guides(color=guide_legend(nrow=rw,title="")) +
      xlab("Interpolation method") + 
      ylab(yl[i])
  if(i>1){
    plist[[i]] <- plot + 
                    geom_boxplot() + 
                    geom_jitter(lwd=3, width=0.2,height=0)
  }else{
    plist[[i]] <- plot + 
                    geom_jitter(lwd=3, width=0.2,height=0)
  }
}
cowplot::plot_grid(plotlist = plist, nrow=1, align="h")
ggsave(paste0(dirresults,"/pointplots/RMSE_noc.png"), width = w*3, height = h)













#################################################################################################
############## STEP 2: CHOOSE DATA SOURCE FOR WIND SPEED MEAN CORRECTION ########################
############## INMET WIND SPEED MEASUREMENTS OR MEAN WIND SPEEDS GLOBAL WIND ATLAS ##############
#################################################################################################
# general data
LonLat <- read_feather(paste(dirmerra,"/lonlat.feather",sep=""))
date.start <- as.POSIXct("2006-01-01",tz="UTC")
rad <- pi/180


##### INMET #####
# determining factor whether mean approximation is carried out:
# limit for max distance to INMET station: 40 km
statpowlist <- calcstatpower_meanAPT(method=1,wscdata="INMET",INMAXDIST=40)
save(statpowlist,cfs_mean,file=paste0(dirresults,"/statpowlist_wsmaIN.RData")) # wsmaIN = wind speed mean approximation with INMET data
# also calculate with always applying correction for wind parks, disregarding limit of 40 km; but keep for other stations
statpowlist <- calcstatpower_meanAPT(method=1,wscdata="INMET",INMAXDIST=40,applylim=0)
save(statpowlist,cfs_mean,file=paste0(dirresults,"/statpowlist_wsmaIN_allc.RData"))



##### WIND ATLAS #####
# prepare wind atlas data (for faster loading)
tif = raster(paste(dirwindatlas,"/wind_atlas_all_clip.tif",sep=""))
windatlas <- rasterToPoints(tif)
save(windatlas,file=paste(dirwindatlas,"/wind_atlas.RData",sep=""))

# do mean approximation with Wind Atlas Data and Nearest Neighbour method
statpowlist <- calcstatpower_meanAPT(method=1,wscdata="WINDATLAS")
save(statpowlist,file=paste0(dirresults,"/statpowlist_wsmaWA.RData")) # wsmaWA = wind speed mean approximation with Wind Atlas data
save(cfs_mean,file=paste0(dirresults,"/cfs_WA.RData"))



##### prepare results for brazil, subsystems, states and wind parks #####
load(paste(dirresults,"/statpowlist_NN.RData",sep=""))
statpowlistNN = statpowlist
load(paste(dirresults,"/statpowlist_wsmaIN.RData",sep=""))
statpowlistIN <- statpowlist
load(paste(dirresults,"/statpowlist_wsmaWA.RData",sep=""))
statpowlistWA <- statpowlist
rm(statpowlist)

# sum up: Brazil
BRA_NN <- sum_brasil(statpowlistNN)
BRA_IN <- sum_brasil(statpowlistIN)
BRA_WA <- sum_brasil(statpowlistWA)
# states:
STA_NN <- makeSTATEpowlist(statpowlistNN)
STA_IN <- makeSTATEpowlist(statpowlistIN)
STA_WA <- makeSTATEpowlist(statpowlistWA)
# subsystems:
SUB_NN <- sum_subsystem(STA_NN)
SUB_IN <- sum_subsystem(STA_IN)
SUB_WA <- sum_subsystem(STA_WA)
# windparks:
WPS_NN <- makeWPpowlist(statpowlistNN)
WPS_IN <- makeWPpowlist(statpowlistIN)
WPS_WA <- makeWPpowlist(statpowlistWA)

# aggregate daily
# Brazil:
BRA_NNd <- dailyaggregate(list(BRA_NN))[[1]]
BRA_INd <- dailyaggregate(list(BRA_IN))[[1]]
BRA_WAd <- dailyaggregate(list(BRA_WA))[[1]]
# states:
STA_NNd <- dailyaggregate(STA_NN)
names(STA_NNd) <- gsub(" ","",names(STA_NN))
STA_INd <- dailyaggregate(STA_IN)
names(STA_INd) <- gsub(" ","",names(STA_IN))
STA_WAd <- dailyaggregate(STA_WA)
names(STA_WAd) <- gsub(" ","",names(STA_WA))
# subsystems:
SUB_NNd <- dailyaggregate(SUB_NN)
names(SUB_NNd) <- names(SUB_NN)
SUB_INd <- dailyaggregate(SUB_IN)
names(SUB_INd) <- names(SUB_IN)
SUB_WAd <- dailyaggregate(SUB_WA)
names(SUB_WAd) <- names(SUB_WA)
# windparks:
WPS_NNd <- dailyaggregate(WPS_NN)
names(WPS_NNd) <- names(WPS_NN)
WPS_INd <- dailyaggregate(WPS_IN)
names(WPS_INd) <- names(WPS_IN)
WPS_WAd <- dailyaggregate(WPS_WA)
names(WPS_WAd) <- names(WPS_WA)


# get production data
prodBRA <- getprodSUBBRA("BRASIL")
prodNE <- getprodSUBBRA("NE")
prodS <- getprodSUBBRA("S")
prodSTA <- list()
for(state in gsub(" ","",names(STA_NN))){
  prodSTA[[state]] <- getSTATEproddaily(state)
}
prodWPS <- list()
for(wps in names(WPS_NN)){
  prodWPS[[wps]] <- getstatproddaily(substr(wps,1,2))
}

# cut to same lengths
# Brazil:
BRA_NNdc <- csl(prodBRA,BRA_NNd)
BRA_INdc <- csl(prodBRA,BRA_INd)
BRA_WAdc <- csl(prodBRA,BRA_WAd)
# states:
STA_NNdc <- list()
STA_INdc <- list()
STA_WAdc <- list()
for(state in names(STA_NNd)){
  if(!is.null(prodSTA[[state]])){
    STA_NNdc[[state]] <- csl(prodSTA[[state]],STA_NNd[[state]])
    STA_INdc[[state]] <- csl(prodSTA[[state]],STA_INd[[state]])
    STA_WAdc[[state]] <- csl(prodSTA[[state]],STA_WAd[[state]])
  }
}
# subsystems:
SUB_NNdc <- list()
SUB_NNdc[["NE"]] <- csl(prodNE,SUB_NNd$NE)
SUB_NNdc[["S"]] <- csl(prodS,SUB_NNd$S)
SUB_INdc <- list()
SUB_INdc[["NE"]] <- csl(prodNE,SUB_INd$NE)
SUB_INdc[["S"]] <- csl(prodS,SUB_INd$S)
SUB_WAdc <- list()
SUB_WAdc[["NE"]] <- csl(prodNE,SUB_WAd$NE)
SUB_WAdc[["S"]] <- csl(prodS,SUB_WAd$S)
# windparks:
WPS_NNdc <- list()
WPS_INdc <- list()
WPS_WAdc <- list()
for(wps in names(WPS_NNd)){
  WPS_NNdc[[wps]] <- csl(prodWPS[[wps]],WPS_NNd[[wps]])
  WPS_INdc[[wps]] <- csl(prodWPS[[wps]],WPS_INd[[wps]])
  WPS_WAdc[[wps]] <- csl(prodWPS[[wps]],WPS_WAd[[wps]])
}

# load capacity correction factors
load(paste(dircaps,"/cap_cfs.RData",sep=""))
# apply capacity correction factors and kWh -> GWh
BRA_NNdc[,3] <- BRA_NNdc[,3]*cfB/10^6
BRA_INdc[,3] <- BRA_INdc[,3]*cfB/10^6
BRA_WAdc[,3] <- BRA_WAdc[,3]*cfB/10^6
SUB_NNdc[['NE']][,3] <- SUB_NNdc[['NE']][,3]*cfNE/10^6
SUB_NNdc[['S']][,3] <- SUB_NNdc[['S']][,3]*cfS/10^6
SUB_INdc[['NE']][,3] <- SUB_INdc[['NE']][,3]*cfNE/10^6
SUB_INdc[['S']][,3] <- SUB_INdc[['S']][,3]*cfS/10^6
SUB_WAdc[['NE']][,3] <- SUB_WAdc[['NE']][,3]*cfNE/10^6
SUB_WAdc[['S']][,3] <- SUB_WAdc[['S']][,3]*cfS/10^6
# of states and stations last two are in S, others in NE
for(i in c(1:(length(STA_NNdc)-2))){
  STA_NNdc[[i]][,3] <- STA_NNdc[[i]][,3]*cfNE/10^6
  WPS_NNdc[[i]][,3] <- WPS_NNdc[[i]][,3]*cfNE/10^6
  STA_INdc[[i]][,3] <- STA_INdc[[i]][,3]*cfNE/10^6
  WPS_INdc[[i]][,3] <- WPS_INdc[[i]][,3]*cfNE/10^6
  STA_WAdc[[i]][,3] <- STA_WAdc[[i]][,3]*cfNE/10^6
  WPS_WAdc[[i]][,3] <- WPS_WAdc[[i]][,3]*cfNE/10^6
}
for(i in c((length(STA_NNdc)-1):length(STA_NNdc))){
  STA_NNdc[[i]][,3] <- STA_NNdc[[i]][,3]*cfS/10^6
  WPS_NNdc[[i]][,3] <- WPS_NNdc[[i]][,3]*cfS/10^6
  STA_INdc[[i]][,3] <- STA_INdc[[i]][,3]*cfS/10^6
  WPS_INdc[[i]][,3] <- WPS_INdc[[i]][,3]*cfS/10^6
  STA_WAdc[[i]][,3] <- STA_WAdc[[i]][,3]*cfS/10^6
  WPS_WAdc[[i]][,3] <- WPS_WAdc[[i]][,3]*cfS/10^6
}
# name columns
names(BRA_NNdc) <- c("time","obs","sim")
names(SUB_NNdc[['NE']]) <- c("time","obs","sim")
names(SUB_NNdc[['S']]) <- c("time","obs","sim")
names(BRA_INdc) <- c("time","obs","sim")
names(SUB_INdc[['NE']]) <- c("time","obs","sim")
names(SUB_INdc[['S']]) <- c("time","obs","sim")
names(BRA_WAdc) <- c("time","obs","sim")
names(SUB_WAdc[['NE']]) <- c("time","obs","sim")
names(SUB_WAdc[['S']]) <- c("time","obs","sim")
for(i in c(1:7)){
  names(STA_NNdc[[i]]) <- c("time","obs","sim")
  names(WPS_NNdc[[i]]) <- c("time","obs","sim")
  names(STA_INdc[[i]]) <- c("time","obs","sim")
  names(WPS_INdc[[i]]) <- c("time","obs","sim")
  names(STA_WAdc[[i]]) <- c("time","obs","sim")
  names(WPS_WAdc[[i]]) <- c("time","obs","sim")
}


# put all in one list
comp_NNd <- list()
comp_NNd[["Brazil"]] <- BRA_NNdc
comp_NNd[["North-East"]] <- SUB_NNdc[["NE"]]
comp_NNd[["South"]] <- SUB_NNdc[["S"]]
comp_INd <- list()
comp_INd[["Brazil"]] <- BRA_INdc
comp_INd[["North-East"]] <- SUB_INdc[["NE"]]
comp_INd[["South"]] <- SUB_INdc[["S"]]
comp_WAd <- list()
comp_WAd[["Brazil"]] <- BRA_WAdc
comp_WAd[["North-East"]] <- SUB_WAdc[["NE"]]
comp_WAd[["South"]] <- SUB_WAdc[["S"]]
for(i in c(1:7)){
  comp_NNd[[names(STA_NNdc)[i]]] <- STA_NNdc[[i]]
  comp_NNd[[names(WPS_NNdc)[i]]] <- WPS_NNdc[[i]]
  comp_INd[[names(STA_INdc)[i]]] <- STA_INdc[[i]]
  comp_INd[[names(WPS_INdc)[i]]] <- WPS_INdc[[i]]
  comp_WAd[[names(STA_WAdc)[i]]] <- STA_WAdc[[i]]
  comp_WAd[[names(WPS_WAdc)[i]]] <- WPS_WAdc[[i]]
}

save(comp_NNd,comp_INd,comp_WAd,file=paste0(dirresults,"/comp_wsma.RData"))


##### analyse results for Brazil, subsystems, states and wind parks #####
statsNN <- NULL
statsIN <- NULL
statsWA <- NULL
types <- data.frame(region=names(comp_NNd),type=c("Brazil+Subs",rep("Brazil+Subs",2),rep(c("State","Windpark"),7)))
for(i in c(1:length(comp_NNd))){
  
  stat <- data.frame(region = names(comp_NNd)[i],
                     area = types$type[which(names(comp_NNd)[i]==types$region)],
                     cor = cor(comp_NNd[[i]]$sim,comp_NNd[[i]]$obs),
                     RMSE = rmse(comp_NNd[[i]]$sim,comp_NNd[[i]]$obs),
                     MBE = mean(comp_NNd[[i]]$sim-comp_NNd[[i]]$obs),
                     mean = mean(comp_NNd[[i]]$sim),
                     obs = mean(comp_NNd[[i]]$obs))
  statsNN <- rbind(statsNN,stat)
  stat <- data.frame(region = names(comp_INd)[i],
                     area = types$type[which(names(comp_INd)[i]==types$region)],
                     cor = cor(comp_INd[[i]]$sim,comp_INd[[i]]$obs),
                     RMSE = rmse(comp_INd[[i]]$sim,comp_INd[[i]]$obs),
                     MBE = mean(comp_INd[[i]]$sim-comp_INd[[i]]$obs),
                     mean = mean(comp_INd[[i]]$sim),
                     obs = mean(comp_INd[[i]]$obs))
  statsIN <- rbind(statsIN,stat)
  stat <- data.frame(region = names(comp_WAd)[i],
                     area = types$type[which(names(comp_WAd)[i]==types$region)],
                     cor = cor(comp_WAd[[i]]$sim,comp_WAd[[i]]$obs),
                     RMSE = rmse(comp_WAd[[i]]$sim,comp_WAd[[i]]$obs),
                     MBE = mean(comp_WAd[[i]]$sim-comp_WAd[[i]]$obs),
                     mean = mean(comp_WAd[[i]]$sim),
                     obs = mean(comp_WAd[[i]]$obs))
  statsWA <- rbind(statsWA,stat)
}

stats <- rbind(data.frame(statsNN,int="NN"),data.frame(statsIN,int="INMET"),data.frame(statsWA,int="GWA"))

# prepare stats for table
stats_tidy <- gather(stats,key="type",value="val",-region,-area,-int)

save(stats,stats_tidy,file=paste0(dirresults,"/stats_wsma.RData"))

par <- rep(c("cor","RMSE","MBE","mean"),each=3)
sim <- c("NN_","INMET_","GWA_")
vars <- c("region", paste0(sim,par),"NN_obs")
stats_table <- stats_tidy %>% 
  mutate(val=round(val,3)) %>%
  arrange(area) %>%
  mutate(type=paste0(int,"_",type)) %>%
  dplyr::select(-int) %>%
  spread(key=type,value=val) %>%
  arrange(area) %>%
  dplyr::select(vars) %>%
  rename(obs = NN_obs)

write.table(stats_table,file=paste(dirresults,"/stats_wsma.csv",sep=""),sep=";")


##### relative results #####

load(paste0(dirbase,"/table_mean_caps_validation.RData"))
tab$region <- as.vector(tab$region)
# adapt names of regions
names <- data.frame(old = as.vector(c("Northeast", "Macaubas",   "Praia Formosa",  "Sao Clemente",  "Araripe",   "Alegria II",  "Elebras Cidreira 1", "Bom Jardim",  "Rio Grande do Norte","Rio Grande do Sul","Santa Catarina")),
                    new = as.vector(c("North-East","BA-Macaubas","CE-PraiaFormosa","PE-SaoClemente","PI-Araripe","RN-AlegriaII","RS-ElebrasCidreira1","SC-BomJardim","RioGrandedoNorte",   "RioGrandedoSul",   "SantaCatarina")),stringsAsFactors = FALSE)
tab$region[match(names$old,tab$region)] <- names$new

stats_r <- subset(stats,select=-c(mean,obs))
stats_r$RMSE <- stats$RMSE/(tab$mean_cap_MW[match(stats$region,tab$region)]/1000*24) #/1000 MW->GW, *24 24 hours of day
stats_r$MBE <- stats$MBE/(tab$mean_cap_MW[match(stats$region,tab$region)]/1000*24) #/1000 MW->GW, *24 24 hours of day



##### plot differences for Brazil, subsystems, states and wind parks #####
# Brazil
# merge all data to a tibble
reg <- "Brazil"
dat <- melt(data.frame(NN=comp_NNd[[reg]][,3]-comp_NNd[[reg]][,2],INMET=comp_INd[[reg]][,3]-comp_NNd[[reg]][,2],GWA=comp_WAd[[reg]][,3]-comp_NNd[[reg]][,2]))
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  xlab("Wind speed mean approximation source") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirims,"/diff_B_wsma.png",sep=""), width = 6, height = 4.125)
# Subsystems
dat <- NULL
reg <- c("North-East","South")
for(i in reg){
  # merge all data to a tibble
  dat1 <- melt(data.frame(NN=comp_NNd[[i]][,3]-comp_NNd[[i]][,2],INMET=comp_INd[[i]][,3]-comp_NNd[[i]][,2],GWA=comp_WAd[[i]][,3]-comp_NNd[[i]][,2]))
  dat1 <- data.frame(subsystem=i,dat1)
  if(length(dat)>1){
    dat <- rbind(dat,dat1)
  }else{
    dat <- dat1
  }
}
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  facet_wrap(~subsystem) +
  xlab("Wind speed mean approximation source") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirims,"/diff_SUB_wsma.png",sep=""), width = 6, height = 4.125)
# States
dat <- NULL
for(i in names(STA_NNdc)){
  # merge all data to a tibble
  dat1 <- melt(data.frame(NN=comp_NNd[[i]][,3]-comp_NNd[[i]][,2],INMET=comp_INd[[i]][,3]-comp_NNd[[i]][,2],GWA=comp_WAd[[i]][,3]-comp_NNd[[i]][,2]))
  dat1 <- data.frame(state=i,dat1)
  if(length(dat)>1){
    dat <- rbind(dat,dat1)
  }else{
    dat <- dat1
  }
}
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  facet_wrap(~state,nrow=2) +
  xlab("Wind speed mean approximation source") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirims,"/diff_STATE_wsma.png",sep=""), width = 9, height = 4.125)
# Wind power plants
dat <- NULL
for(i in names(WPS_NNd)){
  # merge all data to a tibble
  dat1 <- melt(data.frame(NN=comp_NNd[[i]][,3]-comp_NNd[[i]][,2],INMET=comp_INd[[i]][,3]-comp_NNd[[i]][,2],GWA=comp_WAd[[i]][,3]-comp_NNd[[i]][,2]))
  dat1 <- data.frame(state=i,dat1)
  if(length(dat)>1){
    dat <- rbind(dat,dat1)
  }else{
    dat <- dat1
  }
}
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  facet_wrap(~state,nrow=2) +
  xlab("Wind speed mean approximation source") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirims,"/diff_stat_wsma.png",sep=""), width = 9, height = 4.125)



##### plot statistical analysis #####
# width and height of images
w = 3.375
h = 5
# rows in legend
rw = 4


# get mean INMET correction factors of wind parks, to find out which ones have not been bias corrected
uncor <- getWPmeanCFs(cfs_mean)
# set special settings for those
stats_r$size <- 3
stats_r$size[(stats_r$region %in% uncor[which(uncor[,2]==1),1])&(stats_r$int=="INMET")] <- 3.5
stats_r$shape <- 19
stats_r$shape[(stats_r$region %in% uncor[which(uncor[,2]==1),1])&(stats_r$int=="INMET")] <- 15


# correlations, RMSE and MBE
stats_rl <- split(stats_r,stats_r$area)
ys <- list(element_text(),element_blank(),element_blank())
col <- list(c("#c72321", "#0d8085", "#efc220"),c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c"),c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c"))
mar <- c(0,-0.3,-0.3)

### correlations
plist <- list()
yl <- c("Daily correlations","","")
for(i in 1:3){
  plot <- 
    ggplot(data=stats_rl[[i]], aes(x=int,y=cor,group=int,color=region)) +
    coord_cartesian(ylim=c(0.2,1)) +
    scale_colour_manual(values=col[[i]]) +
    theme(legend.position = "bottom", axis.text.y = ys[[i]], plot.margin=margin(l=mar[i],unit="cm")) +
    guides(color=guide_legend(nrow=rw,title="")) +
    xlab("Wind speed mean approximation source") + 
    ylab(yl[i])
  if(i>1){
    plist[[i]] <- plot + 
      geom_boxplot() + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl[[i]]$size,shape=stats_rl[[i]]$shape)
  }else{
    plist[[i]] <- plot + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl[[i]]$size,shape=stats_rl[[i]]$shape)
  }
}
cowplot::plot_grid(plotlist = plist, nrow=1, align="h")
ggsave(paste0(dirresults,"/pointplots/cor_wsma.png"), width = w*2.9, height = h)


### RMSES
plist <- list()
yl <- c("Daily relative RMSE","","")
for(i in 1:3){
  plot <- 
    ggplot(data=stats_rl[[i]], aes(x=int,y=RMSE,group=int,color=region)) +
    coord_cartesian(ylim=c(0.085,0.455)) +
    scale_colour_manual(values=col[[i]]) +
    theme(legend.position = "bottom", axis.text.y = ys[[i]], plot.margin=margin(l=mar[i],unit="cm")) +
    guides(color=guide_legend(nrow=rw,title="")) +
    xlab("Wind speed mean approximation source") + 
    ylab(yl[i])
  if(i>1){
    plist[[i]] <- plot + 
      geom_boxplot() + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl[[i]]$size,shape=stats_rl[[i]]$shape)
  }else{
    plist[[i]] <- plot + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl[[i]]$size,shape=stats_rl[[i]]$shape)
  }
}
cowplot::plot_grid(plotlist = plist, nrow=1, align="h")
ggsave(paste0(dirresults,"/pointplots/RMSE_wsma.png"), width = w*2.9, height = h)


### MBEs
plist <- list()
yl <- c("Daily relative MBE","","")
for(i in 1:3){
  plot <- 
    ggplot(data=stats_rl[[i]], aes(x=int,y=MBE,group=int,color=region)) +
    coord_cartesian(ylim=c(-0.45,0.375)) +
    scale_colour_manual(values=col[[i]]) +
    theme(legend.position = "bottom", axis.text.y = ys[[i]], plot.margin=margin(l=mar[i],unit="cm")) +
    guides(color=guide_legend(nrow=rw,title="")) +
    xlab("Wind speed mean approximation source") + 
    ylab(yl[i])
  if(i>1){
    plist[[i]] <- plot + 
      geom_boxplot() + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl[[i]]$size,shape=stats_rl[[i]]$shape)
  }else{
    plist[[i]] <- plot + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl[[i]]$size,shape=stats_rl[[i]]$shape)
  }
}
cowplot::plot_grid(plotlist = plist, nrow=1, align="h")
ggsave(paste0(dirresults,"/pointplots/MBE_wsma.png"), width = w*2.9, height = h)











##########################################################################################
############ STEP 3: COMPARISON OF DIFFERENT WIND SPEED CORRECTION METHODS: ##############
############ MONTHLY, MONTHLY + HOURLY, ONLY MEAN APPROXIMATION ##########################
##########################################################################################

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



# determining factor whether wind speed correction is carried out:
# limit for max distance to INMET station: 40 km
# calculate wind power generation with wind speed correction

# hourly and monthly
statpowlist <- calcstatpower_windcor(INmaxdist=40,corrlimit=0.5,method=1,mhm="hm",applylim=1)
save(statpowlist,file=paste0(dirresults,"/statpowlist_wschm.RData")) # wschm = wind speed correction hourly and monthly

# monthly
statpowlist <- calcstatpower_windcor(INmaxdist=40,corrlimit=0.5,method=1,mhm="m",applylim=1)
save(statpowlist,file=paste0(dirresults,"/statpowlist_wscm.RData")) # wscm = wind speed correction monthly









##### prepare results for brazil, subsystems and states #####
load(paste(dirresults,"/statpowlist_wsmaWA.RData",sep=""))
statpowlist_wsma <- statpowlist
load(paste(dirresults,"/statpowlist_wschm.RData",sep=""))
statpowlist_wschm <- statpowlist
load(paste(dirresults,"/statpowlist_wscm.RData",sep=""))
statpowlist_wscm <- statpowlist
rm(statpowlist)

# sum up: Brazil
BRA_wsma <- sum_brasil(statpowlist_wsma)
BRA_wscm <- sum_brasil(statpowlist_wscm)
BRA_wschm <- sum_brasil(statpowlist_wschm)
# states:
STA_wsma <- makeSTATEpowlist(statpowlist_wsma)
STA_wscm <- makeSTATEpowlist(statpowlist_wscm)
STA_wschm <- makeSTATEpowlist(statpowlist_wschm)
# subsystems:
SUB_wsma <- sum_subsystem(STA_wsma)
SUB_wscm <- sum_subsystem(STA_wscm)
SUB_wschm <- sum_subsystem(STA_wschm)
# windparks:
WPS_wsma <- makeWPpowlist(statpowlist_wsma)
WPS_wscm <- makeWPpowlist(statpowlist_wscm)
WPS_wschm <- makeWPpowlist(statpowlist_wschm)

# aggregate daily
# Brazil:
BRA_wsmad <- dailyaggregate(list(BRA_wsma))[[1]]
BRA_wscmd <- dailyaggregate(list(BRA_wscm))[[1]]
BRA_wschmd <- dailyaggregate(list(BRA_wschm))[[1]]
# states:
STA_wsmad <- dailyaggregate(STA_wsma)
names(STA_wsmad) <- gsub(" ","",names(STA_wsma))
STA_wscmd <- dailyaggregate(STA_wscm)
names(STA_wscmd) <- gsub(" ","",names(STA_wscm))
STA_wschmd <- dailyaggregate(STA_wschm)
names(STA_wschmd) <- gsub(" ","",names(STA_wschm))
# subsystems:
SUB_wsmad <- dailyaggregate(SUB_wsma)
names(SUB_wsmad) <- names(SUB_wsma)
SUB_wscmd <- dailyaggregate(SUB_wscm)
names(SUB_wscmd) <- names(SUB_wscm)
SUB_wschmd <- dailyaggregate(SUB_wschm)
names(SUB_wschmd) <- names(SUB_wschm)
# windparks:
WPS_wsmad <- dailyaggregate(WPS_wsma)
names(WPS_wsmad) <- names(WPS_wsma)
WPS_wscmd <- dailyaggregate(WPS_wscm)
names(WPS_wscmd) <- names(WPS_wscm)
WPS_wschmd <- dailyaggregate(WPS_wschm)
names(WPS_wschmd) <- names(WPS_wschm)


# get production data
prodBRA <- getprodSUBBRA("BRASIL")
prodNE <- getprodSUBBRA("NE")
prodS <- getprodSUBBRA("S")
prodSTA <- list()
for(state in gsub(" ","",names(STA_wsma))){
  prodSTA[[state]] <- getSTATEproddaily(state)
}
prodWPS <- list()
for(wps in names(WPS_wsma)){
  prodWPS[[wps]] <- getstatproddaily(substr(wps,1,2))
}

# cut to same lengths
# Brazil:
BRA_wsmadc <- csl(prodBRA,BRA_wsmad)
BRA_wscmdc <- csl(prodBRA,BRA_wscmd)
BRA_wschmdc <- csl(prodBRA,BRA_wschmd)
# states:
STA_wsmadc <- list()
STA_wscmdc <- list()
STA_wschmdc <- list()
for(state in names(STA_wsmad)){
  if(!is.null(prodSTA[[state]])){
    STA_wsmadc[[state]] <- csl(prodSTA[[state]],STA_wsmad[[state]])
    STA_wscmdc[[state]] <- csl(prodSTA[[state]],STA_wscmd[[state]])
    STA_wschmdc[[state]] <- csl(prodSTA[[state]],STA_wschmd[[state]])
  }
}
# subsystems:
SUB_wsmadc <- list()
SUB_wsmadc[["NE"]] <- csl(prodNE,SUB_wsmad$NE)
SUB_wsmadc[["S"]] <- csl(prodS,SUB_wsmad$S)
SUB_wscmdc <- list()
SUB_wscmdc[["NE"]] <- csl(prodNE,SUB_wscmd$NE)
SUB_wscmdc[["S"]] <- csl(prodS,SUB_wscmd$S)
SUB_wschmdc <- list()
SUB_wschmdc[["NE"]] <- csl(prodNE,SUB_wschmd$NE)
SUB_wschmdc[["S"]] <- csl(prodS,SUB_wschmd$S)
# windparks:
WPS_wsmadc <- list()
WPS_wscmdc <- list()
WPS_wschmdc <- list()
for(wps in names(WPS_wsmad)){
  WPS_wsmadc[[wps]] <- csl(prodWPS[[wps]],WPS_wsmad[[wps]])
  WPS_wscmdc[[wps]] <- csl(prodWPS[[wps]],WPS_wscmd[[wps]])
  WPS_wschmdc[[wps]] <- csl(prodWPS[[wps]],WPS_wschmd[[wps]])
}

# load capacity correction factors
load(paste(dircaps,"/cap_cfs.RData",sep=""))
# apply capacity correction factors and kWh -> GWh
BRA_wsmadc[,3] <- BRA_wsmadc[,3]*cfB/10^6
BRA_wscmdc[,3] <- BRA_wscmdc[,3]*cfB/10^6
BRA_wschmdc[,3] <- BRA_wschmdc[,3]*cfB/10^6
SUB_wsmadc[['NE']][,3] <- SUB_wsmadc[['NE']][,3]*cfNE/10^6
SUB_wsmadc[['S']][,3] <- SUB_wsmadc[['S']][,3]*cfS/10^6
SUB_wscmdc[['NE']][,3] <- SUB_wscmdc[['NE']][,3]*cfNE/10^6
SUB_wscmdc[['S']][,3] <- SUB_wscmdc[['S']][,3]*cfS/10^6
SUB_wschmdc[['NE']][,3] <- SUB_wschmdc[['NE']][,3]*cfNE/10^6
SUB_wschmdc[['S']][,3] <- SUB_wschmdc[['S']][,3]*cfS/10^6
# of states and stations last two are in S, others in NE
for(i in c(1:(length(STA_wsmadc)-2))){
  STA_wsmadc[[i]][,3] <- STA_wsmadc[[i]][,3]*cfNE/10^6
  WPS_wsmadc[[i]][,3] <- WPS_wsmadc[[i]][,3]*cfNE/10^6
  STA_wscmdc[[i]][,3] <- STA_wscmdc[[i]][,3]*cfNE/10^6
  WPS_wscmdc[[i]][,3] <- WPS_wscmdc[[i]][,3]*cfNE/10^6
  STA_wschmdc[[i]][,3] <- STA_wschmdc[[i]][,3]*cfNE/10^6
  WPS_wschmdc[[i]][,3] <- WPS_wschmdc[[i]][,3]*cfNE/10^6
}
for(i in c((length(STA_wsmadc)-1):length(STA_wsmadc))){
  STA_wsmadc[[i]][,3] <- STA_wsmadc[[i]][,3]*cfS/10^6
  WPS_wsmadc[[i]][,3] <- WPS_wsmadc[[i]][,3]*cfS/10^6
  STA_wscmdc[[i]][,3] <- STA_wscmdc[[i]][,3]*cfS/10^6
  WPS_wscmdc[[i]][,3] <- WPS_wscmdc[[i]][,3]*cfS/10^6
  STA_wschmdc[[i]][,3] <- STA_wschmdc[[i]][,3]*cfS/10^6
  WPS_wschmdc[[i]][,3] <- WPS_wschmdc[[i]][,3]*cfS/10^6
}
# name columns
names(BRA_wsmadc) <- c("time","obs","sim")
names(SUB_wsmadc[['NE']]) <- c("time","obs","sim")
names(SUB_wsmadc[['S']]) <- c("time","obs","sim")
names(BRA_wscmdc) <- c("time","obs","sim")
names(SUB_wscmdc[['NE']]) <- c("time","obs","sim")
names(SUB_wscmdc[['S']]) <- c("time","obs","sim")
names(BRA_wschmdc) <- c("time","obs","sim")
names(SUB_wschmdc[['NE']]) <- c("time","obs","sim")
names(SUB_wschmdc[['S']]) <- c("time","obs","sim")
for(i in c(1:7)){
  names(STA_wsmadc[[i]]) <- c("time","obs","sim")
  names(WPS_wsmadc[[i]]) <- c("time","obs","sim")
  names(STA_wscmdc[[i]]) <- c("time","obs","sim")
  names(WPS_wscmdc[[i]]) <- c("time","obs","sim")
  names(STA_wschmdc[[i]]) <- c("time","obs","sim")
  names(WPS_wschmdc[[i]]) <- c("time","obs","sim")
}


# put all in one list
comp_wsmad <- list()
comp_wsmad[["Brazil"]] <- BRA_wsmadc
comp_wsmad[["North-East"]] <- SUB_wsmadc[["NE"]]
comp_wsmad[["South"]] <- SUB_wsmadc[["S"]]
comp_wscmd <- list()
comp_wscmd[["Brazil"]] <- BRA_wscmdc
comp_wscmd[["North-East"]] <- SUB_wscmdc[["NE"]]
comp_wscmd[["South"]] <- SUB_wscmdc[["S"]]
comp_wschmd <- list()
comp_wschmd[["Brazil"]] <- BRA_wschmdc
comp_wschmd[["North-East"]] <- SUB_wschmdc[["NE"]]
comp_wschmd[["South"]] <- SUB_wschmdc[["S"]]
for(i in c(1:7)){
  comp_wsmad[[names(STA_wsmadc)[i]]] <- STA_wsmadc[[i]]
  comp_wsmad[[names(WPS_wsmadc)[i]]] <- WPS_wsmadc[[i]]
  comp_wscmd[[names(STA_wscmdc)[i]]] <- STA_wscmdc[[i]]
  comp_wscmd[[names(WPS_wscmdc)[i]]] <- WPS_wscmdc[[i]]
  comp_wschmd[[names(STA_wschmdc)[i]]] <- STA_wschmdc[[i]]
  comp_wschmd[[names(WPS_wschmdc)[i]]] <- WPS_wschmdc[[i]]
}

save(comp_wsmad,comp_wscmd,comp_wschmd,file=paste0(dirresults,"/comp_wsc.RData"))



##### analyse results for Brazil, subsystems, states and wind parks #####
stats_wsma <- NULL
stats_wscm <- NULL
stats_wschm <- NULL
types <- data.frame(region=names(comp_wsmad),type=c("Brazil+Subs",rep("Brazil+Subs",2),rep(c("State","Windpark"),7)))
for(i in c(1:length(comp_wsmad))){
  
  stat <- data.frame(region = names(comp_wsmad)[i],
                     area = types$type[which(names(comp_wsmad)[i]==types$region)],
                     cor = cor(comp_wsmad[[i]]$sim,comp_wsmad[[i]]$obs),
                     RMSE = rmse(comp_wsmad[[i]]$sim,comp_wsmad[[i]]$obs),
                     MBE = mean(comp_wsmad[[i]]$sim-comp_wsmad[[i]]$obs),
                     mean = mean(comp_wsmad[[i]]$sim),
                     obs = mean(comp_wsmad[[i]]$obs))
  stats_wsma <- rbind(stats_wsma,stat)
  stat <- data.frame(region = names(comp_wscmd)[i],
                     area = types$type[which(names(comp_wscmd)[i]==types$region)],
                     cor = cor(comp_wscmd[[i]]$sim,comp_wscmd[[i]]$obs),
                     RMSE = rmse(comp_wscmd[[i]]$sim,comp_wscmd[[i]]$obs),
                     MBE = mean(comp_wscmd[[i]]$sim-comp_wscmd[[i]]$obs),
                     mean = mean(comp_wscmd[[i]]$sim),
                     obs = mean(comp_wscmd[[i]]$obs))
  stats_wscm <- rbind(stats_wscm,stat)
  stat <- data.frame(region = names(comp_wschmd)[i],
                     area = types$type[which(names(comp_wschmd)[i]==types$region)],
                     cor = cor(comp_wschmd[[i]]$sim,comp_wschmd[[i]]$obs),
                     RMSE = rmse(comp_wschmd[[i]]$sim,comp_wschmd[[i]]$obs),
                     MBE = mean(comp_wschmd[[i]]$sim-comp_wschmd[[i]]$obs),
                     mean = mean(comp_wschmd[[i]]$sim),
                     obs = mean(comp_wschmd[[i]]$obs))
  stats_wschm <- rbind(stats_wschm,stat)
}

stats <- rbind(data.frame(stats_wsma,int="wsma"),data.frame(stats_wscm,int="wsc_m"),data.frame(stats_wschm,int="wsc_hm"))

# prepare stats for table
stats_tidy <- gather(stats,key="type",value="val",-region,-area,-int)

save(stats,stats_tidy,file=paste0(dirresults,"/stats_wsc.RData"))

par <- rep(c("cor","RMSE","MBE","mean"),each=3)
sim <- c("wsma_","wsc_m_","wsc_hm_")
vars <- c("region", paste0(sim,par),"wsma_obs")
stats_table <- stats_tidy %>% 
  mutate(val=round(val,3)) %>%
  arrange(area) %>%
  mutate(type=paste0(int,"_",type)) %>%
  dplyr::select(-int) %>%
  spread(key=type,value=val) %>%
  arrange(area) %>%
  dplyr::select(vars) %>%
  rename(obs = wsma_obs)

write.table(stats_table,file=paste(dirresults,"/stats_wsc.csv",sep=""),sep=";")


##### relative results #####

load(paste0(dirbase,"/table_mean_caps_validation.RData"))
tab$region <- as.vector(tab$region)
# adapt names of regions
names <- data.frame(old = as.vector(c("Northeast", "Macaubas",   "Praia Formosa",  "Sao Clemente",  "Araripe",   "Alegria II",  "Elebras Cidreira 1", "Bom Jardim",  "Rio Grande do Norte","Rio Grande do Sul","Santa Catarina")),
                    new = as.vector(c("North-East","BA-Macaubas","CE-PraiaFormosa","PE-SaoClemente","PI-Araripe","RN-AlegriaII","RS-ElebrasCidreira1","SC-BomJardim","RioGrandedoNorte",   "RioGrandedoSul",   "SantaCatarina")),stringsAsFactors = FALSE)
tab$region[match(names$old,tab$region)] <- names$new

stats_r <- subset(stats,select=-c(mean,obs))
stats_r$RMSE <- stats$RMSE/(tab$mean_cap_MW[match(stats$region,tab$region)]/1000*24) #/1000 MW->GW, *24 24 hours of day
stats_r$MBE <- stats$MBE/(tab$mean_cap_MW[match(stats$region,tab$region)]/1000*24) #/1000 MW->GW, *24 24 hours of day



##### plot differences for Brazil, subsystems, states and wind parks #####
# Brazil
# merge all data to a tibble
reg <- "Brazil"
dat <- melt(data.frame(wsma=comp_wsmad[[reg]][,3]-comp_wsmad[[reg]][,2],wsc_m=comp_wscmd[[reg]][,3]-comp_wsmad[[reg]][,2],wsc_hm=comp_wschmd[[reg]][,3]-comp_wsmad[[reg]][,2]))
ggplot(data=dat,aes(x=variable,y=value)) +
  geom_boxplot() +
  xlab("Wind speed correction method") +
  scale_y_continuous(name="Differences in daily wind power generation [GWh]") +
  ggsave(paste(dirims,"/diff_B_wsc.png",sep=""), width = 6, height = 4.125)
# Subsystems
dat <- NULL
reg <- c("North-East","South")
for(i in reg){
  # merge all data to a tibble
  dat1 <- melt(data.frame(wsma=comp_wsmad[[i]][,3]-comp_wsmad[[i]][,2],wsc_m=comp_wscmd[[i]][,3]-comp_wsmad[[i]][,2],wsc_hm=comp_wschmd[[i]][,3]-comp_wsmad[[i]][,2]))
  dat1 <- data.frame(subsystem=i,dat1)
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
  ggsave(paste(dirims,"/diff_SUB_wsc.png",sep=""), width = 6, height = 4.125)
# States
dat <- NULL
for(i in names(STA_wsmadc)){
  # merge all data to a tibble
  dat1 <- melt(data.frame(wsma=comp_wsmad[[i]][,3]-comp_wsmad[[i]][,2],wsc_m=comp_wscmd[[i]][,3]-comp_wsmad[[i]][,2],wsc_hm=comp_wschmd[[i]][,3]-comp_wsmad[[i]][,2]))
  dat1 <- data.frame(state=i,dat1)
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
  ggsave(paste(dirims,"/diff_STATE_wsc.png",sep=""), width = 9, height = 4.125)
# Wind power plants
dat <- NULL
for(i in names(WPS_wsmad)){
  # merge all data to a tibble
  dat1 <- melt(data.frame(wsma=comp_wsmad[[i]][,3]-comp_wsmad[[i]][,2],wsc_m=comp_wscmd[[i]][,3]-comp_wsmad[[i]][,2],wsc_hm=comp_wschmd[[i]][,3]-comp_wsmad[[i]][,2]))
  dat1 <- data.frame(state=i,dat1)
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
  ggsave(paste(dirims,"/diff_stat_wsc.png",sep=""), width = 9, height = 4.125)



##### plot statistical analysis #####
# width and height of images
w = 3.375
h = 5
# rows in legend
rw = 4
# find out which areas have been corrected with INMET
change <-  c(rep(FALSE,17),rep(filter(stats_r,int=="wsma")$RMSE,2)==filter(stats_r,int %in% c("wsc_m","wsc_hm"))$RMSE)

# set special settings for those
stats_r$size <- 3
stats_r$size[change] <- 3.5
stats_r$shape <- 19
stats_r$shape[change] <- 15


# correlations, RMSE and MBE
stats_rl <- split(stats_r,stats_r$area)
ys <- list(element_text(),element_blank(),element_blank())
col <- list(c("#c72321", "#0d8085", "#efc220"),c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c"),c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c"))
mar <- c(0,-0.3,-0.3)

### correlations
plist <- list()
yl <- c("Daily correlations","","")
for(i in 1:3){
  plot <- 
    ggplot(data=stats_rl[[i]], aes(x=int,y=cor,group=int,color=region)) +
    coord_cartesian(ylim=c(0.2,1)) +
    scale_colour_manual(values=col[[i]]) +
    theme(legend.position = "bottom", axis.text.y = ys[[i]], plot.margin=margin(l=mar[i],unit="cm")) +
    guides(color=guide_legend(nrow=rw,title="")) +
    xlab("Wind speed correction method") + 
    ylab(yl[i])
  if(i>1){
    plist[[i]] <- plot + 
      geom_boxplot() + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl[[i]]$size,shape=stats_rl[[i]]$shape)
  }else{
    plist[[i]] <- plot + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl[[i]]$size,shape=stats_rl[[i]]$shape)
  }
}
cowplot::plot_grid(plotlist = plist, nrow=1, align="h")
ggsave(paste0(dirresults,"/pointplots/cor_wsc.png"), width = w*2.9, height = h)


### RMSES
plist <- list()
yl <- c("Daily relative RMSE","","")
for(i in 1:3){
  plot <- 
    ggplot(data=stats_rl[[i]], aes(x=int,y=RMSE,group=int,color=region)) +
    coord_cartesian(ylim=c(0.1,0.35)) +
    scale_colour_manual(values=col[[i]]) +
    theme(legend.position = "bottom", axis.text.y = ys[[i]], plot.margin=margin(l=mar[i],unit="cm")) +
    guides(color=guide_legend(nrow=rw,title="")) +
    xlab("Wind speed correction method") + 
    ylab(yl[i])
  if(i>1){
    plist[[i]] <- plot + 
      geom_boxplot() + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl[[i]]$size,shape=stats_rl[[i]]$shape)
  }else{
    plist[[i]] <- plot + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl[[i]]$size,shape=stats_rl[[i]]$shape)
  }
}
cowplot::plot_grid(plotlist = plist, nrow=1, align="h")
ggsave(paste0(dirresults,"/pointplots/RMSE_wsc.png"), width = w*2.9, height = h)


### MBEs
plist <- list()
yl <- c("Daily relative MBE","","")
for(i in 1:3){
  plot <- 
    ggplot(data=stats_rl[[i]], aes(x=int,y=MBE,group=int,color=region)) +
    coord_cartesian(ylim=c(-0.45,0.375)) +
    scale_colour_manual(values=col[[i]]) +
    theme(legend.position = "bottom", axis.text.y = ys[[i]], plot.margin=margin(l=mar[i],unit="cm")) +
    guides(color=guide_legend(nrow=rw,title="")) +
    xlab("Wind speed correction method") + 
    ylab(yl[i])
  if(i>1){
    plist[[i]] <- plot + 
      geom_boxplot() + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl[[i]]$size,shape=stats_rl[[i]]$shape)
  }else{
    plist[[i]] <- plot + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl[[i]]$size,shape=stats_rl[[i]]$shape)
  }
}
cowplot::plot_grid(plotlist = plist, nrow=1, align="h")
ggsave(paste0(dirresults,"/pointplots/MBE_wsc.png"), width = w*2.9, height = h)








###################### ALWAYS APPLY CORRECTION FOR SINGLE WIND PARKS ######################
###################### BECAUSE OTHERWISE HARDLY WIND SPEED CORRECTION #####################
minmonth = 4
mindaynum = 30
monthlim = 1
shortmonths = 10
LonLat <- read_feather(paste(dirmerra,"/lonlat.feather",sep=""))
date.start <- as.POSIXct("1999-01-01",tz="UTC")
rad <- pi/180

# determining factor whether wind speed correction is carried out:
# limit for max distance to INMET station: 40 km
# calculate wind power generation with wind speed correction

# hourly and monthly
statpowlist <- calcstatpower_windcor(INmaxdist=40,corrlimit=0.5,method=1,mhm="hm",applylim=0)
save(statpowlist,file=paste0(dirresults,"/statpowlist_wschm_WPallc.RData")) # wschm = wind speed correction hourly and monthly

# monthly
statpowlist <- calcstatpower_windcor(INmaxdist=40,corrlimit=0.5,method=1,mhm="m",applylim=0)
save(statpowlist,file=paste0(dirresults,"/statpowlist_wscm_WPallc.RData")) # wscm = wind speed correction monthly




##### prepare results for wind parks #####
load(paste(dirresults,"/statpowlist_wschm_WPallc.RData",sep=""))
statpowlist_wschm_ac <- statpowlist
load(paste(dirresults,"/statpowlist_wscm_WPallc.RData",sep=""))
statpowlist_wscm_ac <- statpowlist
rm(statpowlist)

# aggregate per wind parks
WPS_wscm_ac <- makeWPpowlist(statpowlist_wscm_ac)
WPS_wschm_ac <- makeWPpowlist(statpowlist_wschm_ac)

# aggregate daily
WPS_wscmd_ac <- dailyaggregate(WPS_wscm_ac)
names(WPS_wscmd_ac) <- names(WPS_wscm_ac)
WPS_wschmd_ac <- dailyaggregate(WPS_wschm_ac)
names(WPS_wschmd_ac) <- names(WPS_wschm_ac)


# cut to same lengths
WPS_wscmdc_ac <- list()
WPS_wschmdc_ac <- list()
for(wps in names(WPS_wsmad)){
  WPS_wscmdc_ac[[wps]] <- csl(prodWPS[[wps]],WPS_wscmd_ac[[wps]])
  WPS_wschmdc_ac[[wps]] <- csl(prodWPS[[wps]],WPS_wschmd_ac[[wps]])
}

# load capacity correction factors
load(paste(dircaps,"/cap_cfs.RData",sep=""))
# apply capacity correction factors and kWh -> GWh
# of stations last two are in S, others in NE
for(i in c(1:(length(WPS_wsmadc)-2))){
  WPS_wscmdc_ac[[i]][,3] <- WPS_wscmdc_ac[[i]][,3]*cfNE/10^6
  WPS_wschmdc_ac[[i]][,3] <- WPS_wschmdc_ac[[i]][,3]*cfNE/10^6
}
for(i in c((length(WPS_wsmadc)-1):length(WPS_wsmadc))){
  WPS_wscmdc_ac[[i]][,3] <- WPS_wscmdc_ac[[i]][,3]*cfS/10^6
  WPS_wschmdc_ac[[i]][,3] <- WPS_wschmdc_ac[[i]][,3]*cfS/10^6
}
# name columns
for(i in c(1:7)){
  names(WPS_wscmdc_ac[[i]]) <- c("time","obs","sim")
  names(WPS_wschmdc_ac[[i]]) <- c("time","obs","sim")
}


# put all in one list
comp_wscmd_ac <- comp_wscmd
comp_wschmd_ac <- comp_wschmd
for(i in c(1:7)){
  comp_wscmd_ac[[names(WPS_wscmdc_ac)[i]]] <- WPS_wscmdc_ac[[i]]
  comp_wschmd_ac[[names(WPS_wschmdc_ac)[i]]] <- WPS_wschmdc_ac[[i]]
}

save(comp_wsmad,comp_wscmd_ac,comp_wschmd_ac,file=paste0(dirresults,"/comp_wsc_WPallc.RData"))




##### analyse results for wind parks #####
types <- data.frame(region=names(comp_wsmad),type=c("Brazil+Subs",rep("Brazil+Subs",2),rep(c("State","Windpark"),7)))
stats_wscm_ac <- list()
stats_wschm_ac <- list()
for(i in c(1:length(comp_wsmad))){
  
  stat <- data.frame(region = names(comp_wscmd_ac)[i],
                     area = types$type[which(names(comp_wscmd_ac)[i]==types$region)],
                     cor = cor(comp_wscmd_ac[[i]]$sim,comp_wscmd_ac[[i]]$obs),
                     RMSE = rmse(comp_wscmd_ac[[i]]$sim,comp_wscmd_ac[[i]]$obs),
                     MBE = mean(comp_wscmd_ac[[i]]$sim-comp_wscmd_ac[[i]]$obs),
                     mean = mean(comp_wscmd_ac[[i]]$sim),
                     obs = mean(comp_wscmd_ac[[i]]$obs))
  stats_wscm_ac <- rbind(stats_wscm_ac,stat)
  stat <- data.frame(region = names(comp_wschmd_ac)[i],
                     area = types$type[which(names(comp_wschmd_ac)[i]==types$region)],
                     cor = cor(comp_wschmd_ac[[i]]$sim,comp_wschmd_ac[[i]]$obs),
                     RMSE = rmse(comp_wschmd_ac[[i]]$sim,comp_wschmd_ac[[i]]$obs),
                     MBE = mean(comp_wschmd_ac[[i]]$sim-comp_wschmd_ac[[i]]$obs),
                     mean = mean(comp_wschmd_ac[[i]]$sim),
                     obs = mean(comp_wschmd_ac[[i]]$obs))
  stats_wschm_ac <- rbind(stats_wschm_ac,stat)
}

stats_ac <- rbind(data.frame(stats_wsma,int="wsma"),data.frame(stats_wscm_ac,int="wsc_m"),data.frame(stats_wschm_ac,int="wsc_hm"))

# prepare stats for table
stats_tidy_ac <- gather(stats_ac,key="type",value="val",-region,-area,-int)

save(stats_ac,stats_tidy_ac,file=paste0(dirresults,"/stats_wsc_WPallc.RData"))

par <- rep(c("cor","RMSE","MBE","mean"),each=3)
sim <- c("wsma_","wsc_m_","wsc_hm_")
vars <- c("region", paste0(sim,par),"wsma_obs")
stats_table_ac <- stats_tidy_ac %>% 
  mutate(val=round(val,3)) %>%
  arrange(area) %>%
  mutate(type=paste0(int,"_",type)) %>%
  dplyr::select(-int) %>%
  spread(key=type,value=val) %>%
  arrange(area) %>%
  dplyr::select(vars) %>%
  rename(obs = wsma_obs)

write.table(stats_table_ac,file=paste(dirresults,"/stats_wsc_WPallc.csv",sep=""),sep=";")

##### relative results wind parks #####

load(paste0(dirbase,"/table_mean_caps_validation.RData"))
tab$region <- as.vector(tab$region)
# adapt names of regions
names <- data.frame(old = as.vector(c("Northeast", "Macaubas",   "Praia Formosa",  "Sao Clemente",  "Araripe",   "Alegria II",  "Elebras Cidreira 1", "Bom Jardim",  "Rio Grande do Norte","Rio Grande do Sul","Santa Catarina")),
                    new = as.vector(c("North-East","BA-Macaubas","CE-PraiaFormosa","PE-SaoClemente","PI-Araripe","RN-AlegriaII","RS-ElebrasCidreira1","SC-BomJardim","RioGrandedoNorte",   "RioGrandedoSul",   "SantaCatarina")),stringsAsFactors = FALSE)
tab$region[match(names$old,tab$region)] <- names$new

stats_r_ac <- subset(stats_ac,select=-c(mean,obs))
stats_r_ac$RMSE <- stats_ac$RMSE/(tab$mean_cap_MW[match(stats_ac$region,tab$region)]/1000*24) #/1000 MW->GW, *24 24 hours of day
stats_r_ac$MBE <- stats_ac$MBE/(tab$mean_cap_MW[match(stats_ac$region,tab$region)]/1000*24) #/1000 MW->GW, *24 24 hours of day



##### plot differences for wind parks #####
# Wind power plants
dat <- NULL
for(i in names(WPS_wsmadc)){
  # merge all data to a tibble
  dat1 <- melt(data.frame(wsma=comp_wsmad[[i]][,3]-comp_wsmad[[i]][,2],wsc_m=comp_wscmd_ac[[i]][,3]-comp_wsmad[[i]][,2],wsc_hm=comp_wschmd_ac[[i]][,3]-comp_wsmad[[i]][,2]))
  dat1 <- data.frame(state=i,dat1)
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
  ggsave(paste(dirims,"/diff_stat_wsc_WPallc.png",sep=""), width = 9, height = 4.125)


##### plot statistical analysis #####
# width and height of images
w = 3.375
h = 5
# rows in legend
rw = 4
# find out which areas have been corrected with INMET
change_ac <-  c(rep(FALSE,17),rep(filter(stats_r_ac,int=="wsma")$RMSE,2)==filter(stats_r_ac,int %in% c("wsc_m","wsc_hm"))$RMSE)

# set special settings for those
stats_r_ac$size <- 3
stats_r_ac$size[change_ac] <- 3.5
stats_r_ac$shape <- 19
stats_r_ac$shape[change_ac] <- 15


# correlations, RMSE and MBE
stats_rl_ac <- split(stats_r_ac,stats_r_ac$area)
ys <- list(element_text(),element_blank(),element_blank())
col <- list(c("#c72321", "#0d8085", "#efc220"),c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c"),c("#c72321","#861719","#f0c320","#af8f19","#6e9b9e","#0d8085","#19484c"))
mar <- c(0,-0.3,-0.3)

### correlations
plist <- list()
yl <- c("Daily correlations","","")
for(i in 1:3){
  plot <- 
    ggplot(data=stats_rl_ac[[i]], aes(x=int,y=cor,group=int,color=region)) +
    coord_cartesian(ylim=c(-0.6,1)) +
    scale_colour_manual(values=col[[i]]) +
    theme(legend.position = "bottom", axis.text.y = ys[[i]], plot.margin=margin(l=mar[i],unit="cm")) +
    guides(color=guide_legend(nrow=rw,title="")) +
    xlab("Wind speed correction method") + 
    ylab(yl[i])
  if(i>1){
    plist[[i]] <- plot + 
      geom_boxplot() + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl_ac[[i]]$size,shape=stats_rl_ac[[i]]$shape)
  }else{
    plist[[i]] <- plot + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl_ac[[i]]$size,shape=stats_rl_ac[[i]]$shape)
  }
}
cowplot::plot_grid(plotlist = plist, nrow=1, align="h")
ggsave(paste0(dirresults,"/pointplots/cor_wsc_WPallc.png"), width = w*2.9, height = h)


### RMSES
plist <- list()
yl <- c("Daily relative RMSE","","")
for(i in 1:3){
  plot <- 
    ggplot(data=stats_rl_ac[[i]], aes(x=int,y=RMSE,group=int,color=region)) +
    coord_cartesian(ylim=c(0.1,0.45)) +
    scale_colour_manual(values=col[[i]]) +
    theme(legend.position = "bottom", axis.text.y = ys[[i]], plot.margin=margin(l=mar[i],unit="cm")) +
    guides(color=guide_legend(nrow=rw,title="")) +
    xlab("Wind speed correction method") + 
    ylab(yl[i])
  if(i>1){
    plist[[i]] <- plot + 
      geom_boxplot() + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl_ac[[i]]$size,shape=stats_rl_ac[[i]]$shape)
  }else{
    plist[[i]] <- plot + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl_ac[[i]]$size,shape=stats_rl_ac[[i]]$shape)
  }
}
cowplot::plot_grid(plotlist = plist, nrow=1, align="h")
ggsave(paste0(dirresults,"/pointplots/RMSE_wsc_WPallc.png"), width = w*2.9, height = h)


### MBEs
plist <- list()
yl <- c("Daily relative MBE","","")
for(i in 1:3){
  plot <- 
    ggplot(data=stats_rl_ac[[i]], aes(x=int,y=MBE,group=int,color=region)) +
    coord_cartesian(ylim=c(-0.375,0.6)) +
    scale_colour_manual(values=col[[i]]) +
    theme(legend.position = "bottom", axis.text.y = ys[[i]], plot.margin=margin(l=mar[i],unit="cm")) +
    guides(color=guide_legend(nrow=rw,title="")) +
    xlab("Wind speed correction method") + 
    ylab(yl[i])
  if(i>1){
    plist[[i]] <- plot + 
      geom_boxplot() + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl_ac[[i]]$size,shape=stats_rl_ac[[i]]$shape)
  }else{
    plist[[i]] <- plot + 
      geom_jitter(width=0.2,height=0,lwd=stats_rl_ac[[i]]$size,shape=stats_rl_ac[[i]]$shape)
  }
}
cowplot::plot_grid(plotlist = plist, nrow=1, align="h")
ggsave(paste0(dirresults,"/pointplots/MBE_wsc_WPallc.png"), width = w*2.9, height = h)











