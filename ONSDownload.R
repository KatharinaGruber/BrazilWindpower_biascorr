####This script downloads daily wind power production data for Brazil from the ONS daily buletins.
####Author: Johannes Schmidt
####Adaptations by Katharina Gruber

library("RCurl")
library(dplyr)
library(tibble)
library(ggplot2)
library(tidyr)
library(readr)
library(lubridate)
library(readxl)
library(rstudioapi)

#####set this to your working directory!
#####if command does not work (e.g. because you are not working with R-Studio)
#####set the working directory manually
setwd(dirname(rstudioapi::getActiveDocumentContext()$path))


#####Reads windpower generation from line of  HTML Stream
getVal<-function(x){
  val<-substr(x,59,64)
  val<-gsub("<","",val)
  val<-gsub("/","",val)
  val<-gsub("t","",val)
  val<-gsub("\\.","",val)
  val<-gsub(",",".",val)
  val<-as.numeric(val)
  return(val)
}

####THIS PART OF THE SCRIPT HAS ONLY TO BE RUN, IF THE FILE 
####is not available in your directory!
if(!file.exists("daily_wind_power_production_2015_07_01-2017_01_31.csv")){

  ####wind available starting on 2015-01-01
  ####format change in 2017-02-01
  ss<-seq(as.Date("2015-07-01"),as.Date("2017-01-31"),1)
  prod<-data.frame(matrix(nrow=length(ss),ncol=4))

  for(i in 1:length(ss)){
    i1<-gsub("-","_",as.character(ss[i]))
    #link<-paste("http://www.ons.org.br/resultados_operacao/boletim_diario/",i1,"/geracao_arquivos/sheet003.htm#RANGE!A1",sep="")
    link <- paste0("http://sdro.ons.org.br/boletim_diario/",i1,"/geracao_arquivos/sheet003.htm#RANGE!A1")
    print(link)
    out<-getURL(link)
    out.split<-strsplit(out,"\n")
    SE<-getVal(out.split[[1]][[408]])
    S<-getVal(out.split[[1]][[415]]) 
    NE<-getVal(out.split[[1]][[422]])
    N<-getVal(out.split[[1]][[429]])
    prod[i,]<-c(SE,S,NE,N)
  }

  names(prod)<-c("SE","S","NE","N")
  prod_tibble<-cbind(date=ss,prod) %>% as_tibble() %>% gather(region,val,-date) %>% arrange(date)
  prod_tibble %>% ggplot(aes(x=date,y=val)) + geom_line(aes(col=region))
  write.csv(prod_tibble,"daily_wind_power_production_2015_07_01-2017_01_31.csv")

}

#####this is the script to keep your files updated
#####first the data up to 2017-01-31 is read from disk
#####then the timeseries is updated
prod_tibble<-read_delim("daily_wind_power_production_2015_07_01-2017_01_31.csv",delim=",") %>% 
  dplyr::select(date,region,val)

prod_tibble.updated<-prod_tibble

prod_per_windfarm<-tibble(date=as.Date("2017-02-01"),names="dummy",predicted=0,actual=0)

#####create output directory for excel files
dir.create(file.path(getwd(), "excel_files"), showWarnings = FALSE)

####get data until 2017-08-01
ss<-seq(as.Date("2017-02-01"),as.Date("2017-08-01"),1)

n<-"09_ProducaoEolicaUsina_"

for(j in 1:length(ss)){
  i<-ss[j]
  print(i)
  day<-strftime(i, format = "%d")
  month<-strftime(i,format = "%m")
  year<-strftime(i,format="%Y")

  outfile<-paste("excel_files/output",year,month,day,".xlsx",sep="")
  
  if(!file.exists(outfile)){
    #link<-paste("http://www.ons.org.br/resultados_operacao/SDRO/Diario/",
    link <- paste0("http://sdro.ons.org.br/SDRO/DIARIO/",
              year,"_",
              month,"_",
              day,
 #             "/Html/DIARIO_",
              "/HTML/",
              n,
              day,
              "-",
              month,
              "-",
              year,
              ".xlsx")
    out<-getBinaryURL(link)
    writeBin(out,outfile)
    print(paste("Downloaded..",outfile))
  }
  if(j==104){
    n<-"10_ProducaoEolicaUsina_"
  }
  #print(j)
  
  ex<-read_excel(outfile,sheet="Plan1")
  
  
  ####regional data
  
  t<-bind_cols(tibble(date=i,region=c("N","NE","S","SE")),as_tibble(data.matrix(ex[3:6,2])))
  names(t)<-c("date","region","val")
  
  prod_tibble.updated<-bind_rows(prod_tibble.updated,t)
  
  if(j<=104){
    t1<-bind_cols(tibble(date=i,names=unlist(ex[13:nrow(ex),1])),
            as_tibble(data.matrix(ex[13:nrow(ex),3:4])))
  }else{
    t1 <- bind_cols(tibble(date=i,names=unlist(ex[20:nrow(ex),1])),
            as_tibble(data.matrix(ex[20:nrow(ex),3:4])))
  }
  names(t1)<-c("date","names","predicted","actual")
  prod_per_windfarm<-bind_rows(prod_per_windfarm,
                               t1)
  
}


###get data from day 2017-08-02 on
ss<-seq(as.Date("2017-08-02"),as.Date(Sys.Date()-4),1)

#http://sdro.ons.org.br/SDRO/DIARIO/2017_08_29/HTML/10_ProducaoEolicaUsina_29-08-2017.xlsx

for(j in 1:length(ss)){
  i<-ss[j]
  print(i)
  day<-strftime(i, format = "%d")
  month<-strftime(i,format = "%m")
  year<-strftime(i,format="%Y")
  
  outfile<-paste("excel_files/output",year,month,day,".xlsx",sep="")
  
  if(!file.exists(outfile)){
    
    link<-paste("http://sdro.ons.org.br/SDRO/DIARIO/",
                year,"_",
                month,"_",
                day,
                "/Html/10_ProducaoEolicaUsina_",
                day,
                "-",
                month,
                "-",
                year,
                ".xlsx",sep="")
    out<-getBinaryURL(link)
    print(link)
    writeBin(out,outfile)
    print(paste("Downloaded..",outfile))
  }
  #if(j>104){
  #  n<-"10-Produção Eólica"
  #}
  #print(j)
  
  ex<-read_excel(outfile,sheet=1)
  
  
  ####regional data
  t<-bind_cols(tibble(date=i,region=c("N","NE","S","SE")),as_tibble(data.matrix(ex[3:6,2]))) 
  names(t)<-c("date","region","val")
  
  prod_tibble.updated<-bind_rows(prod_tibble.updated,t)
  
  t1<-bind_cols(tibble(date=i,names=unlist(ex[20:nrow(ex),1])),
                as_tibble(data.matrix(ex[20:nrow(ex),3:4])))
  names(t1)<-c("date","names","predicted","actual")
  prod_per_windfarm<-bind_rows(prod_per_windfarm,
                               t1)
  
}




theme_set(theme_classic(base_size = 5))

prod_tibble.updated %>% ggplot(aes(x=date,y=val)) + geom_line(aes(col=region),size=0.1) +
  xlab("Day") + ylab("Production (GWh)")
ggsave("windPowerProduction.png",width=2, height=1)

####predicted: predicted wind farm production (1 day ahead)
####actual: actual observed wind farm production
prod_per_windfarm %>% ggplot(aes(x=predicted,y=actual)) + geom_point(aes(col=names),size=0.1)+ theme(legend.position="none")+
  xlab("Actual Production (MWh)")+ylab("Predicted Production (MWh)")
ggsave("windPowerProduction_Forecast.png",width=2,height=1)

write.csv(prod_tibble.updated,
          paste("daily_wind_power_production_2015_07_01-",as.Date(Sys.Date()-4),".csv",sep=""))
write.csv(prod_per_windfarm,
          paste("daily_wind_power_production_per_wind_farm_2015_07_01-",as.Date(Sys.Date()-4),".csv",sep=""))
