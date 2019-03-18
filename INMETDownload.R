
library(RCurl)
library(parallel)
library(XML)
library(tidyverse)

url<- "http://www.inmet.gov.br/projetos/grafico/ema_html_pg.php"

# set working directory to directory where inmet data are to be stored, meta data must be in above directory
setwd(dirinmetmeta)
stations<-read.table("./stations_meta_data.csv",sep=";",header=T,stringsAsFactors=F)
dir.create("../data",showWarnings = FALSE)

downloadYear<-function(y,stations,url,station){
  
  print(y)
  df<-data.frame(date.time=rep(0,8784*8),val=rep(0,8784*8),type=rep("",8784*8),stringsAsFactors =FALSE)
  names(df)<-c("date.time","val","type")
  
  result <- postForm(url, mRelEstacao=stations$cod[station],
                     mRelAno=y,
                     btnProcesso=" Gera ")
  
  
  if(substr(result[1],1,21)=="Registro Inexistente."){
    print(paste(stations$cod[station],y," not found"))
    return(NULL)  
  }
  
  result<-gsub("; \r","",result)
  result<-gsub("vlr = ","",result)
  result<-gsub(".push","",result)
  result<-gsub("dados_","",result)
  result<-gsub("var dt  =","",result)
  result<-gsub("\r","",result)
  result<-gsub("\\(\\[dt,vlr\\]\\);","",result)
  result<-gsub("var ","",result)
  result<-gsub(";","",result)
  result<-gsub(" ","",result)
  txtvec <- strsplit(result,'\n')[[1]]
  
  if(txtvec[22]=="<!---alert(temp)-->"){
    return(NULL)
  }
  
  is<-seq(22,length(txtvec),3)
  dates<-as.numeric(txtvec[is])/1000
  vals<-as.numeric(txtvec[is+1])
  types<-(txtvec[is+2])
  
  t<-tibble(dates,vals,types) 
  t<-t[!is.na(t[,1]),]
  return(t)
  #t %>% spread(types,vals) %>% return()
  
}


for(station in 1:nrow(stations)){
  if(!file.exists(paste("data/",stations$name[station],".csv",sep=""))){
    
    s<-seq(ISOdate(1999,1,1), ISOdate(2017,1,1), "hour")
    final<-data.frame(matrix(nrow=length(s),ncol=9))
    names(final)<-c("date.time","temp","umi","po","pres","rad","pre","vdd","vvel")
    
    final[,1]<-as.numeric(s)
    
    print(paste("Dealing with",stations$name[station]))
    
    #station<-470
    #debug(downloadYear)
    #downloadYear(2002,stations=stations,station=station,url=url)
    
    no_cores <- detectCores() - 1
    
    cl <- makeCluster(no_cores)
    clusterEvalQ(cl, library("RCurl"))
    clusterEvalQ(cl, library("tidyverse"))
    clusterEvalQ(cl, sink(paste0("c:/temp/output", Sys.getpid(), ".txt")))
    
    a<-parSapply(cl,1999:2016,downloadYear,stations=stations,station=station,url=url,simplify=FALSE)
    
    stopCluster(cl)
    
    
    marker<-c("temp","umi","po","pres","rad","pres","vdd","vvel")
    
    for(j in 1:length(a)){
      df<-a[[j]]
      if(is.null(df)){
        next
      }
      
      for(i in 1:8){
        
        df1<-df[df$types==marker[i],]
        
        if(nrow(df1)==0){
          next 
        }
     
        
        cc<-1:nrow(df1)
        cc1<-1:nrow(final)
        d1<-data.frame(df1[,1],cc)
        names(d1)<-c("date","cc")
        d2<-data.frame(final[,1],cc1)
        names(d2)<-c("date","cc1")
        # merger<-merge(d1,d2,by=c("date"))
        suppressWarnings(merger<-merge(d1,d2,by=c("date")))
        final[merger[,3],i+1]<-df1[merger[,2],2]
        }
      }
    
    
    
    
    
    write.table(final,paste("data/",stations$name[station],".csv",sep=""),sep=";")
  }else{
    print(paste("file exists already:",stations$name[station]))
  }
}



