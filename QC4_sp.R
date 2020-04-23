library(dplyr)
library(data.table)
library(purrr)
library(rlist)
library(tidyverse)
library(sparklyr)

#files<-list.files(path = "evaluation", pattern = NULL, all.files = FALSE,
#                  full.names = FALSE, recursive = FALSE,
#                  ignore.case = FALSE, include.dirs = FALSE,  no.. = FALSE)

#files<-list.files(path = "nf", pattern = NULL, all.files = FALSE,
#                  full.names = FALSE, recursive = FALSE,
#                  ignore.case = FALSE, include.dirs = FALSE,  no.. = FALSE)


#for(filename in files){           
filename<-paste("nf/", filename, sep="") 
filename<-"evaluation/mushroom.dat"
#outputname<-paste("out/", filename, "_output_sp.txt", sep="")
  outputname<-paste(filename, "_output_sp.txt", sep="")
  print(outputname)
  #filename<-paste("evaluation/", filename, sep="") 

  print(filename)
  
  

# conf <- spark_config()
# conf$spark.executor.memory <- "512M"
# conf$spark.executor.cores <- 2
# conf$spark.executor.instances <- 3
# conf$spark.dynamicAllocation.enabled <- "false"
# 
# sc <- spark_connect(master = "local",
#                     spark_home = "spark/spark-2.4.3-bin-hadoop2.7/",
#                     version = "2.4.3",
#                     config = conf)


begin<-Sys.time()
readtime<-Sys.time()


ncol <- max(count.fields(filename, sep = " "))
tr<-read.table(filename, header=F, sep=' ', fill=T, col.names=paste0('V', seq_len(ncol)))
#tr<- do.call("rbind", lapply(dir("BasesBinaires", full.names = TRUE   ), read.table, as.is = TRUE, fill = TRUE, row.names=NULL))
#tr<-  read.table(filename, header=F,stringsAsFactors=FALSE, )


#gc()
#get last column
#get highest value in last column
#_______________________________________________________________-----------------
#ncol-1 bc last column was na
maxcol<-max(tr[, ncol-1], na.rm = TRUE)
maxrow<-nrow(tr)
#maxcol<-max(tr, na.rm = TRUE)

#_______________________________________________________________-----------------------------


cat( " " ,file=outputname,append=TRUE,sep="\n")
cat( "------" ,file=outputname,append=TRUE,sep="\n")
cat( "spark ver" ,file=outputname,append=TRUE,sep="\n")
cat(format(Sys.Date(), format="%B %d %Y") ,file=outputname,append=TRUE,sep="\n")
cat(filename ,file=outputname,append=TRUE,sep="\n")

print("Hello")
print(format(Sys.Date(), format="%B %d %Y"))

ds<-matrix(FALSE, nrow = maxrow, ncol = maxcol)
covered<-matrix(FALSE, nrow = maxrow, ncol = maxcol)
for(y in 1:maxrow) {
  #suppy[y,2]<-sum(!is.na(tr[y,]))
  for(x in tr[y,]) {
    ds[y,x]=TRUE
  }}

print("matrix done")
trues<-which(ds==TRUE, arr.ind = TRUE)
colnames<-unique( trues[,2])
rownames<- unique( trues[,1])
truesdf<-as.data.frame(trues)

#truesdf <- copy_to(sc, truesdf, "truesdf", memory = FALSE, overwrite = TRUE, repartition = 4)
# truesdf <- copy_to(sc, truesdf,  memory = TRUE, overwrite = TRUE, repartition = 4)
# print("trues done")
# colnames<-as.data.frame(unique( trues[,2]))
# colnames(colnames) <- c("x")
# 
# rownames<- as.data.frame(unique( trues[,1]))
# colnames(rownames) <- c("y")

#extent and intent formula
getextentsingle<- function(x){
  list(x,c(as.vector(unlist( truesdf %>% dplyr::filter(col==x) %>% dplyr::distinct(row)))))   }


getintentsingle<- function(y) {
  list(y,c(as.vector(unlist(truesdf %>% dplyr::filter(row==y) %>% dplyr::distinct(col)))) )      }


y_table <- data.frame(t(data.frame(list(mapply(getintentsingle, rownames))))) # õige

x_table <- data.frame(t(data.frame(list(mapply(getextentsingle, colnames))))) # õige

print(" x y tabledone local")
# colnameslist <- copy_to(sc, colnames, memory = FALSE, overwrite = TRUE, repartition = 4)
# rownameslist <- copy_to(sc, rownames, memory = FALSE, overwrite = TRUE, repartition = 4)
# x_table<- copy_to(sc, x_table,  memory = TRUE, overwrite = TRUE, repartition = 4)
# y_table<- copy_to(sc, y_table,  memory = FALSE, overwrite = TRUE, repartition = 4)


print(" x y tabledone")

#get intent of intent function:
#get X, get extent of x (list of y)
f1<- function(x, extent){  unique(c(unlist(mapply(f2, x, extent))))    }

#get intent (list of x.1) of each member of exent (list of y)
f2<- function(x, y){  mapply(f3, x,y, (y_table %>% filter(X1==y)%>% select(X2))$X2[[1]])   }

#get extent( list of y.1) for each member of (x.1 list)
f3<- function(x,y,x1){
  #if extent  (list of y.1)  includes all the items of original extent (list of y) , add the item (x.1) to final outcome  as intent of x
  if(x==x1){    x  }
  else{
    if(length(na.omit(match((x_table %>% filter(X1==x)%>% select(X2))$X2[[1]], (x_table %>% filter(X1==x1)%>% select(X2))$X2[[1]]))) == length((x_table %>% filter(X1==x)%>% select(X2))$X2[[1]])){
      x1
    }     }   }


#size
qc<-Sys.time()
getsize<- function(x, y){
  length((x_table %>% filter(X1==x)%>% select(X2))$X2[[1]])*length((y_table %>% filter(X1==y)%>% select(X2))$X2[[1]])    }


truesdf<-mutate(truesdf, size=mapply(getsize, truesdf$col, truesdf$row), covered=FALSE)
print("size to local done")
truesdf<-arrange(truesdf, size)
truesdf<-mutate(truesdf,nr = row_number())
nrowt<-nrow(truesdf)
#truesdf2 <- copy_to(sc, truesdf2,  memory = FALSE, overwrite = TRUE, repartition = 4)
 # truesdf <- copy_to(sc, truesdf,  memory = TRUE, overwrite = TRUE, repartition = 4)
 # truesdf <- copy_to(sc, truesdf)
 # print("trues done")


print("size done")
print("sizetime")
print(Sys.time()-qc)
sizetime <- difftime(Sys.time(),qc,units="secs")
cat("sizetime" ,file=outputname,append=TRUE,sep="\n")
cat(sizetime ,file=outputname,append=TRUE,sep="\n")

#ds

# qcover4<- function(tbl, nrowt){
#   num<-0
#   for(i in 1:nrowt){
#     if(!tbl$covered[i]){
#       num<-num+1
#       #calculate intent here!
#       extent1<-((x_table %>% filter(X1==tbl$col[i] )%>% select(X2))$X2[[1]])
#       tbl <- tbl%>%
#         mutate(covered=replace(covered, (col %in% unlist(unique(mapply(f1, tbl$col[i], extent1   ))) ) & (row %in% (x_table %>% filter(X1==tbl$col[i] )%>% select(X2))$X2[[1]] )       , TRUE))
#     }}
#   num }



qcover4<- function(tbl, nrowt){
 # print(tbl)
#  print(nrowt)
  num<-0
  for(i in 1:nrowt){
    print(i)
   if(!(tbl %>% dplyr::filter(nr==i))$covered){
        tablerow<-(truesdf %>% dplyr::filter(nr==i))
       num<-num+1
    #   #calculate intent here!
       extent1<-((x_table %>% filter(X1== tablerow$col   )%>% select(X2))$X2[[1]])
       tbl <- tbl%>%
         mutate(covered=replace(covered, (col %in% unlist(unique(mapply(f1, tablerow$col  , extent1   ))) ) & (row %in% (x_table %>% filter(X1==tablerow$col )%>% select(X2))$X2[[1]] )       , TRUE))
     }
  }
  print(num)
  num }

# (truesdf %>% dplyr::filter(nr==i))$covered       

qc<-Sys.time()
print("num of FC")
num<-qcover4(truesdf, nrowt)  
print("algorithm runtime")
print(Sys.time()-qc)
tot <- difftime(Sys.time(),qc,units="secs")
cat("algorithm runtime" ,file=outputname,append=TRUE,sep="\n")
cat(tot,file=outputname,append=TRUE,sep="\n")
cat("num of concepts" ,file=outputname,append=TRUE,sep="\n")
cat(num,file=outputname,append=TRUE,sep="\n")

print("total time")
print(Sys.time()-begin)


#spark_disconnect(sc)
#}
