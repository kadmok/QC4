library(dplyr)
library(data.table)
library(purrr)
library(rlist)
library(tidyverse)
library(sparklyr)

#loop over entire directory of files
#files<-list.files(path = "evaluation", pattern = NULL, all.files = FALSE,
#           full.names = FALSE, recursive = FALSE,
#           ignore.case = FALSE, include.dirs = FALSE,  no.. = FALSE)
          
#for(filename in files){           



filename<-paste("evaluation/", filename, sep="") 
print(filename)
begin<-Sys.time()
readtime<-Sys.time()

#run a single file
#filename<-'evaluation/zoo_bi.dat'

ncol <- max(count.fields(filename, sep = " "))
tr<-read.table(filename, header=F, sep=' ', fill=T, col.names=paste0('V', seq_len(ncol)))
tr[is.na(tr)] <- 0

#_______________________________________________________________-----------------
#ncol-1 bc last column is 'na' in Takeaki Uno benchmark datasets
#maxcol<-max(tr[, ncol-1], na.rm = TRUE)
maxcol<-max(tr)
maxrow<-nrow(tr)
#maxrow<-rows
#maxcol<-max(tr, na.rm = TRUE)
print(maxcol)
print(maxrow)
#_______________________________________________________________-----------------------------
outputname<-paste(filename, "_output.txt", sep="")

cat( " " ,file=outputname,append=TRUE,sep="\n")
cat( "------" ,file=outputname,append=TRUE,sep="\n")
cat( "fast ver" ,file=outputname,append=TRUE,sep="\n")
cat( " " ,file=outputname,append=TRUE,sep="\n")
cat("KADRI version " ,file=outputname,append=TRUE,sep="\n")  
cat("(size= len support x* len support y)" ,file=outputname,append=TRUE,sep="\n")  
cat("no mandatory concepts calc" ,file=outputname,append=TRUE,sep="\n")  
cat(format(Sys.Date(), format="%B %d %Y") ,file=outputname,append=TRUE,sep="\n")
cat(filename ,file=outputname,append=TRUE,sep="\n")



#ds<-matrix( rep( FALSE, len=maxcol*maxrow), nrow = maxrow)
#covered<-matrix( rep( FALSE, len=maxcol*maxrow), nrow = maxrow)

df1<-data.frame(matrix(FALSE, nrow = maxrow, ncol = maxcol))
ds<-as.matrix(df1)
covered<-as.matrix(df1)
#suppy<-data.frame(matrix((1:maxrow), nrow = maxrow, ncol = 2))

for(y in 1:maxrow) {
 # print(y)
  for(x in tr[y,]) {
   #print(x) 
      ds[y,x]=TRUE
  }}



QC1403<-function(ds, covered){ 
  qc<-Sys.time()
  
  numofc<-0
  #mandatory
  qc<-Sys.time()
  trues<-which(ds==TRUE, arr.ind = TRUE)
  trues<-cbind(trues, size=c(0))

  
  qc<-Sys.time()
  nrow<-nrow(trues)

  print(Sys.time()-qc)
  sizetime <- difftime(Sys.time(),qc,units="secs")
  
  qc<-Sys.time()
 # couplestable <- copy_to(sc, trues, overwrite = TRUE) 
          
 #Calculate Size value
  for (nr in 1:nrow){
    trues[nr,3]<-sum(ds[,trues[nr,2]])* sum(ds[trues[nr,1],])
  } 
  trues<-trues[order(trues[, 3]), ]

print("sizetime")
print(Sys.time()-qc)
sizetime <- difftime(Sys.time(),qc,units="secs")
cat("sizetime" ,file=outputname,append=TRUE,sep="\n")
cat(sizetime ,file=outputname,append=TRUE,sep="\n")


  # non mandatory
  qc<-Sys.time()
  for (i in 1:nrow(trues)){
    #if not covered
    if(!covered[trues[i, 1], trues[i, 2]]){
      numofc<-numofc+1
      
      
      
      extents<-trues[trues[,2]==trues[i, 2],1]
      intents<-c()
      for(xn in 1:length(ds[1,])){
        if(sum(ds[ds[,trues[i, 2]] == TRUE,xn])==sum(ds[,trues[i, 2]]))
          intents<-c(intents, xn)        }
      
      # cat("\n")
      # print("INTS")
      # print(unique(intents))
      # print("EXTS")
      # print(extents)
      
    #mark as covered
      for(int in intents){
        for( e in extents){
          covered[e, int]<-TRUE
        }}
    }
    
  }

  print("num of concepts")
  print(Sys.time()-qc)
  print(numofc)
  concepts <- difftime(Sys.time(),qc,units="secs")
  cat("num of concepts" ,file=outputname,append=TRUE,sep="\n")
  cat(numofc ,file=outputname,append=TRUE,sep="\n")
  cat("concepts time" ,file=outputname,append=TRUE,sep="\n")
  cat(concepts ,file=outputname,append=TRUE,sep="\n")
}






print("preptime")
print(Sys.time()-readtime)
prep <- difftime(Sys.time(),readtime,units="secs")
cat("preptime" ,file=outputname,append=TRUE,sep="\n")
cat(prep ,file=outputname,append=TRUE,sep="\n")



qc<-Sys.time()
QC1403(ds, covered) 
print("algorithm runtime")
print(Sys.time()-qc)
print("total time")
print(Sys.time()-begin)
tot <- difftime(Sys.time(),qc,units="secs")
cat("algorithm runtime" ,file=outputname,append=TRUE,sep="\n")
cat(tot,file=outputname,append=TRUE,sep="\n")




#}

