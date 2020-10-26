# Procedure Occurrence
library(dplyr)
library(tidyverse)
library(readr)
library(reshape2)
library(data.table)

# Select rows for procedure codes and procedure dates


rows<-0:116
rows2<-0:15

px_cols<-paste0('41272-0.', rows)
px2_cols<-paste0('41273-0.', rows2)

date_cols<-paste0('41282-0.', rows)
date2_cols<-paste0('41283-0.', rows2)

#Read in neccessary data
px<-fread('ukb43407.csv', select=c('eid',px_cols,px2_cols))
px_dates<- fread('ukb43407.csv', select=c('eid', date_cols, date2_cols))

# Reshape into long format
px2 <- melt(px, id.vars="eid", value.name = 'procedure_source_value')
px_dates2 <- melt(px_dates, id.vars="eid", value.name='procedure_date')

#Merge procedure and dates
po<-cbind(px2,px_dates2)
po2<-po%>%select(,c(1,3,6))
po3<-po2 %>% filter(procedure_source_value!='')
colnames(po3)[1]<-'person_id'

#Export
po3%>% write_csv('ukbiobank_procedure_occurrence.csv')
