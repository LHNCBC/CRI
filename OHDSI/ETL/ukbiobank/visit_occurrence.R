library(dplyr)
library(tidyverse)
library(reshape2)
library(data.table)


# -----Visit Occurrence
file <-'ukb43407.csv'
visit<- fread(file, select=c('eid',  '53-0.0',  '53-1.0',  '53-2.0',  '53-3.0'))


#Combine all visits into single column
vo <- melt(visit, id.vars="eid")
vo2<-vo %>% filter(!is.na(value))

#Create visit occurrence id as (person_id-visit indicator)
vo2$variable<-paste(vo2$eid,'-',vo2$variable)
colnames(vo2)<-c('person_id','visit_occurrence_id', 'visit_start_date')
#visit end date equals visit start date
vo2$visit_end_date<-vo2$visit_start_date

vo2 %>% write_csv('visit_occurrence.csv')
