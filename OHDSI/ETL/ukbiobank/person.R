library(dplyr)
library(tidyverse)
library(data.table)


# -----Person table
file<-'ukb43407.csv'
#this file is huge, it is 36GB !!!!!
#the fread approach works the best
person<-fread(file, select=c('eid','31-0.0', '34-0.0', '52-0.0','40000-0.0' ))

colnames(person)<-c('person_id', 'gender_source_value', 'year_of_birth', 'month_of_birth',  'death_date_time')

#Set gender concept id
person$gendet_concept_id<-0
person$gendet_concept_id[which(person$gender_source_value == 0)] <-8532
person$gendet_concept_id[which(person$gender_source_value == 1)] <-8507
person%>% write_csv('person.csv')
