# Condition Occurrence
library(dplyr)
library(tidyverse)
library(readr)
library(reshape2)
library(data.table)

# Select rows for diagnosis codes (ICD10, ICD9) and diagnosis dates
rows<-0:212
rows2<-1:46
dx_cols<-paste0('41270-0.', rows)
dx_cols2<-paste0('41271-0.', rows2)

date_cols<-paste0('41280-0.', rows)
date_cols2<-paste0('41281-0.', rows2)

#Load files separately
dx<-fread('ukb43407.csv', select=c('eid',dx_cols, dx_cols2))
dx_dates<- fread('ukb43407.csv', select=c('eid', date_cols, date_cols2))

# Reshape into long format
dx2 <- melt(dx, id.vars="eid", value.name = 'condition_source_value')
dx_dates2 <- melt(dx_dates, id.vars="eid", value.name='condition_start_date')

#Merge diagnoses and dates
co<-cbind(dx2,dx_dates2)
co2<-co%>%select(,c(1,3,6))
co3<-co2 %>% filter(condition_source_value!='')
colnames(co3)[1]<-'person_id'

#Add concept id
concept <- readRDS("concept.rds")
concept_dx<-concept %>% filter(vocabulary_id == 'ICD10')
concept_dx$concept_code2<- sub("\\.", "", concept_dx$concept_code)
co_concept<-left_join(co3, concept_dx, by = c('condition_source_value' = 'concept_code2'))
co_concept2<-co_concept%>%select(,c(1:4))
colnames(co_concept2)[4]<-'condition_concept_id'

# Export
co_concept2%>% write_csv('ukbiobank_condition_occurrence.csv')
