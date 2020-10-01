# PCORNET to OMOP COnversion

#OMOP concept RDs
library(readr)
library(dplyr)
library(tidyverse)
library(lubridate)
concept<-read_rds('concept.rds')

#Person----------------------------------------------
person<- read.csv(file='Demograpics.csv', header=TRUE, sep=",")

person$year_of_birth<-year(person$BIRTH_DATE)
#gender concept id
person2<- left_join(person,concept %>% filter(domain_id == 'Gender'), by=c("SEX"="concept_code") )
#ethnicity concept id
person2$ethnicity_concept_id<-0
person2<- within(person2, ethnicity_concept_id[HISPANIC == 'Y'] <- '38003563')
person2<- within(person2, ethnicity_concept_id[HISPANIC == 'N'] <- '38003564')
person3<-data.frame(person2$PATID, person2$SEX, person2$RACE,person2$HISPANIC, person2$year_of_birth, person2$concept_id, person2$ethnicity_concept_id)

#OMOP rename
names(person3) <- c("person_id", "gender_source_value","race_source_value",'ethnicity_source_value', "year_of_birth", 'gender_concept_id', 'ethnicity_concept_id' )

#Export person table
saveRDS(person3, file = "person.rds")
person3 %>% write_csv('person.csv')






#condition Occurrence-------------------------------------

#read in file
dx<- read.csv(file='Diagnosis.csv', header=TRUE, sep=",")

#filter concepts for just ICD
concept_dx<- concept %>% filter(vocabulary_id == 'ICD10CM'| vocabulary_id == 'ICD9CM' )

#remove . to make mapping effective
concept_dx$concept_code2= as.character(gsub("\\.", "", concept_dx$concept_code))
dx$DX2 = as.character(gsub("\\.", "", dx$DX))

#Map source value to OMOP concept
dx2 = left_join(x=dx, y=concept_dx, by=c("DX2"="concept_code2"))

#Create OMOP version
condition<-data.frame(dx2$PATID, dx2$concept_id, dx2$ADMIT_DATE,dx2$ENCOUNTERID, dx2$DX)
names(condition) <- c("person_id", "condition_concept_id","condition_start_date","visit_occurrence_id", "condition_source_value" )
condition$condition_start_date<-as.Date(condition$condition_start_date)
condition$condition_end_date<-as.Date(condition$condition_start_date)

condition$condition_occurrence_id <- seq.int(nrow(condition))

#Export
saveRDS(condition, file = "condition_occurrence.rds")
condition %>% write_csv('condition_occurrence.csv')




#Measurement------------------------------------------
#Read in file
measurement<-read_csv('Labs.csv')

#Filter to just LOINC concepts
sconcept<-concept %>% filter(vocabulary_id=='LOINC') 


#COnvert to OMOP names
measurement <- rename(measurement, person_id=PATID)
measurement <- rename(measurement,measurement_source_value=LAB_LOINC)
measurement <- rename(measurement,measurement_date=RESULT_DATE) #problem here with order date
measurement <- rename(measurement,value_as_number=RESULT_NUM) 
measurement <- rename(measurement,visit_occurrence_id=ENCOUNTERID) 
measurement <- rename(measurement,unit_source_value=RESULT_UNIT) 
measurement <- rename(measurement,value_source_value=RAW_RESULT) 



# Join to vocab and map
measurement2<- left_join(measurement, select(sconcept,concept_name,concept_code,concept_id),by=c('measurement_source_value'='concept_code'))
measurement2 <- rename(measurement2,measurement_concept_id=concept_id) 
measurement2$measurement_date<- lubridate::as_date(measurement2$measurement_date)
measurement3<-select(measurement2, person_id,measurement_concept_id, measurement_date,value_as_number, unit_source_value, visit_occurrence_id,measurement_source_value, value_source_value)

measurement3$measurement_id <- seq.int(nrow(measurement3))

#Export

saveRDS(measurement3, file = "measurement.rds")
measurement3 %>% write_csv('measurement.csv')



#Visit_Occurrence------------------------------------

visit<-read_csv('Encounter.csv')


#Set to OMOP
vo<-visit %>% select(
  visit_occurrence_id=ENCOUNTERID
  ,person_id=PATID,visit_start_date=ADMIT_DATE
  ,visit_end_date=DISCHARGE_DATE
  ,provider_id=PROVIDERID
  ,visit_source_value=ENC_TYPE
  ,adminitting_source_value=ADMITTING_SOURCE
  ,discharge_to_source_value=DISCHARGE_DISPOSITION
)
vo$visit_start_date %<>%  lubridate::as_date()
vo$visit_end_date %<>%  lubridate::as_date()

#vitit types map

vo$visit_type_concept_id<-0
vo <- within(vo, visit_type_concept_id[visit_source_value == 'ED'] <- '9203')
vo <- within(vo, visit_type_concept_id[visit_source_value == 'AV' |visit_source_value == 'OA' ] <- '9202')
vo <- within(vo, visit_type_concept_id[visit_source_value == 'IP'] <- '9201')
vo <- within(vo, visit_type_concept_id[visit_source_value == 'EI'] <- '262')


#write csv and rds
saveRDS(vo, file = "visit_occurrence.rds")
vo %>% write_csv('visit_occurrence.csv')


#Observation_period------------------------------------
#Beginning of observation from start of first visit
earliest_pt<-vo %>%
  group_by(person_id) %>%
  arrange(visit_start_date) %>%
  slice(1L)

#End of observation from end of latest visit
latest_pt<-vo %>%
  group_by(person_id) %>%
  arrange(desc(visit_end_date)) %>%
  slice(1L)

#Combining of earliest and latest
observation_period<-merge(earliest_pt, latest_pt, by ='person_id')
observation_period2<- data_frame(observation_period$person_id, observation_period$visit_start_date.x, observation_period$visit_end_date.y)
colnames(observation_period2) <- c('person_id', 'observation_period_start_date', 'observation_period_end_date')

#Export
saveRDS(observation_period2, file = "observation_period.rds")
observation_period2 %>% write_csv('observation_period.csv')


#Drug_exposure----------------------------------------

drug<-read_csv('Prescribing.csv')


#COnvert to OMOP names
drug<-drug %>% rename(person_id=PATID)
drug<-drug %>%  rename(quantity=RX_QUANTITY)
drug<-drug %>%  rename(route_source_value=RX_ROUTE) 
drug<-drug %>% rename(drug_exposure_start_date=RX_START_DATE) 
drug<-drug %>% rename(drug_source_value=RXNORM_CUI) 
drug<-drug %>%  rename(  visit_occurrence_id=ENCOUNTERID)



# Join to vocab and map
dconcept<-concept %>% filter(vocabulary_id=='RxNorm') 
drug$drug_source_value<-as.character(drug$drug_source_value)
drug<-drug %>% left_join(select(dconcept,concept_name,concept_code,concept_id),by=c('drug_source_value'='concept_code'))

drug$drug_exposure_start_date<- drug$drug_exposure_start_date %>% lubridate::as_date()

drug$drug_exposure_end_date<-drug$RX_END_DATE %>% as_date()

drug$drug_exposure_end_date<-as.Date(drug$RX_END_DATE , "%d-%b-%Y")

drug$drug_exposure_id <- seq.int(nrow(drug))
drug_exposure<-drug%>% select(drug_exposure_id, person_id, visit_occurrence_id, drug_exposure_start_date, drug_exposure_end_date, quantity, route_source_value, drug_source_value, concept_name, concept_id)
colnames(drug_exposure)[10]<-'drug_concept_id'

#Export files
saveRDS(drug_exposure, file = "iu_drug_exposure.rds")
drug_exposure %>% write_csv('iu_drug_exposure.csv')


#Database Write------------------------------------------

#connect
source('~/secret/conn.R')
library(DatabaseConnector)
schema='gpc'
connectionDetails<-createConnectionDetails(dbms='postgresql',user=user,password=pw,server=server
                                           ,schema = schema)

connection <-connect(connectionDetails)

#table selection
tbl='visit_occurrence'
tbl='condition_occurrence'
tbl='measurement'
tbl='drug_exposure'
t<-read_csv(glue::glue('{tbl}.csv'))
t$person_id %<>% as.numeric()


#Drop existing table and overwrite with new version
executeSql(connection,glue::glue('drop table {tbl}') %>% as.character())
(res<-dbWriteTable(connection,tbl,t)) 


#Disconnect

dbDisconnect(connection)

