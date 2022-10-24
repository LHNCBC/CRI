
install.packages(c("dbplyr", "RSQLite"))
library(dplyr)
library(dbplyr)
library(readr)
library(DBI)

library(tidyverse)





rel<-read_csv('stndard_map2.csv')%>%filter(vocabulary_id.x=='SNOMED')%>%select(,c(1:2))

rel<-read_tsv('CPRD/dmd/CONCEPT_RELATIONSHIP.csv')%>%filter(relationship_id=='Maps to') %>%select(,c(1:2))





med <- read_delim("//lhcdevfiler/OMOP/CPRD/202203_Lookups_CPRDAurum/202203_Lookups_CPRDAurum/202203_EMISMedicalDictionary.txt", 
                  delim = "\t", escape_double = FALSE, 
                  trim_ws = TRUE)

ObsType <- read_delim("//lhcdevfiler/OMOP/CPRD/202203_Lookups_CPRDAurum/202203_Lookups_CPRDAurum/ObsType.txt", 
                      delim = "\t", escape_double = FALSE, 
                      trim_ws = TRUE)
snomed<-concept%>%filter(vocabulary_id=='SNOMED')


#Table creation
for (i in 1:50){
  
  
  
  
  
  # Care site---------------------------------------------------------------------------------------------------------------------------------
  practice <- read_delim(paste0('//lhcdevfiler/OMOP/CPRD/Aurum_AllPatID_set', i,'_Extract/Practice/Aurum_AllPatID_set',i,'_Extract_Practice_001.txt'), 
                         delim = "\t", escape_double = FALSE, 
                         trim_ws = TRUE)
  practice2<-practice%>%select(,c('pracid', 'region'))
  colnames(practice2)<-c('care_site_id', 'location_id')
  
  
  practice2%>%write_csv(paste0('CPRD/mapped/care_site_',i,'.csv'))
  
  #person----------------------------------------------------------------------------------------------------------
  Aurum_AllPatID_set1_Extract_Patient_001 <- read_delim(paste0("//lhcdevfiler/OMOP/CPRD/Aurum_AllPatID_set",i,"_Extract/Patient/Aurum_AllPatID_set",i,"_Extract_Patient_001.txt"), 
                                                        delim = "\t", escape_double = FALSE, 
                                                        trim_ws = TRUE)
  
  
  person1<-Aurum_AllPatID_set1_Extract_Patient_001%>%select(,c('patid', 'yob', 'mob', 'pracid', 'usualgpstaffid','gender'))
  colnames(person1)<-c('person_id', 'year_of_birth', 'month_of_birth', 'care_site_id', 'provider_id', 'gender_source_value')
  
  
  person1$gender_concept_id<-0
  person1$gender_concept_id[which(person1$gender_source_value == 2)] <-8532
  person1$gender_concept_id[which(person1$gender_source_value == 1)] <-8507
  
  person1%>%write_csv(paste0('CPRD/mapped/person_',i,'.csv'))
  
  
  #------------------------------------------------------------------------------------------------------------
  #Observation source name
  
  obs_files <- list.files(path = paste0('//lhcdevfiler/OMOP/CPRD/Aurum_AllPatID_set',i,'_Extract/Observation/'), pattern = "\\.txt$", full.names = TRUE)
  
  for (f in 11:length(obs_files)){
    
    
    
    obs <- read_delim(obs_files[f], 
                      delim = "\t", escape_double = FALSE, 
                      trim_ws = TRUE)
    
    
    med <- read_delim("//lhcdevfiler/OMOP/CPRD/202203_Lookups_CPRDAurum/202203_Lookups_CPRDAurum/202203_EMISMedicalDictionary.txt", 
                      delim = "\t", escape_double = FALSE, 
                      trim_ws = TRUE)
    
    obs2<-left_join(obs,med, by =c('medcodeid'='MedCodeId'))
    
    ObsType <- read_delim("//lhcdevfiler/OMOP/CPRD/202203_Lookups_CPRDAurum/202203_Lookups_CPRDAurum/ObsType.txt", 
                          delim = "\t", escape_double = FALSE, 
                          trim_ws = TRUE)
    
    obs3<-left_join(obs2,ObsType, by =c('obstypeid'='obstypeid'))
    
    
    obs3$SnomedCTConceptId<-as.character(obs3$SnomedCTConceptId)
    obs4<-left_join(obs3,snomed, by =c('SnomedCTConceptId'='concept_code'))
    
    
    
    
    
    #Procedure---------------------------------------------------------------
    procedure<-obs4%>%filter(domain_id=='Procedure')
    procedure2<-procedure%>%select(,c('obsid','patid','concept_id', 'obsdate', 'staffid','medcodeid', 'parentobsid' ))
    colnames(procedure2)<-c('procedure_occurrence_id', 'person_id', 'procedure_source_concept_id', 'proedure_date', 'provider_id', 'procedure_source_value','visit_occurrence_id' )
    
    procedure2%>%write_csv(paste0('CPRD/mapped/procedure_',i,'_',f,'.csv'))
    
    
    #Measurement-----------------------------------------------------------------
    measurement<-obs4%>%filter(domain_id=='Measurement')
    measurement2<-measurement%>%select(,c('obsid','patid','concept_id', 'obsdate', 'staffid','medcodeid', 'value', 'numunitid','numrangelow', 'numrangehigh', 'parentobsid' ))
    colnames(measurement2)<-c('measurement_id', 'person_id', 'measurement_source_concept_id', 'measurement_date', 'provider_id', 'measurement_source_value', 'value_as_number', 'unit_source_value', 'range_low', 'range_high','visit_occurrence_id')
    
    measurement2%>%write_csv(paste0('CPRD/mapped/measurement_',i,'_',f,'.csv'))
    
    
    
    #observation-----------------------------------------------------------------
    observation<-obs4%>%filter(domain_id=='Observation')
    observation2<-observation%>%select(,c('obsid','patid','concept_id', 'obsdate', 'staffid','medcodeid', 'value', 'numunitid','numrangelow', 'numrangehigh', 'parentobsid' ))
    colnames(observation2)<-c('observation_id', 'person_id', 'observation_source_concept_id', 'observation_date', 'provider_id', 'observation_source_value', 'value_as_number', 'unit_source_value', 'range_low', 'range_high','visit_occurrence_id')
    
    other<-obs4%>%filter(domain_id=='Meas Value'|domain_id=='Gender')%>%select(,c('obsid','patid','concept_id', 'obsdate', 'staffid','medcodeid', 'parentobsid' ))
    colnames(other)<-c('observation_id', 'person_id', 'value_as_concept_id', 'observation_date', 'provider_id', 'value_source_value','visit_occurrence_id')
    
    observation3<-bind_rows(observation2, other)
    
    observation3%>%write_csv(paste0('CPRD/mapped/observation_',i,'_',f,'.csv'))
    
    
    #device exposure-----------------------------------------------------------------
    device<-obs4%>%filter(domain_id=='Device')
    device2<-device%>%select(,c('obsid','patid','concept_id', 'obsdate', 'staffid','medcodeid' , 'parentobsid'))
    colnames(device2)<-c('device_exposure_id', 'person_id', 'device_source_concept_id', 'device_date', 'provider_id', 'device_source_value','visit_occurrence_id')
    
    device2%>%write_csv(paste0('CPRD/mapped/device_exposure_',i,'_',f,'.csv'))
    
    
    #Specimen--------------------------------------------------------------------------------
    spec<-obs4%>%filter(domain_id=='Specimen')%>%select(,c('obsid','patid','concept_id', 'obsdate','medcodeid', 'parentobsid' ))
    colnames(spec)<-c('Specimen_id', 'person_id', 'Specimen_source_concept_id', 'Specimen_date',  'Specimen_source_value','visit_occurrence_id')
    
    spec%>%write_csv(paste0('CPRD/mapped/specimen_',i,'_',f,'.csv'))
    
    #Condition-----------------------------------------------------
    condition<-obs4%>%filter(domain_id=='Condition')
    condition2<-condition%>%select(,c('obsid','patid','concept_id', 'obsdate', 'staffid','medcodeid', 'parentobsid' ))
    colnames(condition2)<-c('condition_occurrence_id', 'person_id', 'condition_source_concept_id', 'condition_start_date', 'provider_id', 'condition_source_value', 'visit_occurrence_id')
    
    condition2%>%write_csv(paste0('CPRD/mapped/condition_occurrence_',i,'_',f,'.csv'))
    
    
    #Obs Drug--------------------------------------------------------------------
    obs_drug<-obs4%>%filter(domain_id=='Drug')
    
    obs_drug%>%write_csv(paste0('CPRD/mapped/obs_drug_',i,'_',f,'.csv'))
    
    #Unlisted---------------------------------------------------------------
    unlisted<-obs4%>%filter(domain_id!='Drug'&domain_id!='Procedure'&domain_id!='Condition'& domain_id!='Observation'& domain_id!='Measurement'& domain_id!='Specimen'& domain_id!='Device'& domain_id!='Meas Value'& domain_id!='Gender')                        
    
    unlisted%>%write_csv(paste0('CPRD/mapped/unlisted_',i,'_',f,'.csv'))
    
    
  }
}


#DB write===================================================================================

condition_occurrence_files <- list.files(path = 'CPRD/mapped/', pattern = "\\condition_occurrence", full.names = TRUE)

for (i in 701:1237){
  
  condition_occurrence <- read_csv(condition_occurrence_files[i])
  
  
  #condition_occurrence2<-bind_rows(condition_occurrence)
  
  condition_occurrence3<-left_join(condition_occurrence, rel, by =c('condition_source_concept_id'='concept_id_1'))
  colnames(condition_occurrence3)[8]<-'condition_concept_id'
  
  
  dbWriteTable(con, name="condition_occurrence", value=condition_occurrence3, append=TRUE)
  
  
  #appendToTable(cprd$condition_occurrence, condition_occurrence3)
  print(i)
}





#----person---------------------------------------------------------------------------
person_files <- list.files(path = 'CPRD/mapped/', pattern = "\\person", full.names = TRUE)

person <- lapply(person_files, read_csv)
person2<-bind_rows(person)

dbWriteTable(con, name="person", value=person2, append=TRUE)

#----care_site---------------------------------------------------------------------------
care_site_files <- list.files(path = 'CPRD/mapped/', pattern = "\\care_site", full.names = TRUE)

care_site <- lapply(care_site_files, read_csv)
care_site2<-bind_rows(care_site)

care_site3<-care_site2[duplicated(care_site2), ]


dbWriteTable(con, name="care_site", value=care_site3, append=TRUE)




#----procedure---------------------------------------------------------------------------
procedure_files <- list.files(path = 'CPRD/mapped/procedure', pattern = "\\procedure", full.names = TRUE)

procedure_files2<-paste0('CPRD/mapped/',procedure_files )

for (i in 3:1238){
  procedure <- read_csv(procedure_files2[i])
  #procedure2<-bind_rows(procedure)
  
  procedure3<-left_join(procedure, rel, by =c('procedure_source_concept_id'='concept_id_1'))
  colnames(procedure3)[8]<-'procedure_concept_id'
  
  dbWriteTable(con, name="procedure_occurrence", value=procedure3, append=TRUE)
  
}


#----specimen---------------------------------------------------------------------------
#specimen_files <- list.files(path = 'CPRD/mapped/specimen')#, pattern = "\\specimen", full.names = TRUE)

specimen_files2<-paste0('CPRD/mapped/',specimen_files )
#specimen <- lapply(specimen_files2, read_csv)

for (i in 1:1237){
  specimen <- read_csv(specimen_files2[i])
  
  specimen$specimen_id<-as.character(specimen$specimen_id)
  #specimen2<-bind_rows(specimen[c(1:67,69:597, 599:1237)])
  
  specimen3<-left_join(specimen, rel, by =c('specimen_source_concept_id'='concept_id_1'))
  colnames(specimen3)['concept_id_2']<-'specimen_concept_id'
  
  #specimen2%>%write_rds('CPRD/tables/cprd_specimen.rds', compress = 'xz' )
  dbWriteTable(con, name="specimen", value=specimen3, append=TRUE)
  
}


#----device_exposure---------------------------------------------------------------------------
#device_exposure_files <- list.files(path = 'CPRD/mapped/device')#, pattern = "\\device_exposure", full.names = TRUE)


device_exposure_files2<-paste0('CPRD/mapped/',device_exposure_files )
for (i in 1:1237){
  device_exposure <- read_csv(device_exposure_files2[i])
  #device_exposure2<-bind_rows(device_exposure[c(1:597, 599:1237)])
  
  device_exposure3<-left_join(device_exposure, rel, by =c('device_source_concept_id'='concept_id_1'))
  colnames(device_exposure3)['concept_id_2']<-'device_concept_id'
  dbWriteTable(con, name="device_exposure", value=device_exposure3, append=TRUE)
  
  #device_exposure2%>%write_rds('CPRD/tables/cprd_device_exposure.rds', compress = 'xz' )
  
}




#measurement_files <- list.files(path = 'CPRD/mapped/', pattern = "\\measurement", full.names = TRUE)

for (i in 51:1237){
  
  measurement <- read_csv(measurement_files[i])
  measurement2<-bind_rows(measurement)
  
  measurement3<-left_join(measurement2, rel, by =c('measurement_source_concept_id'='concept_id_1'))
  colnames(measurement3)[12]<-'measurement_concept_id'
  dbWriteTable(con, name="measurement", value=measurement3, append=TRUE)
  
}

#----observation---------------------------------------------------------------------------
observation_files <- list.files(path = 'CPRD/mapped/', pattern = "\\observation", full.names = TRUE)


for (i in 5:1237){
  
  observation <- read_csv(observation_files[i])
  observation2<-bind_rows(observation)
  
  observation3<-left_join(observation2, rel, by =c('observation_source_concept_id'='concept_id_1'))
  colnames(observation3)[14]<-'observation_concept_id'
  dbWriteTable(con, name="observation", value=observation3, append=TRUE)
  
  
}#------------------------------------------------------------------------
files <- list.files(path = 'CPRD/mapped/', pattern = "\\drug", full.names = TRUE)
drug_files<-paste0('CPRD/mapped/',files )


for (i in 6:890){
  drug <- read_csv(dru_files[i])%>%select(,c(1:10, "concept_id" ))
  colnames(drug)[9]<-'visit_occurrence_id'
  colnames(drug)[11]<-'drug_source_concept_id'
  
  drug2<-left_join(drug, rel, by =c('drug_source_concept_id'='concept_id_1'))
  colnames(drug2)[12]<-'drug_concept_id'
  dbWriteTable(con, name="drug_exposure", value=drug2, append=TRUE)
}

#--------------------------------------------------------------------------

race3<-read_csv('cprd_race.csv')
race4<-left_join(race3, rel, c('concept_id'='concept_id_1'))
person2<-left_join(person, race4, by =c('person_id'='patid'))%>%select(,c(1:8,11))
colnames(person2)[8]<-'race_source_concept_id'
colnames(person2)[9]<-'race_concept_id'
dbWriteTable(con, name="person", value=person2, overwrite=TRUE)


#cprd$condition_occurrence<-condition_occurrence3

#------------------------------------------------------------------

#--------------------------------------------------------------------------

#person----------------------------------------------------------------------------------------------------------
for (i in 2:50){
  death <- read_delim(paste0("//lhcdevfiler/OMOP/CPRD/Aurum_AllPatID_set",i,"_Extract/Patient/Aurum_AllPatID_set",i,"_Extract_Patient_001.txt"), 
                      delim = "\t", escape_double = FALSE, 
                      trim_ws = TRUE)
  
  death2<-death%>%filter(cprd_ddate!='NA')%>%select(patid, cprd_ddate)
  colnames(death2)<-c('perso_id', 'death_date')
  dbWriteTable(con, name="death", value=death2, append=TRUE)
}


#------------------------------------------------------------------

for (i in 2:50){
  op <- read_delim(paste0("//lhcdevfiler/OMOP/CPRD/Aurum_AllPatID_set",i,"_Extract/Patient/Aurum_AllPatID_set",i,"_Extract_Patient_001.txt"), 
                   delim = "\t", escape_double = FALSE, 
                   trim_ws = TRUE)
  
  op2<-op%>%select(patid, regstartdate,regenddate)
  colnames(op2)<-c('perso_id', 'observation_period_start_date', 'observation_period_end_date')
  dbWriteTable(con, name="observation_period", value=op2, append=TRUE)
}


#------------------------------------------------------------------


#------------------------------------------------------------------
provider2<-data.frame()

for (i in 1:50){
  provider <- read_delim(paste0("//lhcdevfiler/OMOP/CPRD/Aurum_AllPatID_set",i,"_Extract/Staff/Aurum_AllPatID_set",i,"_Extract_Staff_001.txt"), 
                         delim = "\t", escape_double = FALSE, 
                         trim_ws = TRUE)
  
  colnames(provider)<-c('provider_id', 'care_site_id', 'provider_specialty_source_value')
  provider2<-bind_rows(provider2, provider)
}

provider3<-provider2[duplicated(provider2), ]
dbWriteTable(con, name="provider", value=provider3, append=TRUE)

