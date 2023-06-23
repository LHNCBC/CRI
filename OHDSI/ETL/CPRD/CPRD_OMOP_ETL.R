# This code was used to transform the CPRD AURUM data into the OMOP CDM
#-Craig S Mayer
#-National Library of Medicine

#Load Libraries
library(dplyr)
library(dbplyr)
library(readr)
library(DBI)

library(tidyverse)
library(RSQLite)



#Load reference files

rel<-read_tsv('CONCEPT_RELATIONSHIP')%>%filter(relationship_id=='Maps to')

concept<-read_tsv('CONCEPT.csv')
#filter to just get SNOMED concepts
snomed<-concept%>%filter(vocabulary_id=='SNOMED')


med <- read_delim("//lhcdevfiler/OMOP/CPRD/202203_Lookups_CPRDAurum/202203_Lookups_CPRDAurum/202203_EMISMedicalDictionary.txt", 
                  delim = "\t", escape_double = FALSE, 
                  trim_ws = TRUE)

ObsType <- read_delim("//lhcdevfiler/OMOP/CPRD/202203_Lookups_CPRDAurum/202203_Lookups_CPRDAurum/ObsType.txt", 
                      delim = "\t", escape_double = FALSE, 
                      trim_ws = TRUE)
ObsType <- read_delim("//lhcdevfiler/OMOP/CPRD/202203_Lookups_CPRDAurum/202203_Lookups_CPRDAurum/ObsType.txt", 
                      delim = "\t", escape_double = FALSE, 
                      trim_ws = TRUE)


# Create empty OMOP database
con <- dbConnect(drv=RSQLite::SQLite(), dbname=paste0("cprd.sqlite"))



#Table creation through each data extract
for (i in 1:50){
  
  

  
  # Practice to Care site---------------------------------------------------------------------------------------------------------------------------------
  practice <- read_delim(paste0('//lhcdevfiler/OMOP/CPRD/Aurum_AllPatID_set', i,'_Extract/Practice/Aurum_AllPatID_set',i,'_Extract_Practice_001.txt'), 
                         delim = "\t", escape_double = FALSE, 
                         trim_ws = TRUE)
  practice2<-practice%>%select(,c('pracid', 'region'))
  #Rename to omop column names
  colnames(practice2)<-c('care_site_id', 'location_id')
  
  #Add to database
  dbWriteTable(con, name="care_site", value=practice2, append=TRUE)
  
  #Creating the observation period table------------------------------------------------------------------
  
  op <- read_delim(paste0("//lhcdevfiler/OMOP/CPRD/Aurum_AllPatID_set",i,"_Extract/Patient/Aurum_AllPatID_set",i,"_Extract_Patient_001.txt"), 
                   delim = "\t", escape_double = FALSE, 
                   trim_ws = TRUE)
  
  op2<-op%>%select(patid, regstartdate,regenddate)
  colnames(op2)<-c('perso_id', 'observation_period_start_date', 'observation_period_end_date')
  dbWriteTable(con, name="observation_period", value=op2, append=TRUE)
  
  #Provider------------------------------------------------------------------
  provider2<-data.frame()
  
  
  provider <- read_delim(paste0("//lhcdevfiler/OMOP/CPRD/Aurum_AllPatID_set",i,"_Extract/Staff/Aurum_AllPatID_set",i,"_Extract_Staff_001.txt"), 
                         delim = "\t", escape_double = FALSE, 
                         trim_ws = TRUE)
  
  colnames(provider)<-c('provider_id', 'care_site_id', 'provider_specialty_source_value')
  provider2<-bind_rows(provider2, provider)
}

provider3<-provider2[duplicated(provider2), ]
dbWriteTable(con, name="provider", value=provider3, append=TRUE)

  
  
  #CPRD Obervation file------------------------------------------------------------------------------------------------------------
  #Observation source name
  #There are multiple observation files per extract
  obs_files <- list.files(path = paste0('//lhcdevfiler/OMOP/CPRD/Aurum_AllPatID_set',i,'_Extract/Observation/'), pattern = "\\.txt$", full.names = TRUE)
  
  for (f in 11:length(obs_files)){
    
    
    
    obs <- read_delim(obs_files[f], 
                      delim = "\t", escape_double = FALSE, 
                      trim_ws = TRUE)
    
    
   
    #join observation file on Med COde ID
    obs2<-left_join(obs,med, by =c('medcodeid'='MedCodeId'))
    
    
    #Testing if ObsType works for domain determination
    obs3<-left_join(obs2,ObsType, by =c('obstypeid'='obstypeid'))
    
    
    obs3$SnomedCTConceptId<-as.character(obs3$SnomedCTConceptId)
    
    #Joining observation on snomed code to get OMOP concepts
    obs4_0<-left_join(obs3,snomed, by =c('SnomedCTConceptId'='concept_code'))
    #join releationshp table to get standard cocnept
    obs4_1<-left_join(obs4_1,rel, by =c('concept_id'='concept_id_1'))
    #join on standard concept, concpet table to get information on standradized concept such as domain
    obs4<-left_join(obs4_2,concept, by =c('concept_id_2'='concept_id'))
    
    
    
    
    
    
    #Procedure---------------------------------------------------------------
    procedure<-obs4%>%filter(domain_id=='Procedure')
    procedure2<-procedure%>%select(,c('obsid','patid','concept_id', 'obsdate', 'staffid','medcodeid', 'parentobsid' ))
    colnames(procedure2)<-c('procedure_occurrence_id', 'person_id', 'procedure_source_concept_id', 'proedure_date', 'provider_id', 'procedure_source_value','visit_occurrence_id' )
    
    dbWriteTable(con, name="procedure_occurrence", value=procedure2, append=TRUE)
    
    
    #Measurement-----------------------------------------------------------------
    measurement<-obs4%>%filter(domain_id=='Measurement')
    measurement2<-measurement%>%select(,c('obsid','patid','concept_id', 'obsdate', 'staffid','medcodeid', 'value', 'numunitid','numrangelow', 'numrangehigh', 'parentobsid' ))
    colnames(measurement2)<-c('measurement_id', 'person_id', 'measurement_source_concept_id', 'measurement_date', 'provider_id', 'measurement_source_value', 'value_as_number', 'unit_source_value', 'range_low', 'range_high','visit_occurrence_id')
    
    dbWriteTable(con, name="measurmenet", value=measurement2, append=TRUE)
    
    
    
    #observation-----------------------------------------------------------------
    #stated omop observation domain
    observation<-obs4%>%filter(domain_id=='Observation')
    observation2<-observation%>%select(,c('obsid','patid','concept_id', 'obsdate', 'staffid','medcodeid', 'value', 'numunitid','numrangelow', 'numrangehigh', 'parentobsid' ))
    colnames(observation2)<-c('observation_id', 'person_id', 'observation_source_concept_id', 'observation_date', 'provider_id', 'observation_source_value', 'value_as_number', 'unit_source_value', 'range_low', 'range_high','visit_occurrence_id')
    
    #other domains that typically fit inobservation table
    other<-obs4%>%filter(domain_id=='Meas Value'|domain_id=='Gender')%>%select(,c('obsid','patid','concept_id', 'obsdate', 'staffid','medcodeid', 'parentobsid' ))
    colnames(other)<-c('observation_id', 'person_id', 'value_as_concept_id', 'observation_date', 'provider_id', 'value_source_value','visit_occurrence_id')
    
    #combine to get full observation table
    observation3<-bind_rows(observation2, other)
    
    dbWriteTable(con, name="observation", value=observation3, append=TRUE)
    
    
    #device exposure-----------------------------------------------------------------
    device<-obs4%>%filter(domain_id=='Device')
    device2<-device%>%select(,c('obsid','patid','concept_id', 'obsdate', 'staffid','medcodeid' , 'parentobsid'))
    colnames(device2)<-c('device_exposure_id', 'person_id', 'device_source_concept_id', 'device_date', 'provider_id', 'device_source_value','visit_occurrence_id')
    
    dbWriteTable(con, name="device_exposure", value=device2, append=TRUE)
    
    
    #Specimen--------------------------------------------------------------------------------
    spec<-obs4%>%filter(domain_id=='Specimen')%>%select(,c('obsid','patid','concept_id', 'obsdate','medcodeid', 'parentobsid' ))
    colnames(spec)<-c('Specimen_id', 'person_id', 'Specimen_source_concept_id', 'Specimen_date',  'Specimen_source_value','visit_occurrence_id')
    
    dbWriteTable(con, name="specimen", value=spec, append=TRUE)
    
    #Condition-----------------------------------------------------
    condition<-obs4%>%filter(domain_id=='Condition')
    condition2<-condition%>%select(,c('obsid','patid','concept_id', 'obsdate', 'staffid','medcodeid', 'parentobsid' ))
    colnames(condition2)<-c('condition_occurrence_id', 'person_id', 'condition_source_concept_id', 'condition_start_date', 'provider_id', 'condition_source_value', 'visit_occurrence_id','condition_concept_id')
    
    dbWriteTable(con, name="condition_occurrence", value=condition2, append=TRUE)
    
    
    #Obs Drug--------------------------------------------------------------------
    #add drugs from observation table to previously created drug exposure
    obs_drug<-obs4%>%filter(domain_id=='Drug')
    
    dbWriteTable(con, name="drug_exposure", value=obs_drug, append=TRUE)
    
    #Unlisted---------------------------------------------------------------
    #Data rows that do not fit into omop cdm
    unlisted<-obs4%>%filter(domain_id!='Drug'&domain_id!='Procedure'&domain_id!='Condition'& domain_id!='Observation'& domain_id!='Measurement'& domain_id!='Specimen'& domain_id!='Device'& domain_id!='Meas Value'& domain_id!='Gender')                        
    

    #person----------------------------------------------------------------------------------------------------------
    person <- read_delim(paste0("//lhcdevfiler/OMOP/CPRD/Aurum_AllPatID_set",i,"_Extract/Patient/Aurum_AllPatID_set",i,"_Extract_Patient_001.txt"), 
                         delim = "\t", escape_double = FALSE, 
                         trim_ws = TRUE)
    
    
    person1<-person%>%select(,c('patid', 'yob', 'mob', 'pracid', 'usualgpstaffid','gender'))
    colnames(person1)<-c('person_id', 'year_of_birth', 'month_of_birth', 'care_site_id', 'provider_id', 'gender_source_value')
    
    #COnvert custom sex codings to omop gender concepts
    person1$gender_concept_id<-0
    person1$gender_concept_id[which(person1$gender_source_value == 2)] <-8532
    person1$gender_concept_id[which(person1$gender_source_value == 1)] <-8507
    
    
    #grabbing race from cprd observation table
    
    race4<-obs4%>%filter(domain_id='Race')
    person2<-left_join(person1, race4, by =c('person_id'='patid'))%>%select(,c(1:8,11))
    colnames(person2)[8]<-'race_source_concept_id'
    colnames(person2)[9]<-'race_concept_id'
    dbWriteTable(con, name="person", value=person2, overwrite=TRUE)
    
    
    
  }
}

