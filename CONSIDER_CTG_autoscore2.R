# CONSIDER section 2 Data Sharing Auto Score

#ClinicalTrials.gov Connection
# To run this script  first go to https://aact.ctti-clinicaltrials.org/connect and create a login
# Enter created username and password below 
user=username
psw=password


# Connects to AACt relational version of CTG database
library(RPostgreSQL)
library(tidyverse)
library(readr)
drv <- dbDriver('PostgreSQL')
con <- dbConnect(drv, dbname="aact",host="aact-db.ctti-clinicaltrials.org", port=5432, user= user, password= psw)
tbls<-RPostgreSQL::dbListTables(con)
tbls


# Set study to score with ClinicalTrials.gov (NCT) ID
nct =covid9_vacc[,1]

CONSIDER<-function(nct){
  
study_q<-paste0("select * FROM studies  where nct_id = '", nct, "'") 
study<-dbGetQuery(con, study_q)
doc_q<-paste0("select * FROM documents  where nct_id = '", nct, "'") 
doc<-dbGetQuery(con, doc_q)
# 2.1 Register your study at ClinicalTrials.gov registry
# If not registered on CLinicalTrials.gov 2.1-2.7 = 0, Data Sharing = 0

  registered<-1
  # 2.2 Do not limit study metadata to the legally required elements. Also populate optional elements
  # Cannot be auto scored as requires multiple fields based on legal and CTG registry requirments
  
  # 2.3 Fully populate data_sharing_plan text filed on ClinicalTrials.gov
  if (is.na(study$plan_to_share_ipd_description )){
    sharing_plan<-0
  } else {
    sharing_plan<-1
  }
  # 2.4 If Individual Participant Data is shared on a data sharing platform, update the ClinicalTrials.gov record with the URL link to the data.
  if(is.na(study$ipd_url )){
    link<-0
  } else{
    link<-1
  }
 # 2.5 Provide basic summary results using results registry component of Clinicaltrials.gov
   if( is.na(study$results_first_posted_date)){
    results<-0
  }else {
    results<-1
  }
  # 2.6 Utilize ClinicalTrials.gov fields for uploading study protocol, empty case report forms, statistical analysis plan and study URL link
  
  if (length(doc$nct_id)!= 0){
    doc_upload<-1
  } else{
    doc_upload<-0
  }
  # 2.7 Provide de-identified Individual Participant Data
  
  if (is.na(study$plan_to_share_ipd )){
    ipd<-0
  } else if (study$plan_to_share_ipd == 'Yes'){
    ipd<-1
  } else{
    ipd<-0
  }

# Combine for section 2. Data Sharing score
checklist_item<-c('2.1 Registration','2.3 Sharing Plan','2.4 IPD Link','2.5 Deposited Results','2.6 Doc Upload','2.7 Share IPD')
CTG_score<-data.frame(rbind (registered, sharing_plan, link, results, doc_upload,ipd))
Sharing_section_score= sum(CTG_score)
CTG_score2<-cbind(checklist_item, CTG_score)
colnames(CTG_score2)[2]<-'CONSIDER_Score'
CTG_score2 %>% write_csv(paste0("'",nct,"_CONSIDER_score.csv"))

}

lapply(nct, CONSIDER)    