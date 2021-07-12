# Hello, world!
#
# This is an example function named 'hello'
# which prints 'Hello, world!'.
#
# You can learn more about package authoring with RStudio at:
#
#   http://r-pkgs.had.co.nz/
#
# Some useful keyboard shortcuts for package authoring:
#
#   Install Package:           'Ctrl + Shift + B'
#   Check Package:             'Ctrl + Shift + E'
#   Test Package:              'Ctrl + Shift + T'

#' List tables in the database
#'
#' @return
#' @export
#'
#' @examples
aou_tables <- function() {
  cdmDatabaseSchema=Sys.getenv('WORKSPACE_CDR')
  t<-bigrquery::bq_dataset_tables(Sys.getenv('WORKSPACE_CDR'))
  tbls=unlist(purrr::map(t,'table'))
  tbls

}


#' run sql command
#'
#' @param sql code to be executed
#'
#' @return
#' @export
aou_run <-function(sql){
  billing=Sys.getenv('GOOGLE_PROJECT')
  cdmDatabaseSchema=Sys.getenv('WORKSPACE_CDR')
  sql <- SqlRender::render(sql,cdmDatabaseSchema=cdmDatabaseSchema)
  sql <- SqlRender::translate(sql,targetDialect = 'bigquery')
  sql=stringr::str_replace_all(sql,'r2020q4r3','R2020Q4R3')
  q <- bigrquery::bq_project_query(billing, sql)
  out<-bigrquery::bq_table_download(q)
  list(query=sql,result=out)
}




#' Write data into bucket
#'
#' @param data data frame with the data
#' @param destination_filename  name of the file to store the data
#'
#' @return
#' @export
aou_write<-function(data,destination_filename){


  # store the dataframe in current workspace
  readr::write_excel_csv(data, destination_filename)

  # Get the bucket name
  my_bucket <- Sys.getenv('WORKSPACE_BUCKET')

  # Copy the file from current workspace to the bucket
  system(paste0("gsutil cp ./", destination_filename, " ", my_bucket, "/data/"), intern=T)

  # Check if file is in the bucket
  #system(paste0("gsutil ls ", my_bucket, "/data/*.csv"), intern=T)
}

#' Write rds data into bucket
#'
#' @param data data frame with the data
#' @param destination_filename  name of the file to store the data
#'
#' @return
#' @export
aou_write_rds<-function(data,destination_filename){


  # store the dataframe in current workspace
  readr::write_rds(data, destination_filename,compress='xz')

  # Get the bucket name
  my_bucket <- Sys.getenv('WORKSPACE_BUCKET')

  # Copy the file from current workspace to the bucket
  system(paste0("gsutil cp ./", destination_filename, " ", my_bucket, "/data/"), intern=T)

  # Check if file is in the bucket
  #system(paste0("gsutil ls ", my_bucket, "/data/*.csv"), intern=T)
}



#' List columns of a table
#'
#' @param table name of table (e.g., person)
#'
#' @return data frame with fields and type and description
#' @export
aou_tbl_fields<-function(table){
  cdm2=purrr::map_chr(stringr::str_split(Sys.getenv('WORKSPACE_CDR'),pattern = '\\.'),c(2))
  project2=purrr::map_chr(stringr::str_split(Sys.getenv('WORKSPACE_CDR'),pattern = '\\.'),c(1))

  bqtable=list(projectId=project2,datasetId=cdm2,tableId=table)
  #bqtable
  f=bigrquery::bq_table_fields(bqtable)
  #f
  out=data.frame(table=table,name=purrr::map_chr(f,'name'),type=purrr::map_chr(f,'type'),mode=purrr::map_chr(f,'mode'), description=(purrr::map_chr(f,'description')))
  out
}





#' Generate list of tables and columns
#'
#' @return data.frame with data dictionary
#' @export
aou_get_dd<-function(){
  t<-bigrquery::bq_dataset_tables(Sys.getenv('WORKSPACE_CDR'))
  #t[[1]]$table
  #f=bq_table_fields(t[[1]])

  #str(f)
  #f
  #f[[1]]

  parse<-function(table) {
    f=bigrquery::bq_table_fields(table)

    #writeLines(table$table)
    Sys.sleep(0.2)
    data.frame(table=table$table,name=purrr::map_chr(f,'name')
               ,type=purrr::map_chr(f,'type')
               ,mode=purrr::map_chr(f,'mode')
               ,description=(purrr::map_chr(f,'description')))
  }
  #parse(t[[2]])
  #length(t)
  #lm=map(sample(t,80),parse)
  #lm=map(head(t,80),parse)
  lm=purrr::map(t,parse)


  ln=dplyr::bind_rows(lm)
  ln
  #as.data.frame(ln)
  #nrow(ln)
}





#' Execute a cohort from server 
#'
#' @param cohortId identifier of cohort on atlas server (in URL of the cohort when defining it)
#' @param baseUrl URL of the Atlas server
#' @return cohort id and data.frame with cohort data (person, start and end date)
#' @export
aou_execute_cohort <-function(cohortId,baseUrl='http://atlas-demo.ohdsi.org:80/WebAPI'){
billing=Sys.getenv('GOOGLE_PROJECT')
cdmDatabaseSchemaInternal=Sys.getenv('WORKSPACE_CDR')

#grabs sql from cohort_id

version <- ROhdsiWebApi:::getWebApiVersion(baseUrl = baseUrl)
d=getDefinitionsMetadata(baseUrl,category = 'cohort')
aa=getCohortDefinition(cohortId,baseUrl)
sql=getCohortSql(aa,baseUrl)

#prep the temp table (compensate for lack of result schema here    - ^ step
  #cohort sensor table, target_cohort_table (always call 
  #@target_database_schema
  #@target_cohort_table  
  
#create pseudoresult table targetCohortTable  
TargetCohortTable  = cohortId
  
#modify slighly the SQL  
        sql2= gsub("@cdm_database_schema", "@cdmDatabaseSchema", sql)
        #Remove result schema since not present
        sql2= gsub("@results_database_schema.", "", sql)
#declare vocabulary schema as same as database schema
sql2= gsub("@vocabulary_database_schema", "@cdm_database_schema", sql2)
sql2= gsub("@target_database_schema.", "", sql2)
sql2=gsub("delete from cohort_censor_stats where cohort_definition_id = @target_cohort_id;","",sql2)
sql2=gsub("cohort_definition_id","@cohort_definition_id",sql2)

# Add parentheses at end of table creation
#generate output since no result schema exist
sql2= paste(sql2, "
        select * from #target_cohort_table;")
#create final table with select statment
  sql2=paste("CREATE temp TABLE #target_cohort_table (
  cohort_definition_id INT64 not null, subject_id INT64 not null, cohort_start_date DATE, cohort_end_date DATE)
;", sql2)

sql3rendered <- SqlRender::render(sql2,cdm_database_schema=cdmDatabaseSchema,target_cohort_id=cohortId, target_cohort_table= '#target_cohort_table')

#switching to not a calling aou_run at all here 
#step TWO - translating
sql4translated <- SqlRender::translate(sql3rendered,targetDialect = 'bigquery')

sql5= gsub("create table", "CREATE TEMP TABLE", sql4translated)
sql5= gsub("CREATE TABLE", "CREATE TEMP TABLE", sql5)
sql5=gsub("and e.end_date >= i.start_date","",sql5)

sql5=stringr::str_replace_all(sql5,'r2020q4r3','R2020Q4R3')
#run and export results
q <- bigrquery::bq_project_query(billing, sql5)
out<-bigrquery::bq_table_download(q)
output<-list(query=sql5,result=out)
return(list(result=output$result,cohortId=cohortId,OhdsiSql=sql,RenderedSql=sql3rendered,TranslatedSql=sql4translated,ExecutedSql=sql5))
}




#' obtain CRF data dictionary as data.frame using vocabulary tables
#'
#' @return dataframe of CRF dictionary as Answe/Question/Topic/Module
#' @export

aou_get_case_report_forms <-function(){
  
  # q means question, a means answer, t means topic and m means module
  # connect concept_relationship  to concept
  
  #fetch tables into memory
  concept<-aou_run("select * from @cdmDatabaseSchema.concept")$result 
  concept_relationship<-aou_run("select * from @cdmDatabaseSchema.concept_relationship")$result
  
  #make joins in memory  
  relationship_7<-left_join(concept_relationship,concept, by=c('concept_id_1'= 'concept_id'))
  relationship_71<-left_join(relationship_7,concept, by=c('concept_id_2'= 'concept_id'))
  
  # end of preparation and moving on to main problem of the function (in memory) to create CRF dictionary
  
  #Find topic/module relationship
  tm= relationship_71 %>% filter(concept_class_id.x=='Topic') %>% filter(relationship_id=='Is a') %>% filter(concept_class_id.y=='Module')
  tm_1= relationship_71 %>% filter(concept_class_id.y=='Topic') %>% filter(relationship_id=='PPI parent code of') %>% filter(concept_class_id.x=='Module')
  
  #find question/topic relationship
  qt<-relationship_71%>%filter(relationship_id=='Is a'| relationship_id=='Has PPI parent code')
  qt2<-qt%>%filter(concept_class_id.y=='Topic')
  qt3<-qt2%>%filter(concept_class_id.x=='Clinical Observation'|concept_class_id.x=='Question'|concept_class_id.x=='Survey')
  
  # Combine  Module/Topic/Question
  qtm<-left_join(qt3, tm2, by=c('concept_id_2'='concept_id_1'))
  
  #Get Question/Answer
  aq2= relationship_71 %>% filter(concept_class_id=='Answer') %>% filter(relationship_id=='Answer of (PPI)') #%>% filter(concept_class_id_1=='Question')
  
  #Combine Answer/Question/Topic/Module
  aqtm2 =left_join(aq2, qtm, by=c('concept_id_2'='concept_id_1'))
  
  # return dictionary as dataframe
  return(aqtm2)
}
