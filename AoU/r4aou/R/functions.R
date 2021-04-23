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

#' Run query
#'
#' @param sql
#'
#' @return
#' @export
#'
#' @examples
aou_run_old <-function(sql){
  billing=Sys.getenv('GOOGLE_PROJECT')
  cdmDatabaseSchema=Sys.getenv('WORKSPACE_CDR')
  sql <- SqlRender::render(sql,cdmDatabaseSchema=cdmDatabaseSchema)
  sql <- SqlRender::translate(sql,targetDialect = 'bigquery')
  #below has error Warning message in stringr::str_replace_all(sql, "r2020q4r2", "R2020Q4R2"):
     #Error in stringr::str_replace_all(sql, "r2020q4r2", "R2020Q4R2"): lazy-load database '/usr/local/lib/R/site-library/stringi/R/stringi.rdb' is corrupt
  #sql=stringr::str_replace_all(sql,'r2020q4r2','R2020Q4R2')

  #using base R instead  
  sql=gsub("r2020q4r2", "R2020Q4R2", sql)

  #cat(sql)
  q <- bigrquery::bq_project_query(billing, sql)
  out<-bigrquery::bq_table_download(q)
  out
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
  sql=stringr::str_replace_all(sql,'r2020q4r2','R2020Q4R2')
  cat(sql)
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
aou_getDd<-function(){
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



# Run cohorts
#load from Atlas
#Edit query
# Run SQL

execute_cohort <-function(cohortID){
  #grabs sql from cohort_id
  baseUrl='http://atlas-demo.ohdsi.org:80/WebAPI'
  version <- ROhdsiWebApi:::getWebApiVersion(baseUrl = baseUrl)
  d=getDefinitionsMetadata(baseUrl,category = 'cohort')
  aa=getCohortDefinition(cohortID,baseUrl)
  sql=getCohortSql(aa,baseUrl)
  
  #cohort edits
  # Declare database schema
  sql2= gsub("@cdm_database_schema", "@cdmDatabaseSchema", sql)
  #Remove result schema since not present
  sql2= gsub("@results_database_schema.", "", sql2)
  #declare vocabulary schema as same as database schema
  sql2= gsub("@vocabulary_database_schema", "@cdmDatabaseSchema", sql2)
  #Remove deletion since table does not already exist
  sql2=gsub("DELETE FROM @target_database_schema.@target_cohort_table where cohort_definition_id = @target_cohort_id;","",sql2)
  #create final table with select statment
  sql2=gsub("select @target_cohort_id as cohort_definition_id, person_id, start_date, end_date", "create temp table #target_cohort_table AS (select cohort_definition_id as target_cohort_id, co.person_id as subject_id, co.start_date as cohort_start_date, co.end_date as 
cohort_end_date ", sql2) 
  # Add parentheses at end of table creation
  sql2=gsub("FROM #final_cohort CO", "FROM #final_cohort CO)", sql2)
  # Remove irrelevent parts
  sql2=gsub("delete from cohort_censor_stats where cohort_definition_id = @target_cohort_id;","",sql2)
  #generate output since no result schema exist
  sql2= paste(sql2, "
select * from #target_cohort_table")
  sql2 <- SqlRender::render(sql2,cdmDatabaseSchema=cdmDatabaseSchema)
  sql2 <- SqlRender::translate(sql2,targetDialect = 'bigquery')
  sql2=stringr::str_replace_all(sql2,'r2020q4r2','R2020Q4R2')
  # state temp table
  sql5= gsub("create table", "CREATE TEMP TABLE", sql2)
  sql5= gsub("CREATE TABLE", "CREATE TEMP TABLE", sql5)
  sql5=gsub("insert into @target_database_schema.@target_cohort_table ","",sql5)
  sql5=gsub("\\(cohort_definition_id, subject_id, cohort_start_date, cohort_end_date)","",sql5)
  sql5=gsub("cohort_definition_id",cohortID,sql5)
  cat(sql5)
  
  #run sql
  output=aou_run(sql5)
  return(output$result)
}

# Get the Case Report FOrms for the included data elements

aou_getCaseReportForms <-function(){
#connect concept_relationship  to concept
concept<-aou_run("select * from @cdmDatabaseSchema.concept")
  concept_relationship<-aou_run("select * from @cdmDatabaseSchema.concept_relationship")
  relationship_7<-left_join(concept_relationship,concept, by=c('concept_id_1'= 'concept_id'))
relationship_71<-left_join(relationship_7,concept, by=c('concept_id_2'= 'concept_id'))

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
}
