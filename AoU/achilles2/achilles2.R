forgeMeasure<-function(type,column,column2,tbl){
    sql2=''
    if (type=='cnt') sql=glue::glue("select '{tbl}' as a_table, '{column}' as a_column, 'cnt' as a_type, count(*) as count_value, {column} as stratum_1 
from @cdmDatabaseSchema.{tbl} group by {column}")
    else if (type=='cnt2') sql=glue::glue("select '{tbl}' as a_table, '{column}|{column2}' as a_column, '{type}' as a_type, count(*) as count_value, {column} as stratum_1, {column2} as stratum_2 
from @cdmDatabaseSchema.{tbl} group by {column},{column2}")
    else if (type=='tbl') sql=glue::glue("select '{tbl}' as a_table,  '{type}' as a_type
, count(*) as count_value
from @cdmDatabaseSchema.{tbl}")
    else if (type=='cnt3') sql=glue::glue("select '{tbl}' as a_table, '{column}|{column2}|{column3}' as a_column, '{type}' as a_type, count(*) as count_value, {column} as stratum_1, {column2} as stratum_2, {column3} as stratum_3 
from @cdmDatabaseSchema.{tbl} group by {column},{column2}, {column3}")
    else if (type=='cntAge')  sql=glue::glue("select '{tbl}' as a_table, '{column}|{column2}|Age_decile|gender_concept_id' as a_column, '{type}' as a_type,  
COUNT_BIG(distinct a.PERSON_ID) as count_value, {column} as stratum_1, year({column2}) as stratum_2, floor((year({column2}) - b.year_of_birth)/10) 
as stratum_3, gender_concept_id as stratum_4
from @cdmDatabaseSchema.{tbl} a inner join @cdmDatabaseSchema.person b on a.person_id=b.person_id
group by {column},year({column2}), floor((year({column2}) - b.year_of_birth)/10), gender_concept_id ")
    else if (type=='cntSrc')sql=   glue::glue("select '{tbl}' as a_table, '{column}|src_id'as a_column, '{type}' as a_type,
    a.{column} as stratum_1, src_id as src, count(*) as count_value
  from @cdmDatabaseSchema.{tbl} a inner join @cdmDatabaseSchema.{tbl}_ext b on a.{tbl}_id = b.{tbl}_id
  group by a.{column},  b.src_id")
    else if (type=='cnt2Src')sql=   glue::glue("select '{tbl}' as a_table, '{column}|{column2}|src_id' as a_column, '{type}' as a_type,
    a.{column} as stratum_1, a.{column2} as stratum_2, src_id as src, count(*) as count_value
  from @cdmDatabaseSchema.{tbl} a inner join @cdmDatabaseSchema.{tbl}_ext b on a.{tbl}_id = b.{tbl}_id
  group by a.{column}, a.{column2}, b.src_id")
    else if (type=='cntpt') sql=glue::glue("select '{tbl}' as a_table, '{column}' as a_column, 'cntpt' as a_type, COUNT_BIG(distinct PERSON_ID) as count_value, {column} as stratum_1 
from @cdmDatabaseSchema.{tbl} group by {column}")
    else if (type=='cnt2pt') sql=glue::glue("select '{tbl}' as a_table, '{column}|{column2}' as a_column, '{type}' as a_type, COUNT_BIG(distinct PERSON_ID) as count_value, {column} as stratum_1, {column2} as stratum_2 
from @cdmDatabaseSchema.{tbl} group by {column},{column2}")
    else if (type=='tblpt') sql=glue::glue("select '{tbl}' as a_table,  '{type}' as a_type
, COUNT_BIG(distinct PERSON_ID) as count_value
from @cdmDatabaseSchema.{tbl}")
    else if (type=='cnt3pt') sql=glue::glue("select '{tbl}' as a_table, '{column}|{column2}|{column3}' as a_column, '{type}' as a_type, COUNT_BIG(distinct PERSON_ID) as count_value, {column} as stratum_1, {column2} as stratum_2, {column3} as stratum_3 
from @cdmDatabaseSchema.{tbl} group by {column},{column2}, {column3}")
    else if (type=='cntMnthPt') sql=glue::glue("select '{tbl}' as a_table, '{column}|year|month' as a_column,'{type}' as a_type, 
COUNT_BIG(distinct PERSON_ID) as count_value, {column} as stratum_1, 
YEAR( {column2} ) as stratum_2,MONTH( {column2} ) as stratum_3  from @cdmDatabaseSchema.{tbl} group by {column},YEAR( {column2} ), MONTH( {column2} )")
   else if (type=='cntMnth') sql=glue::glue("select '{tbl}' as a_table, '{column}|year|month' as a_column,'{type}' as a_type, 
count(*) as count_value, {column} as stratum_1, 
YEAR( {column2} ) as stratum_2,MONTH( {column2} ) as stratum_3  from @cdmDatabaseSchema.{tbl} group by {column},YEAR( {column2} ), MONTH( {column2} )")
   
    else {sql='';sql2=''}
        
    return(sql)
}

#forgeMeasure(type,column,column2,tbl)
type='cnt2'
a=forgeMeasure(type,column,column2,tbl)
#a
ll=list()
#c(ll,a)
#ll
           

#measure data frame (canonical definitions)
mdf=read_csv(file='tbl,column,column2,column3,type
visit_occurrence,visit_concept_id,,,cnt
visit_occurrence,visit_type_concept_id,,,cnt
death,death_type_concept_id,,,cnt
death,cause_concept_id,,,cnt
condition_occurrence,condition_concept_id,,,cnt
condition_occurrence,condition_type_concept_id,,,cnt
procedure_occurrence,procedure_concept_id,,,cnt
procedure_occurrence,procedure_type_concept_id,,,cnt
drug_exposure,drug_concept_id,,,cnt
drug_exposure,drug_type_concept_id,,,cnt
device_exposure,device_concept_id,,,cnt
device_exposure,device_type_concept_id,,,cnt
observation, observation_concept_id,,,cnt
measurement, measurement_concept_id,,,cnt
measurement, measurement_type_concept_id,,,cnt
observation,observation_type_concept_id,,,cnt
observation,observation_concept_id,value_as_concept_id,,cnt2
measurement,measurement_concept_id,value_as_concept_id,,cnt2
condition_occurrence,condition_concept_id,condition_start_date,,cntAge
procedure_occurrence,procedure_concept_id,procedure_date,,cntAge
drug_exposure,drug_concept_id,drug_exposure_start_date,,cntAge
device_exposure,device_concept_id,device_exposure_start_date,,cntAge
person,,,,tbl
provider,,,,tbl
visit_occurrence,,,,tbl
death,,,,tbl
condition_occurrence,,,,tbl
procedure_occurrence,,,,tbl
drug_exposure,,,,tbl
device_exposure,,,,tbl
measurement,,,,tbl
observation,,,,tbl
visit_occurrence,visit_concept_id,,,cntpt
visit_occurrence,visit_type_concept_id,,,cntpt
death,death_type_concept_id,,,cntpt
death,cause_concept_id,,,cntpt
condition_occurrence,condition_concept_id,,,cntpt
procedure_occurrence,procedure_concept_id,,,cntpt
drug_exposure,drug_concept_id,,,cntpt
device_exposure,device_concept_id,,,cntpt
observation, observation_concept_id,,,cntpt
measurement, measurement_concept_id,,,cntpt
observation,observation_concept_id,value_as_concept_id,,cnt2pt
measurement,measurement_concept_id,value_as_concept_id,,cnt2pt
condition_occurrence,condition_type_concept_id,,,cntpt
procedure_occurrence,procedure_type_concept_id,,,cntpt
drug_exposure,drug_type_concept_id,,,cntpt
device_exposure,device_type_concept_id,,,cntpt
measurement, measurement_type_concept_id,,,cntpt
observation,observation_type_concept_id,,,cntpt
visit_occurrence,,,,tblpt
condition_occurrence,,,,tblpt
procedure_occurrence,,,,tblpt
drug_exposure,,,,tblpt
device_exposure,,,,tblpt
measurement,,,,tblpt
observation,,,,tblpt
visit_occurrence,visit_concept_id,,,cntSrc
visit_occurrence,visit_type_concept_id,,,cntSrc
condition_occurrence,condition_concept_id,,,cntSrc
procedure_occurrence,procedure_concept_id,,,cntSrc
drug_exposure,drug_concept_id,,,cntSrc
device_exposure,device_concept_id,,,cntSrc
observation, observation_concept_id,,,cntSrc
measurement, measurement_concept_id,,,cntSrc
observation,observation_concept_id,value_as_concept_id,,cnt2Src
measurement,measurement_concept_id,value_as_concept_id,,cnt2Src
condition_occurrence,condition_type_concept_id,,,cntSrc
procedure_occurrence,procedure_type_concept_id,,,cntSrc
drug_exposure,drug_type_concept_id,,,cntSrc
device_exposure,device_type_concept_id,,,cntSrc
measurement, measurement_type_concept_id,,,cntSrc
observation,observation_type_concept_id,,,cntSrc
visit_occurrence,visit_concept_id, visit_start_date,,cntMnthPt
condition_occurrence,condition_concept_id, condition_start_date,,cntMnthPt
procedure_occurrence,procedure_concept_id, procedure_date,,cntMnthPt
drug_exposure,drug_concept_id, drug_exposure_start_date,,cntMnthPt
measurement,measurement_concept_id, measurement_date,,cntMnthPt
observation,observation_concept_id, observation_date,,cntMnthPt
device_exposure,device_concept_id, device_exposure_start_date,,cntMnthPt
visit_occurrence,visit_type_concept_id, visit_start_date,,cntMnthPt
condition_occurrence,condition_type_concept_id, condition_start_date,,cntMnthPt
drug_exposure,drug_type_concept_id, drug_exposure_start_date,,cntMnthPt
procedure_occurrence,procedure_type_concept_id, procedure_date,,cntMnthPt
measurement,measurement_type_concept_id, measurement_date,,cntMnthPt
observation,observation_type_concept_id, observation_date,,cntMnthPt
device_exposure,device_type_concept_id, device_exposure_start_date,,cntMnthPt
visit_occurrence,visit_concept_id, visit_start_date,,cntMnth
condition_occurrence,condition_concept_id, condition_start_date,,cntMnth
procedure_occurrence,procedure_concept_id, procedure_date,,cntMnth
drug_exposure,drug_concept_id, drug_exposure_start_date,,cntMnth
measurement,measurement_concept_id, measurement_date,,cntMnth
observation,observation_concept_id, observation_date,,cntMnth
device_exposure,device_concept_id, device_exposure_start_date,,cntMnth
visit_occurrence,visit_type_concept_id, visit_start_date,,cntMnth
condition_occurrence,condition_type_concept_id, condition_start_date,,cntMnth
drug_exposure,drug_type_concept_id, drug_exposure_start_date,,cntMnth
procedure_occurrence,procedure_type_concept_id, procedure_date,,cntMnth
measurement,measurement_type_concept_id, measurement_date,,cntMnth
observation,observation_type_concept_id, observation_date,,cntMnth
device_exposure,device_type_concept_id, device_exposure_start_date,,cntMnth
visit_occurrence,visit_source_concept_id,,,cnt
condition_occurrence,condition_source_concept_id,,,cnt
death,cause_source_concept_id,,,cnt
procedure_occurrence,procedure_source_concept_id,,,cnt
drug_exposure,drug_source_concept_id,,,cnt
observation,observation_source_concept_id,,,cnt
measurement,measurement_source_concept_id,,,cnt
device_exposure,device_source_concept_id,,,cnt
measurement, measurement_concept_id, measurement_source_concept_id,,cnt2
observation, observation_concept_id, observation_source_concept_id,,cnt2
condition_occurrence, condition_concept_id, condition_source_concept_id,,cnt2
procedure_occurrence, procedure_concept_id, procedure_source_concept_id,,cnt2
drug_exposure, drug_concept_id, drug_source_concept_id,,cnt2
device_exposure, device_concept_id, device_source_concept_id,,cnt2
procedure_occurrence, procedure_source_concept_id, modifier_concept_id,, cnt2
measurement, measurement_concept_id, value_as_number,,cnt2
observation, observation_concept_id, value_as_number,,cnt2
condition_occurrence, condition_source_concept_id,,,cntSrc
condition_occurrence, condition_type_concept_id, condition_concept_id,,cnt2
death, death_type_concept_id, cause_concept_id,,cnt2
procedure_occurrence, procedure_type_concept_id, procedure_concept_id,,cnt2
drug_exposure, drug_type_concept_id, drug_concept_id,,cnt2
device_exposure, device_type_concept_id, device_concept_id,,cnt2
measurement, measurement_type_concept_id, measurement_concept_id,,cnt2
observation, observation_type_concept_id, observation_concept_id,,cnt2
condition_occurrence, condition_source_concept_id, condition_source_value,,cnt2
procedure_occurrence, procedure_source_concept_id, procedure_source_value,,cnt2
drug_exposure, drug_source_concept_id, drug_source_value,,cnt2
device_exposure, device_source_concept_id, device_source_value,,cnt2
measurement, measurement_source_concept_id, measurement_source_value,,cnt2
observation, observation_source_concept_id, observation_source_value,,cnt2
observation_period, ,,,tbl
observation_period, period_type_concept_id,,,cnt
observation_period, period_type_concept_id,,,cntpt

')


mdf

forgeAllMeasures<-function(mdf){
    #loop over all rows in dataframe and forge each measure
    mdf2=mdf
    for (i in 1:nrow(mdf)){
        mdf2$sql[i]=forgeMeasure(type = mdf$type[i],column=mdf$column[i],column2=mdf$column2[i],tbl=mdf$tbl[i])    
    }
    
    #will have sql column
    return(mdf2)
}




llSql=forgeAllMeasures(mdf)


llSql