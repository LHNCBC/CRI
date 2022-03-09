#Researcher workbench code
# Can only be run using the All of US researcher workbench
# Resting Heart rate

library(bigrquery)

# This query represents dataset "Fitbit_heartrate multiple" for domain "fitbit_heart_rate_level" and was generated for All of Us Registered Tier Dataset v5
dataset_79319044_fitbit_heart_rate_level_sql <- paste("
  select
      person_id,
        DATE_TRUNC(dates, week) as week,
        avg(min_rate) as avg_rate 
from (
SELECT
        heart_rate_minute_level.person_id,
        DATE_TRUNC(heart_rate_minute_level.datetime, day) as dates,
        min(heart_rate_value) as min_rate 
    FROM
        `heart_rate_minute_level` heart_rate_minute_level   
     
                GROUP BY
                    person_id,
                    dates)
            group by
        person_id,
        week

", sep="")

full_fit <- bq_table_download(bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), dataset_79319044_fitbit_heart_rate_level_sql, billing=Sys.getenv("GOOGLE_PROJECT")), bigint="integer64")

fitbit_hr<-full_fit%>%group_by(person_id)%>%summarize(avg_rate2=mean(avg_rate))

fitbit_cnt<-full_fit%>%group_by(person_id)%>%count
fitbit<-merge(fitbit_cnt, fitbit_hr, by ='person_id')


aou_run2 <-function(sql){
  billing=Sys.getenv('GOOGLE_PROJECT')
  cdmDatabaseSchema=Sys.getenv('WORKSPACE_CDR')
  cohort_2=cohort2
  sql <- SqlRender::render(sql,cdmDatabaseSchema=cdmDatabaseSchema,cohort_2=cohort2 )
  sql <- SqlRender::translate(sql,targetDialect = 'bigquery')
  #below has error Warning message in stringr::str_replace_all(sql, "r2019q4r3", "R2019Q4R3"):
  #Error in stringr::str_replace_all(sql, "r2019q4r3", "R2019Q4R3"): lazy-load database '/usr/local/lib/R/site-library/stringi/R/stringi.rdb' is corrupt
  #sql=stringr::str_replace_all(sql,'r2020q4r2','R2020Q4R2')
  
  #using base R instead  
  sql=gsub("r2021q3r5", "R2021Q3R5", sql)
  
  #cat(sql)
  q <- bigrquery::bq_project_query(billing, sql)
  out<-bigrquery::bq_table_download(q)
  out
}

cohort2<-unique(full_fit$person_id)

cond<-"select distinct person_id, condition_concept_id, condition_start_date from @cdmDatabaseSchema.condition_occurrence
where year(condition_start_date) >2009 and person_id IN (@cohort_2)"

cond2<-aou_run2(cond)
cond_sum<-left_join(cond2, fitbit, by ='person_id')%>%filter(avg_rate2!='NA')
cond_fit<-left_join(cond_sum, full_fit, by='person_id')

cond_fit$date_diff<-cond_fit$condition_start_date-as.Date(cond_fit$week)
# week rate -all rate
cond_fit$delta_hr<-cond_fit$avg_rate-cond_fit$avg_rate2


#2 week bfore
near_cond<-cond_fit%>%filter(date_diff<=(7) & date_diff>(0))
#near_cond<-cond_fit%>%filter(date_diff<14 & date_diff>-14)



#length(unique(near_cond$person_id))
near_cond$above_avg_hr<-round(near_cond$delta_hr)
cnt<-near_cond%>%group_by(person_id, condition_concept_id)%>%summarize(above_avg_hr2=max(above_avg_hr))
cnt2<-cnt%>%group_by( condition_concept_id, above_avg_hr2)%>%count()


cnt2%>%write_csv('1wk_bef.csv')






# Heart rate spikes
sql2=" select * from (select a.person_id,
a.heart_rate_value,
a.datetime,
(a.heart_rate_value-LAG(a.heart_rate_value) OVER(PARTITION BY a.person_id ORDER BY a.datetime)) as change_hr,
(b.steps+LAG(b.steps) OVER(PARTITION BY a.person_id ORDER BY a.datetime)) as sum_steps,

  FROM heart_rate_minute_level a inner join steps_intraday b on a.person_id=b.person_id and a.datetime=b.datetime
)
where heart_rate_value >100 and change_hr > 25 and sum_steps <25  

"
all <- bq_table_download(bq_dataset_query(Sys.getenv("WORKSPACE_CDR"), sql2, billing=Sys.getenv("GOOGLE_PROJECT")), bigint="integer64")
cohort2<-unique(all$person_id)



acute_q<-"select condition_concept_id, count(*) as ev_cnt, count(distinct person_id) as pt_cnt
from @cdmDatabaseSchema.condition_occurrence where person_id IN (@cohort_2) group by condition_concept_id "

acute<-aou_run2(acute_q)
acute$density<-acute$ev_cnt/acute$pt_cnt
acute2<-acute%>%filter(pt_cnt>19 & density<6 )

aou_run3 <-function(sql){
  billing=Sys.getenv('GOOGLE_PROJECT')
  cdmDatabaseSchema=Sys.getenv('WORKSPACE_CDR')
  cohort_2=cohort2
  cohort_3=cohort3
  sql <- SqlRender::render(sql,cdmDatabaseSchema=cdmDatabaseSchema,cohort_2=cohort2, cohort_3=cohort3 )
  sql <- SqlRender::translate(sql,targetDialect = 'bigquery')
  #below has error Warning message in stringr::str_replace_all(sql, "r2019q4r3", "R2019Q4R3"):
  #Error in stringr::str_replace_all(sql, "r2019q4r3", "R2019Q4R3"): lazy-load database '/usr/local/lib/R/site-library/stringi/R/stringi.rdb' is corrupt
  #sql=stringr::str_replace_all(sql,'r2020q4r2','R2020Q4R2')
  
  #using base R instead  
  sql=gsub("r2021q3r5", "R2021Q3R5", sql)
  
  #cat(sql)
  q <- bigrquery::bq_project_query(billing, sql)
  out<-bigrquery::bq_table_download(q)
  out}


cohort3<-unique(acute2$condition_concept_id)


cond_q<-"select person_id, condition_concept_id, condition_start_date
from @cdmDatabaseSchema.condition_occurrence where person_id IN (@cohort_2) and condition_concept_id IN (@cohort_3)"
cond<-aou_run3(cond_q)



all$date<-as.Date(all$datetime)
all2<-all%>%group_by(person_id, date)%>%count()
spike<-left_join(cond, all2, by ="person_id")

spike$date_diff<-spike$condition_start_date-spike$date
spike3<-spike%>%filter(date_diff>(-28)&date_diff<28)

spike3$day<-NA
spike3<- within(spike3, day[date_diff >0& date_diff<=7] <- 7)
spike3<- within(spike3, day[date_diff >7& date_diff<=14] <-14)
spike3<- within(spike3, day[date_diff >14& date_diff<=21] <- 21)
spike3<- within(spike3, day[date_diff >21& date_diff<=28] <- 28)
spike3<- within(spike3, day[date_diff <=0& date_diff>=(-7)] <- (-7))
spike3<- within(spike3, day[date_diff <(-7) & date_diff>=(-14)] <- (-14))
spike3<- within(spike3, day[date_diff <(-14) & date_diff>=(-21)] <- (-21))
spike3<- within(spike3, day[date_diff <(-21)& date_diff>=(-28)] <- (-28))


spike3$day<-(-1)*spike3$day
pt<-spike3%>%group_by(person_id, condition_concept_id, day)%>%summarize( ev_cnt= sum(n))
pt$pts<-1
cnt<-pt%>%group_by( condition_concept_id, day)%>%summarize( pt_cnt=sum(pts), ev_cnt= sum(ev_cnt))
cnt$spike_density<-cnt$ev_cnt/cnt$pt_cnt


# Local code after extracting results from previous section from workbench

library(readr)
library(dplyr)
library(tidyverse)
library(maditr)

files <- list.files(path = 'raw2', pattern = "\\.csv$", full.names = TRUE)

read <- lapply(files, read_csv)
files2 = substr(files,1,nchar(files)-4)







ev<-read_csv('condition_occurrence-condition_concept_id_cnt.csv')
pt<-read_csv('condition_occurrence-condition_concept_id_cntpt.csv')

density<-left_join(ev, pt, by ='stratum_1')
density$ev_density<-density$count_value.x/density$count_value.y
density2<-density%>%select(,c(stratum_1, ev_density))

density2%>% write_csv('aou_density.csv')


#aft_1mnt<-read_csv('raw/all_hr_delta_1mnth_aft.csv')
cocnepts<-aou_concept3%>%select(,c('concept_id', 'concept_name'))


for(a in seq(1, length(read), 1)) { 
  
  current<-data.frame(read[a])
  raw1<-left_join(current, cocnepts, by=c('condition_concept_id'= 'concept_id'))
  sum1<-raw1%>%group_by(condition_concept_id, concept_name)%>%summarize(pt_cnt=sum(n))
  raw1$conc<-raw1$n*raw1$above_avg_hr2
  mean0<-raw1%>%group_by(condition_concept_id, concept_name)%>%summarize(total_cond=sum(conc))
  
  mean1<-left_join(sum1, mean0, by ='condition_concept_id')
  mean1$mean<-mean1$total_cond/mean1$pt_cnt
  mean2<-mean1%>%select(,c('condition_concept_id', 'concept_name.x','pt_cnt', 'mean'))
  dist<-current %>% dcast(condition_concept_id ~ above_avg_hr2, sum, value.var="n")
  comb<-left_join(mean2, dist, by ='condition_concept_id' )
  
  
  full<-data.frame()
  for (i in seq(nrow(raw1))){
    rows<-as.numeric(raw1[i,3])
    
    df<-data.frame(matrix(raw1[i,1], nrow = rows, ncol = 1))
    df2<-data.frame(matrix(raw1[i,2], nrow = rows, ncol = 1))
    df3<-data.frame(cbind(df, df2))
    full<-bind_rows(full,df3)
    
  }
  
  colnames(full)<-c('condition_concept_id', 'hr')
  
  #col1<-data.frame(full$condition_concept_id)
  #col2<-data.frame(full$hr)
  
  
  
  
  #full2<-t(cbind(col1,col2))
  #full2<-full2%>%filter(hr!='NA')
  #full2$hr<-as.numeric(full2$hr)
  #mean11<-full2%>%group_by(condition_concept_id)%>%summarize(mean_change=mean(hr))
  #full%>%write_csv('fill2_test.csv')
  
  
  
  
  
  #col1<-data.frame(t(col1))
  #col2<-data.frame(t(col2))
  
  #full2<-cbind(col1, col2)
  
  median11<-full%>%group_by(condition_concept_id)%>%summarize(median_change=median(hr), sd=sd(hr))
  
  
  comb<-left_join(mean11, median11, by ='condition_concept_id')
  comb2<-left_join(sum1, comb, by ='condition_concept_id')%>%left_join(density2, by=c('condition_concept_id'='stratum_1'))%>%left_join( dist, by ='condition_concept_id')
  comb3<-comb2%>%filter(pt_cnt>19)
  #comb2%>% write_csv('all_all_metrics.csv')
  
  files3<-files2[a]
  comb3%>% write_csv(paste0(files3, '_extract.csv'))