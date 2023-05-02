library(dplyr)
library(tidyverse)
library(readr)
library(reshape2)
library(data.table)


# Read file and find questioniarre fields
df<-read.table('ukb670628.tab', header=TRUE, sep='\t')
}
names<-data.frame(names(ukbb))

 surv<- covid<-ukbb%>%select(,c(1, 13682:14078))


#############################################################
#Combine all quetiniarre data elements into long format
melt<- melt(surv, id.vars="f.eid", value.name = 'source_value')%>%filter(source_value!='NA')

#convert column names into field name
colnames(melt)[1]<-'eid'
colnames(melt)[2]<-'field'
melt$field<-as.character(melt$field)
melt$field = substr(melt$field,3,nchar(melt$field)-4)
#Read Field table
field <- read.delim("field.txt")%>%select(,c(1,2,14,15))
field$field_id<-as.character(field$field_id)

melt4<-left_join(melt, field, by =c('field'='field_id'))

#Read value files
end1 <- read.delim("//lhcdevfiler/OMOP/ukbiobank/esimpint.txt")
end2<- read.delim("//lhcdevfiler/OMOP/ukbiobank/esimpstring.txt")
end1$value<-as.character(end1$value)

ehierint<-bind_rows(end1,end2)

melt4<-left_join(melt4,ehierint, by =c('encoding_id', 'source_value'='value'))

# Caluclate metrics for General population
q_all<-melt4%>%group_by(eid, field, title)%>%count()
q_all_pt<-q_all%>%group_by( field,title)%>%count()
q_all_ev<-melt4%>%group_by( field, title)%>%count()


a_all<-melt4%>%group_by(eid, field, source_value, title, meaning)%>%count()
a_all_pt<-a_all%>%group_by( field, source_value, title, meaning)%>%count()
a_all_ev<-melt4%>%group_by( field, source_value, title, meaning)%>%count()

a_all2<-left_join(a_all_ev, a_all_pt, by =c( 'field', 'source_value', 'title', 'meaning'))
q_all2<-left_join(q_all_ev, q_all_pt, by =c( 'field', 'title'))


qa_all<-left_join(q_all2, a_all2, by =c( 'field', 'title'))
colnames(qa_all)<-c('question_id','question', 'q_ev', 'q_pt', 'answer_id', 'answer', 'a_ev', 'a_pt')
qa_all$ev_prop<-qa_all$a_ev/qa_all$q_ev
qa_all$pt_prop<-qa_all$a_pt/qa_all$q_pt

qa_all%>%write_csv('ukbb_covid_qa_all.csv')

#Find vaccinated and unvaccintaed populations
vaccinated<-melt4%>%filter(field==27983 & source_value==1)
unvaccinated<-melt4%>%filter(field==27983 & source_value==0)

vacc_pep<-data.frame(unique(vaccinated$eid))
colnames(vacc_pep)[1]<-'eid'
vacc_df<-left_join(vacc_pep, melt4, by ='eid')


unvacc_pep<-data.frame(unique(unvaccinated$eid))
colnames(unvacc_pep)[1]<-'eid'
unvacc_df<-left_join(unvacc_pep, melt4, by ='eid')

# Caluclate metrics for vccinated and unvaccinated

q_all<-vacc_df%>%group_by(eid, field, title)%>%count()
q_all_pt<-q_all%>%group_by( field,title)%>%count()
q_all_ev<-vacc_df%>%group_by( field, title)%>%count()


a_all<-vacc_df%>%group_by(eid, field, source_value, title, meaning)%>%count()
a_all_pt<-a_all%>%group_by( field, source_value, title, meaning)%>%count()
a_all_ev<-vacc_df%>%group_by( field, source_value, title, meaning)%>%count()

a_all2<-left_join(a_all_ev, a_all_pt, by =c( 'field', 'source_value', 'title', 'meaning'))
q_all2<-left_join(q_all_ev, q_all_pt, by =c( 'field', 'title'))


qa_all<-left_join(q_all2, a_all2, by =c( 'field', 'title'))
colnames(qa_all)<-c('question_id','question', 'q_ev', 'q_pt', 'answer_id', 'answer', 'a_ev', 'a_pt')
qa_all$ev_prop<-qa_all$a_ev/qa_all$q_ev
qa_all$pt_prop<-qa_all$a_pt/qa_all$q_pt

qa_all%>%write_csv('ukbb_covid_qa_vacc.csv')

q_all<-unvacc_df%>%group_by(eid, field, title)%>%count()
q_all_pt<-q_all%>%group_by( field,title)%>%count()
q_all_ev<-unvacc_df%>%group_by( field, title)%>%count()


a_all<-unvacc_df%>%group_by(eid, field, source_value, title, meaning)%>%count()
a_all_pt<-a_all%>%group_by( field, source_value, title, meaning)%>%count()
a_all_ev<-unvacc_df%>%group_by( field, source_value, title, meaning)%>%count()

a_all2<-left_join(a_all_ev, a_all_pt, by =c( 'field', 'source_value', 'title', 'meaning'))
q_all2<-left_join(q_all_ev, q_all_pt, by =c( 'field', 'title'))


qa_all<-left_join(q_all2, a_all2, by =c( 'field', 'title'))
colnames(qa_all)<-c('question_id','question', 'q_ev', 'q_pt', 'answer_id', 'answer', 'a_ev', 'a_pt')
qa_all$ev_prop<-qa_all$a_ev/qa_all$q_ev
qa_all$pt_prop<-qa_all$a_pt/qa_all$q_pt

qa_all%>%write_csv('ukbb_covid_qa_unvacc.csv')

#Find COVID population

#Testing
eng <- read.delim("//lhcdevfiler/OMOP/ukbiobank/covid19_result_england.txt")%>%filter(result==1)
scot <- read.delim("//lhcdevfiler/OMOP/ukbiobank/covid19_result_scotland.txt")%>%filter(result==1)
wales <- read.delim("//lhcdevfiler/OMOP/ukbiobank/covid19_result_wales.txt")%>%filter(result==1)
eng$laboratory<-as.character(eng$laboratory)
wales$laboratory<-as.character(wales$laboratory)



test<-bind_rows(eng, scot, wales)
test$date<-as.Date(test$specdate, '%d/%m/%Y')

#Self Declared
self<-melt4%>%filter(field==28166 |field==28031)%>%filter(source_value!='NA')

self_diff<-anti_join(self, test, by ='eid')

#Diagnosis
covid_dx<-read_rds('ukbb_condition_occurrence.rds')%>%filter(condition_concept_id==37311061)

#compare attribution type
self_diff<-anti_join(self, test, by ='eid')
dx_diff<-anti_join(covid_dx, test, by =c('person_id'='eid'))
all_diff<-anti_join(self_diff, dx_diff, by =c('eid'='person_id'))



self$date<-as.Date(self$source_value, '%Y-%m-%d')
colnames(covid_dx)<-c('eid', 'date')
test<-bind_rows(test, self,covid_dx)



covid_date<-test%>%group_by(eid, date)%>%count()
covid_date<-covid_date%>%group_by( date)%>%count()


covid_date%>%write_csv('ukbb_covid_date.csv')
covid_date<-read_csv('ukbb_covid_date.csv')
#combine for all covid cohort
test<-bind_rows(test, self,covid_dx)

#Plot of cases over time
ggplot() + 
  geom_path(data = covid_date, aes(x = date, y = n)) +
  xlab('Date') +
  ylab('Participant Count')+
  scale_x_discrete(breaks = covid_date$date[seq(1, length(covid_date$date), by = 30)])


#filter by 2020 to get exposure questioniare cohort
test2<-test%>%filter(year(date)==2020)
cov_pep<-data.frame(unique(test2$eid))
colnames(cov_pep)[1]<-'eid'
cov_df<-merge(cov_pep, melt4, by ='eid')

length(unique(cov_df$eid))
#Exposure for COVID cohort
q_all<-cov_df%>%group_by(eid, field, title)%>%count()
q_all_pt<-q_all%>%group_by( field,title)%>%count()
q_all_ev<-cov_df%>%group_by( field, title)%>%count()


a_all<-cov_df%>%group_by(eid, field, source_value, title, meaning)%>%count()
a_all_pt<-a_all%>%group_by( field, source_value, title, meaning)%>%count()
a_all_ev<-cov_df%>%group_by( field, source_value, title, meaning)%>%count()

a_all2<-left_join(a_all_ev, a_all_pt, by =c( 'field', 'source_value', 'title', 'meaning'))
q_all2<-left_join(q_all_ev, q_all_pt, by =c( 'field', 'title'))


qa_all<-left_join(q_all2, a_all2, by =c( 'field', 'title'))
colnames(qa_all)<-c('question_id','question', 'q_ev', 'q_pt', 'answer_id', 'answer', 'a_ev', 'a_pt')
qa_all$ev_prop<-qa_all$a_ev/qa_all$q_ev
qa_all$pt_prop<-qa_all$a_pt/qa_all$q_pt

qa_all%>%write_csv('ukbb_covid_qa_cov.csv')

#Exposure for non-covid
nocov_df<-anti_join( melt4, cov_pep, by ='eid')
q_all<-nocov_df%>%group_by(eid, field, title)%>%count()
q_all_pt<-q_all%>%group_by( field,title)%>%count()
q_all_ev<-nocov_df%>%group_by( field, title)%>%count()


a_all<-nocov_df%>%group_by(eid, field, source_value, title, meaning)%>%count()
a_all_pt<-a_all%>%group_by( field, source_value, title, meaning)%>%count()
a_all_ev<-nocov_df%>%group_by( field, source_value, title, meaning)%>%count()

a_all2<-left_join(a_all_ev, a_all_pt, by =c( 'field', 'source_value', 'title', 'meaning'))
q_all2<-left_join(q_all_ev, q_all_pt, by =c( 'field', 'title'))


qa_all<-left_join(q_all2, a_all2, by =c( 'field', 'title'))
colnames(qa_all)<-c('question_id','question', 'q_ev', 'q_pt', 'answer_id', 'answer', 'a_ev', 'a_pt')
qa_all$ev_prop<-qa_all$a_ev/qa_all$q_ev
qa_all$pt_prop<-qa_all$a_pt/qa_all$q_pt

qa_all%>%write_csv('ukbb_covid_qa_nocov.csv')



#Comparison of covid vs non-covid
cov_comp<-merge(covid, nocovid, by =c('question_id', 'question', 'answer_id', 'answer'),all=TRUE)
cov_comp$ev_diff<-cov_comp$ev_prop.x-cov_comp$ev_prop.y
cov_comp$pt_diff<-cov_comp$pt_prop.x-cov_comp$pt_prop.y


#comparison of vaccinated and unvaccinated
vacc_comp<-merge(vacc, novacc, by =c('question_id', 'question', 'answer_id', 'answer'), all=TRUE)
vacc_comp$ev_diff<-vacc_comp$ev_prop.x-vacc_comp$ev_prop.y
vacc_comp$pt_diff<-vacc_comp$pt_prop.x-vacc_comp$pt_prop.y

#Statstics of comparison=================================================
cov_comp$lower_CI_ev <- cov_comp$ev_diff-(qnorm(.975)*(sqrt(abs(cov_comp$ev_dif)*(1-abs(cov_comp$ev_diff))/((cov_comp$q_ev.x+cov_comp$q_ev.y)/2))))
cov_comp$upper_CI_ev <- cov_comp$ev_diff+(qnorm(.975)*(sqrt(abs(cov_comp$ev_diff)*(1-abs(cov_comp$ev_diff))/((cov_comp$q_ev.x+cov_comp$q_ev.y)/2))))
cov_comp$lower_CI_pt <- cov_comp$pt_diff-(qnorm(.975)*(sqrt(abs(cov_comp$pt_diff)*(1-abs(cov_comp$pt_diff))/((cov_comp$q_pt.x+cov_comp$q_pt.y)/2))))
cov_comp$upper_CI_pt <- cov_comp$pt_diff+(qnorm(.975)*(sqrt(abs(cov_comp$pt_diff)*(1-abs(cov_comp$pt_diff))/((cov_comp$q_pt.x+cov_comp$q_pt.y)/2))))


vacc_comp$lower_CI_ev <- vacc_comp$ev_diff-(qnorm(.975)*(sqrt(abs(vacc_comp$ev_diff)*(1-abs(vacc_comp$ev_diff))/((vacc_comp$q_ev.x+vacc_comp$q_ev.y)/2))))
vacc_comp$upper_CI_ev <- vacc_comp$ev_diff+(qnorm(.975)*(sqrt(abs(vacc_comp$ev_diff)*(1-abs(vacc_comp$ev_diff))/((vacc_comp$q_ev.x+vacc_comp$q_ev.y)/2))))
vacc_comp$lower_CI_pt <- vacc_comp$pt_diff-(qnorm(.975)*(sqrt(abs(vacc_comp$pt_diff)*(1-abs(vacc_comp$pt_diff))/((vacc_comp$q_pt.x+vacc_comp$q_pt.y)/2))))
vacc_comp$upper_CI_pt <- vacc_comp$pt_diff+(qnorm(.975)*(sqrt(abs(vacc_comp$pt_diff)*(1-abs(vacc_comp$pt_diff))/((vacc_comp$q_pt.x+vacc_comp$q_pt.y)/2))))


vacc_comp$p_value_ev<-abs(vacc_comp$ev_diff)/(sqrt(((vacc_comp$a_ev.x^2)/(vacc_comp$q_ev.x))+
                                                     ((vacc_comp$a_ev.y^2)/(vacc_comp$q_ev.y))))

vacc_comp$p_value_pt<-abs(vacc_comp$pt_diff)/(sqrt(((vacc_comp$a_pt.x^2)/(vacc_comp$q_pt.x))+
                                                     ((vacc_comp$a_pt.y^2)/(vacc_comp$q_pt.y))))

cov_comp$p_value_ev<-abs(cov_comp$ev_diff)/(sqrt(((cov_comp$a_ev.x^2)/(cov_comp$q_ev.x))+
                                                   ((cov_comp$a_ev.y^2)/(cov_comp$q_ev.y))))

cov_comp$p_value_pt<-abs(cov_comp$pt_diff)/(sqrt(((cov_comp$a_pt.x^2)/(cov_comp$q_pt.x))+
                                                   ((cov_comp$a_pt.y^2)/(cov_comp$q_pt.y))))



cov_comp%>%write_csv('ukbb_covid_survey_comp.csv')
vacc_comp%>%write_csv('ukbb_vacc_survey_comp.csv')


#Time Trends=================================
library(stringr)
june<-melt4%>%filter(field!=28058)%>%filter(grepl('June', title))
july<-melt4%>%filter(grepl('July', title))
oct<-melt4%>%filter(grepl('Oct', title))
dec<-melt4%>%filter(grepl('Dec', title))
june_q<-data.frame(unique(june$field))
june_q$id<-seq.int(nrow(june_q))

oct_q<-data.frame(unique(oct$field))
oct_q$id<-seq.int(nrow(oct_q))


july_q<-data.frame(unique(july$field))
july_q$id<-seq.int(nrow(july_q))

dec_q<-data.frame(unique(dec$field))
dec_q$id<-seq.int(nrow(dec_q))


comb<-left_join(june_q, july_q, by='id')%>%left_join(oct_q, by ='id')%>%left_join(dec_q, by ='id')

# Combining question id for equvilence accross Time
colnames(comb)<-c('june', 'id', 'july', 'oct', 'dec')
comb_df<-left_join(june, comb, by =c('field'='june'))
comb_df2<-merge(comb_df,july, by.x =c('eid', 'july'),by.y=c('eid','field'), all=TRUE)%>%
  merge(oct, by.x =c('eid', 'oct'),by.y=c('eid','field'), all=TRUE)%>%
  merge(dec, by.x =c('eid', 'dec'),by.y=c('eid','field'), all=TRUE)

comb_df2%>%write_csv('ukbb_covid_time_change2.csv')

comb_df2<-read_csv('ukbb_covid_time_change2.csv')
time<-comb_df2%>%select(,c(1,7, 6,10, 13, 17, 19,23,25, 29))

colnames(time)<-c('eid', 'q', 'a_june', 'mean_june', 'a_july', 'mean_july', 'a_oct', 'mean_oct', 'a_dec', 'mean_dec')

# Comparing accross time frame
june<-time%>%group_by(q, a_june, mean_june)%>%count()
june_q<-june%>%group_by(q)%>%summarize(q_pt=sum(n))
june2<-left_join(june, june_q, by='q')
june2$pt_prop_june<-june2$n/june2$q_pt

july<-time%>%group_by(q, a_july, mean_july)%>%count()
july_q<-july%>%group_by(q)%>%summarize(q_pt=sum(n))
july2<-left_join(july, july_q, by='q')
july2$pt_prop_july<-july2$n/july2$q_pt

oct<-time%>%group_by(q, a_oct, mean_oct)%>%count()
oct_q<-oct%>%group_by(q)%>%summarize(q_pt=sum(n))
oct2<-left_join(oct, oct_q, by='q')
oct2$pt_prop_oct<-oct2$n/oct2$q_pt

dec<-time%>%group_by(q, a_dec, mean_dec)%>%count()
dec_q<-dec%>%group_by(q)%>%summarize(q_pt=sum(n))
dec2<-left_join(dec, dec_q, by='q')
dec2$pt_prop_dec<-dec2$n/dec2$q_pt


time_comp<-merge(june2, july2, by.x=c('q', 'a_june', 'mean_june'), by.y=c('q', 'a_july', 'mean_july'), all=TRUE)%>%
  merge( oct2, by.x=c('q', 'a_june', 'mean_june'), by.y=c('q', 'a_oct', 'mean_oct'), all=TRUE)%>%
  merge( dec2, by.x=c('q', 'a_june', 'mean_june'), by.y=c('q', 'a_dec', 'mean_dec'), all=TRUE)

time_comp%>%write_csv('ukbb_covid_time_comp.csv')

time$june_july<-time$a_june==time$a_july
time$july_oct<-time$a_oct==time$a_july
time$oct_dec<-time$a_oct==time$a_dec
time$june_dec<-time$a_june==time$a_dec

# Time period comparison
june_july<-time%>%filter(june_july!='NA')%>%group_by(q, june_july)%>%count()
june_july_q<-june_july%>%group_by(q)%>%summarize(pt_cnt=sum(n))
june_july2<-left_join(june_july, june_july_q, by ='q')
june_july2$pt_prop<-june_july2$n/june_july2$pt_cnt

july_oct<-time%>%filter(july_oct!='NA')%>%group_by(q, july_oct)%>%count()
july_oct_q<-july_oct%>%group_by(q)%>%summarize(pt_cnt=sum(n))
july_oct2<-left_join(july_oct, july_oct_q, by ='q')
july_oct2$pt_prop<-july_oct2$n/july_oct2$pt_cnt


oct_dec<-time%>%filter(oct_dec!='NA')%>%group_by(q, oct_dec)%>%count()
oct_dec_q<-oct_dec%>%group_by(q)%>%summarize(pt_cnt=sum(n))
oct_dec2<-left_join(oct_dec, oct_dec_q, by ='q')
oct_dec2$pt_prop<-oct_dec2$n/oct_dec2$pt_cnt

time_change<-left_join(june_july2, july_oct2, by =c('q', 'june_july'='july_oct'))%>%left_join( oct_dec2, by =c('q', 'june_july'='oct_dec'))

time_change%>%write_csv('ukbb_covid_time_change.csv')

june_dec<-time%>%filter(june_dec!='NA')%>%group_by(q, june_dec)%>%count()
june_dec_q<-june_dec%>%group_by(q)%>%summarize(pt_cnt=sum(n))
june_dec2<-left_join(june_dec, june_dec_q, by ='q')
june_dec2$pt_prop<-june_dec2$n/june_dec2$pt_cnt



earliest_covid<-test %>%
  group_by(eid) %>%
  arrange(date) %>%
  slice(1L)
library(lubridate)
covid20<-earliest_covid%>%filter(year(date)==2020)

#Comparison around COVID dx
durquest<-merge(covid20, melt4, by ='eid')
durquest2<-durquest%>%group_by(eid, month=month(date))%>%count()
durquest3<-durquest2%>%group_by( month)%>%count()


time_cov<-merge(time, covid20, by ='eid')
time_cov1<-time_cov%>%filter(month(date)<7)
time_cov2<-time_cov%>%filter(month(date)>6 &date<'2020-9-16')
time_cov3<-time_cov%>%filter(month(date)<11 &date>'2020-9-15')
time_cov4<-time_cov%>%filter(month(date)>10)

include1<-time_cov2%>%select(,c(1:8))
colnames(include1)<-c('eid', 'q', 'a_before', 'mean_before', 'a_during', 'mean_during', 'a_after', 'mean_after')

include2<-time_cov3%>%select(,c(1:2, 5:10))
colnames(include2)<-c('eid', 'q', 'a_before', 'mean_before', 'a_during', 'mean_during', 'a_after', 'mean_after')

include<-bind_rows(include1, include2)

after<-time_cov1%>%select(,c(1:6))
colnames(after)<-c('eid', 'q', 'a_during', 'mean_during', 'a_after', 'mean_after')

before<-time_cov4%>%select(,c(1:2,7:10))
colnames(before)<-c('eid', 'q', 'a_before', 'mean_before', 'a_during', 'mean_during')
include_all<-bind_rows(include1, include2, before, after)
before_cnt<-include_all%>%group_by(q, a_before, mean_before)%>%count()
before_ev<-include_all%>%group_by(q)%>%count()
colnames(before_cnt)[4]<-'a_pt_cnt'
colnames(before_ev)[2]<-'q_pt_cnt'
before_comb<-left_join(before_cnt, before_ev, by ='q')
before_comb$pt_prop_before<-before_comb$a_pt_cnt/before_comb$q_pt_cnt


during_cnt<-include_all%>%group_by(q, a_during, mean_during)%>%count()
during_ev<-include_all%>%group_by(q)%>%count()
colnames(during_cnt)[4]<-'a_pt_cnt'
colnames(during_ev)[2]<-'q_pt_cnt'
during_comb<-left_join(during_cnt, during_ev, by ='q')
during_comb$pt_prop_during<-during_comb$a_pt_cnt/during_comb$q_pt_cnt


after_cnt<-include_all%>%group_by(q, a_after, mean_after)%>%count()
after_ev<-include_all%>%group_by(q)%>%count()
colnames(after_cnt)[4]<-'a_pt_cnt'
colnames(after_ev)[2]<-'q_pt_cnt'
after_comb<-left_join(after_cnt, after_ev, by ='q')
after_comb$pt_prop_after<-after_comb$a_pt_cnt/after_comb$q_pt_cnt

include_time_comb<-merge(before_comb, during_comb, by.x =c('q', 'a_before', 'mean_before'), by.y=c('q', 'a_during', 'mean_during'))%>%
  merge( after_comb, by.x =c('q', 'a_before', 'mean_before'), by.y=c('q', 'a_after', 'mean_after'))

include_time_comb%>%write_csv('ukbb_covid_dx_affect.csv')


include_all$before_during<-include_all$a_before==include_all$a_during
include_all$during_after<-include_all$a_after==include_all$a_during
include_all$before_after<-include_all$a_before==include_all$a_after

before_during<-include_all%>%filter(before_during!='NA')%>%group_by(q, before_during)%>%count()
before_during_q<-before_during%>%group_by(q)%>%summarize(pt_cnt=sum(n))
before_during2<-left_join(before_during, before_during_q, by ='q')
before_during2$pt_prop<-before_during2$n/before_during2$pt_cnt

during_after<-include_all%>%filter(during_after!='NA')%>%group_by(q, during_after)%>%count()
during_after_q<-during_after%>%group_by(q)%>%summarize(pt_cnt=sum(n))
during_after2<-left_join(during_after, during_after_q, by ='q')
during_after2$pt_prop<-during_after2$n/during_after2$pt_cnt


before_after<-include_all%>%filter(before_after!='NA')%>%group_by(q, before_after)%>%count()
before_after_q<-before_after%>%group_by(q)%>%summarize(pt_cnt=sum(n))
before_after2<-left_join(before_after, before_after_q, by ='q')
before_after2$pt_prop<-before_after2$n/before_after2$pt_cnt

include_all_change<-left_join(before_during2, during_after2, by =c('q', 'before_during'='during_after'))%>%left_join( before_after2, by =c('q', 'before_during'='before_after'))
include_all_change%>%write_csv('ukbb_covid_change.csv')


# comparison of covid to vaccine dates
vacc_date<-melt4%>%filter(field==27984)
vacc_cov<-left_join(test, vacc_date, by ='eid')
covid_unvacc<-vacc_cov%>%filter(is.na(source_value.y))

vacc_before<-vacc_cov%>%filter(date<source_value.y)
unvacc_cov<-merge(test, unvaccinated, by ='eid')
vacc_cov2<-merge(test, vaccinated, by ='eid')

unvacc_nocov<-anti_join( unvaccinated, test, by ='eid')
vacc_nocov2<-anti_join( vaccinated,test, by ='eid')


#symptoms==================================================
sym_date<-melt4%>%filter(field==28030& source_value>'2019-10-31')
sym<-melt4%>%filter(field>28010 & field<28030 & source_value!=0)
sym_date_wave1<-merge(sym_date, sym, by ='eid')

sym_date_wave1$sym_date<-as.Date(sym_date_wave1$source_value.x, '%Y-%m-%d')
sym_group<-sym_date_wave1%>%group_by(eid,title.y,month=month(sym_date), year=year(sym_date) )%>%count()
sym_group2<-sym_group%>%group_by(title.y,month, year )%>%count()%>%filter(n>9)
over_sym<-sym_group%>%group_by(title.y )%>%count()%>%arrange(-n)
sym_group2$date <- as.yearmon(paste(sym_group2$year, sym_group2$month), "%Y %m")

over_sym2<-over_sym[1:19,1]
library(zoo)
sym_group3<-merge(sym_group2, over_sym2,by ='title.y')%>%arrange(date)
ggplot()+ geom_path(data =sym_group3, aes(x =date, y=n, group = title.y, color=title.y))+ geom_line() +
  ylab('Particpant COunt')



sym_surv<-melt4%>%filter(field==28010 | field==28167)
sym_surv_compl<-merge(sym_surv, test, by ='eid')


sym7<-melt4%>%filter(field>28146 & field<28166)%>%filter(source_value!=0)


wave1_cov<-merge(test, sym_date_wave1, by ='eid')
wave1_cov$sym_date<-as.Date(wave1_cov$source_value.x, '%Y-%m-%d')
wave1_cov$days_diff<-wave1_cov$date-wave1_cov$sym_date
wave1_cov2<-wave1_cov%>%filter(source_value.y!=0)
wave1_cov2$week<-round(wave1_cov2$days_diff/7)
wave1_cov2$month<-round(wave1_cov2$days_diff/30)

weeks<-wave1_cov2%>%group_by(title.y, week)%>%count()
weeks%>%write_csv('ukbb_weekly_symptoms.csv')


wave1_cov<-merge(test, sym_date, by ='eid')%>%filter(year(date)==2020)
wave1_cov$sym_date<-as.Date(wave1_cov$source_value, '%Y-%m-%d')
wave1_cov$days_diff<-wave1_cov$date-wave1_cov$sym_date
wave1_cov2<-wave1_cov#%>%filter(source_value.y!=0)
wave1_cov2$week<-round(wave1_cov2$days_diff/7)
wave1_cov2$month<-round(wave1_cov2$days_diff/30)


weeks<-wave1_cov2%>%group_by( week)%>%count()
weeks%>%write_csv('ukbb_weekly_symptoms_agnostic.csv')



syms1<-merge(test, sym, by ='eid')%>%filter(source_value!=0)


syms1<-anti_join(sym7, test, by ='eid')%>%filter(source_value!=0)
syms2<-anti_join(sym, test, by ='eid')%>%filter(source_value!=0)

syms_comb<-bind_rows(syms1, syms2)
sym72<-merge(test, sym7, by ='eid')%>%filter(source_value!=0)


sym_count<-syms_comb%>%group_by(eid,title)%>%count()
sym_count2<-sym_count%>%group_by(title)%>%count()
pt_cnt<-length(unique(sym_count$eid))
sym_count2$pt_prop<-sym_count2$n/pt_cnt
mean(sym_count2$n)


# Living population during survey period
dbe<-death%>%filter(death_date<'2020-3-1')


anti<-melt4%>%filter(field==28000& source_value==1)
anti_date<-melt4%>%filter(field==28008)
anti2<-left_join(anti, anti_date, by='eid')



