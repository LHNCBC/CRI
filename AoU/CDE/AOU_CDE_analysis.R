#Load required libraries
library(dplyr)
library(tidyverse)
library(stringr)
library(readr)

#Load concept and workbench extract and combine
aou_concept <- readRDS("aou_concept2.rds")
achillies0 <- readRDS("achilles2_27SEP21.rds")
achillies2<-left_join(achillies0 , aou_concept,by=c('stratum_1'='concept_id'))
achillies3<-left_join(achillies2 , aou_concept,by=c('stratum_2'='concept_id'))
achillies4<- achillies0 %>% select(,c(1:6,10,12,15, 19,21,24))

#Filter by wanted analysis type
## cnt: Element level
## cnt2: valu level
achillies4_cnt<-achillies4%>%filter(a_type=='cnt')
achillies4_cnt2<-achillies4%>%filter(a_type=='cnt2')

#Combine to have value and test counts in same rows
achillies5<-left_join(achillies4_cnt2, achillies4_cnt, by =c('a_table', "stratum_1"))

#Remove source value columns; JUst leaving mapped values
achillies6<-achillies5%>%filter(!grepl("source",a_column.y))
achillies7<-achillies6%>%filter(!grepl("type",a_column.y))

# Proportion of value count to test count
achillies7$proportion_of_values<-achillies7$count_value.x/achillies7$count_value.y


# Repeat with participant counts (instead of event count)
achillies4_cntpt<-achillies4%>%filter(a_type=='cntpt')
achillies4_cnt2pt<-achillies4%>%filter(a_type=='cnt2pt')
achillies5pt<-left_join(achillies4_cnt2pt, achillies4_cntpt, by =c('a_table', "stratum_1"))
achillies6pt<-achillies5pt%>%filter(!grepl("source",a_column.y))
achillies7pt<-achillies6pt%>%filter(!grepl("type",a_column.y))
achillies7pt$proportion_of_values<-achillies7pt$count_value.x/achillies7pt$count_value.y

#Combine event counts with participant counts
achillies8<-left_join(achillies7,achillies7pt, by=c('a_table', 'stratum_1', 'stratum_2.x') )



# Limit to relevant columns and easy to understand names
achillies9<-achillies8%>%select(,c(1:2,5:6,10, 12,15, 16,18,21, 4,24, 41,44, 62, 79) )
colnames(achillies9)<-c('a_table', 'a_column', 
                        'test_concept_id', 'value_conceppt_id', 'test_concept_name', 'test_vocabulary_id', 'test_code',
                        'value_concept_name', 'value_vocabulary_id', 'value_code',
                        'count_value', 'count_test','percent', 'pt_count_value', 'pt_count_test','pt_percent')


#Total participants for proportion of paticipants for each element
person<-achillies4%>%filter(a_table=='person')
patients<-person$count_value
achillies9$per_total_pt<-achillies9$pt_count_test/patients

#Comparison of element and value terminologies
achillies9$vocab_equivalent<-achillies9$test_vocabulary_id==achillies9$value_vocabulary_id



# Distinct DE  counts by terminology  by table
distinct_concepts<-achillies4_cnt%>%group_by(a_table,a_column,vocabulary_id.x)%>%count()
distinct_concepts2<-distinct_concepts%>% filter(a_table=='measurement'|a_table=='observation')
distinct_concepts3<-distinct_concepts2%>% filter(a_column=='measurement_concept_id'|a_column=='observation_concept_id')
colnames(distinct_concepts3)<-c('Table', 'column', 'Vocabulary', 'Distinct_test_concepts')

# Total elements in tables
tbl_distinct_concepts<-achillies4_cnt%>%group_by(a_table,a_column)%>%count()
distinct_values<-achillies4_cnt2%>%group_by(stratum_2)%>%count()


# Elements  by terminology
distinct_concepts4<-left_join(distinct_concepts3, tbl_distinct_concepts, by =c('Table'='a_table','column'='a_column'))
distinct_concepts4$DE_percentage<-distinct_concepts4$Distinct_test_concepts/distinct_concepts4$n
colnames(distinct_concepts4)[5]<-'tbl_DEs_cnt'
distinct_concepts5<-distinct_concepts4[with(distinct_concepts4, order(Table, -DE_percentage)),]



#S1-Element overview
pvs_de<-achillies9%>%group_by(a_table, test_concept_id)%>%count()
vocab_pvs_de<-achillies9%>%group_by(a_table, test_concept_id, value_vocabulary_id)%>%count()
vocab_pvs_de2<-vocab_pvs_de%>%group_by(a_table, test_concept_id)%>%count()
vocab_pvs_de3<-left_join(vocab_pvs_de2,aou_concept, by=c('test_concept_id' = 'concept_id') )%>%select(1:4, 6)
colnames(vocab_pvs_de3)<-c('Table', 'test_concept_id', 'PV_vocabs_used', 'test_name', 'test_vocab')

#Value terminologies
full_pvs<-left_join(vocab_pvs_de3,pvs_de, by =c('test_concept_id' ,'Table'='a_table'))
colnames(full_pvs)[6]<-'Distinct_PVs'
colnames(full_pvs)[3]<-'PV_vocabs_used'
vocab_pvs_de<-vocab_pvs_de%>%arrange(-n)

pv_vocabs_coll<-aggregate(vocab_pvs_de, list(vocab_pvs_de$a_table,vocab_pvs_de$test_concept_id), paste, collapse="|")
full_pvs2<-left_join(full_pvs, pv_vocabs_coll, by=c('Table'='Group.1', 'test_concept_id'='Group.2'))%>%select(,c(1:6,9,10))
full_pvs2$pv_de_vocab_match<-full_pvs2$test_vocab==full_pvs2$value_vocabulary_id
colnames(full_pvs2)[9]<-'DEs_by_vocab'
test_level<-achillies9 [!duplicated(achillies9[c('a_table','a_column', 'test_concept_id')]),]
full_pvs3<-left_join(full_pvs2, test_level, by=c('Table'='a_table', 'test_concept_id'))%>%select(,c(1:2, 4,5, 14,19, 22, 24, 3,6) )
                                                                                                   
full_pvs3<-full_pvs3%>%arrange(-per_total_pt)

#Aff AOU dictionary to find CRF elements and originating CRF
dict<-read_csv('AOU_dictionary3.csv')
s1_0<-left_join(full_pvs3, dict, by =c("test_concept_id"='Question'))



achillies4_1<- achillies4 %>% select(,c(1:6,10,12,15, 19,21,24,9))

#Only CRF elements
CRF<-achillies4_1%>%filter(src=='PPI/PM')
s1_crf<-left_join(s1_0,CRF, by =c('Table'='a_table', 'test_concept_id'='stratum_1'))%>%select(,c(1:18))
colnames(s1_crf)[16]<-'CRF_flag'
s1_crf<- within(s1_crf, CRF_flag[CRF_flag != 'NA'] <- '1')
s1_crf2<-s1_crf [!duplicated(s1_crf[c('Table','test_concept_id')]),]
s1_crf2$CRF_proportion<-s1_crf2$count_value/s1_crf2$count_test
s1_crf3<-s1_crf2%>%select(,c(1:16,19))

#Add LOINC dictionary to find intiative CDEs 
loinc<-read_csv('Loinc.csv')
s1_loinc<-left_join(s1_crf3, loinc, by =c("test_code"='LOINC_NUM'))%>%select(,c(1:17,23))


pv_counts<-achillies9%>%filter(percent>0.01)%>%arrange(-count_value)
pv_counts$percent<-round(pv_counts$percent,digits=3)
pv_counts2<-aggregate(pv_counts, list(pv_counts$a_table,pv_counts$test_concept_id), paste, collapse="|")
pv_counts3<-pv_counts2%>%select(,c(1:2, 6,10, 15))
s1_pv<-left_join(s1_loinc, pv_counts3, by =c('Table'='Group.1', 'test_concept_id'='Group.2') )


# Filter for privacy policy compliance
S1<-s1_pv%>%filter(pt_count_test>19)

#Write CRF element results
S1 %>% write_xlsx('aou12/S1_AOU_CRF_elements.xlsx')

CRF2<-CRF%>%select(,c(1:2, 5))
s2_crf<-left_join(achillies9,CRF2, by =c('a_table', 'test_concept_id'='stratum_1'))%>%select(,c(1:19))
colnames(s2_crf)[19]<-'CRF_flag'
s2_crf<- within(s2_crf, CRF_flag[CRF_flag != 'NA'] <- '1')
s2_0<-left_join(s2_crf, dict, by =c("test_concept_id"='Question'))

s2_00<-s2_0 [!duplicated(s2_0[c('a_table', 'test_concept_id','value_conceppt_id')]),]

S2_01<-s2_00%>%arrange(-count_value)
S2<-achillies9%>%filter(pt_count_value>19 )
S2_1<-S2%>%arrange(-pt_count_test)
S2_1 %>% write_xlsx('aou12/S2_AOU_CRF_values.xlsx')



#Only EHR elements
EHR<-achillies4_1%>%filter(src!='PPI/PM')
s1_ehr<-left_join(s1_0,EHR, by =c('Table'='a_table', 'test_concept_id'='stratum_1'))%>%select(,c(1:18))
s1_ehr2<-s1_crf [!duplicated(s1_ehr[c('Table','test_concept_id')]),]
s1_ehr3<-s1_ehr2%>%select(,c(1:16,19))

#Add LOINC dictionary to find intiative CDEs 
loinc<-read_csv('Loinc.csv')
s1_loinc<-left_join(s1_ehr3, loinc, by =c("test_code"='LOINC_NUM'))%>%select(,c(1:17,23))


pv_counts<-achillies9%>%filter(percent>0.01)%>%arrange(-count_value)
pv_counts$percent<-round(pv_counts$percent,digits=3)
pv_counts2<-aggregate(pv_counts, list(pv_counts$a_table,pv_counts$test_concept_id), paste, collapse="|")
pv_counts3<-pv_counts2%>%select(,c(1:2, 6,10, 15))
s1_pv<-left_join(s1_loinc, pv_counts3, by =c('Table'='Group.1', 'test_concept_id'='Group.2') )


# Filter for privacy policy compliance
S1<-s1_pv%>%filter(pt_count_test>19)

#Write CRF element results
S1 %>% write_xlsx('aou12/S1_AOU_EHR_element.xlsx')

EHR2<-EHR%>%select(,c(1:2, 5))
s2_ehr<-left_join(achillies9,EHR2, by =c('a_table', 'test_concept_id'='stratum_1'))%>%select(,c(1:19))

s2_00<-s2_ehr [!duplicated(s2_ehr[c('a_table', 'test_concept_id','value_conceppt_id')]),]

S2_01<-s2_00%>%arrange(-count_value)
S2<-achillies9%>%filter(pt_count_value>19 )
S2_1<-S2%>%arrange(-pt_count_test)
S2_1 %>% write_xlsx('aou12/S2_AOU_EHR_values.xlsx')



