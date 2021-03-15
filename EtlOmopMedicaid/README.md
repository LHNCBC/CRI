# ETL into OMOP Common Data Model (Medicaid data)

Landing page link: [https://lhncbc.github.io/CRI/EtlOmopMedicaid](https://lhncbc.github.io/CRI/EtlOmopMedicaid)  
Full repository link:[https://github.com/lhncbc/CRI/tree/master/EtlOmopMedicaid](https://github.com/lhncbc/CRI/tree/master/EtlOmopMedicaid)  

# Introduction
Common Data Models (CDMs) allow portability of analysis. In order to execute analyses that assume the OMOP representation of data, we convert Medicaid data into OMOP model.

The data dictionary of the TMSIS Analytic Files (TAF) data format is availabe at [https://www2.ccwdata.org/web/guest/data-dictionaries](https://www2.ccwdata.org/web/guest/data-dictionaries) (scroll down to section Medicaid).  
 - E.g., TAF Claims columns: [here](https://www2.ccwdata.org/documents/10280/19022436/record-layout-taf-claims.xlsx)  Values explanation:[here](https://www2.ccwdata.org/documents/10280/19022436/codebook-taf-claims.pdf)

For OMOP model, see here: [https://ohdsi.github.io/CommonDataModel](https://ohdsi.github.io/CommonDataModel)

## Duration
ETL: June 2021 - December 2022

# References
* [Full AMIA conference paper published by us](https://www.researchgate.net/publication/346632577_Data_Characterization_of_Medicaid_Legacy_and_New_Data_Formats_in_the_CMS_Virtual_Research_Data_Center) in 2021
