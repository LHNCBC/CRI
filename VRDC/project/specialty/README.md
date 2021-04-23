# Specialty-diagnosis analysis

This repository contains supplemental files.
Article: Linking provider specialty and outpatient diagnoses in healthcare Medicare claims data: Data quality implications.   
Authors: Vojtech Huser, Nick D. Williams, Craig S. Mayer  
Affiliation: National Library of Medicine, Lister Hill Center for Biomedical Communications  


Files are named in style that mirrors the tables in the manuscript.  

Comments on selected individual files are below. They are organized by chapter heading for the file name.  

# File st4 - supplemental data for table 4

This file is most relevant to researchers authoring an executable phenotype. For each code used in their phenotype, they can review the "operating characteristics" in terms of which specialties contribute this diagnosis. The top contriuting diagnoses should not be missing from the dataset.  
For example, for diagnosis L40.0 Psoriasis vulgaris, the most important specialty is dermatology - 78.99% of events with that diagnoses come from a dermatologist.  
Further examples are in article where we describe multi specialty diagnoses and highly specialty specific diagnoses. L40.0 would meet the definion of highly specialty-specific diagnosis.  
This file is not as relevant for data quality assesment of a dataset. It is too detailed for that. For such assesment, simply the aggregation on specialty level or diagnosis level allows simpler comparison of given dataset to some reference data.  

## Example view of one diagnosis
![image](https://user-images.githubusercontent.com/7526119/115904897-574ec700-a433-11eb-8476-46ca80934717.png)

# Network folder
Two files (one for notes and one for edges of the network) are provided. These can be used by software (e.g., Cytoscape) to import the specialty-diagnosis data as a network. On request, one of the authors (NW) can provide results of several standard network analysis metrics.
