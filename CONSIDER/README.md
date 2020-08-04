# CONSIDER statement 

CONSolidated REcommendations for sharing Individual participant Data

The acronym is loosely derived from letters contained in the title: CONSolidated REcommendations for sharing Individual participant Data). Letters E and R are re-ordered to create a more memorable acronym.


# Content
This has documents that provides a checklist and a set of recommendations to guide principal investigators (PIs), study team members and data sharing platform representatives in optimal sharing of individual participant data (IPD) from human clinical studies. We use the term study to include interventional trials and observational studies.

# Categories of recommendations

Recommendations are structured by several domains. Checklist below represents a brief summary of recommendations. The details link leads to elaborated descriptions of each item.

- Data Format
- Data Sharing
- Study Design
- Case Report Forms
- Data Dictionary
- Data De-identification
- Choice of a Data Sharing Platform

# Examples

The document aims to provide examples (with URL links) to studies that demostrate a positive examples. In some cases, examples of challenges are also included.

# Views

- Full: the full checklist with detailed descirptions, and positive and challenging examples when available
- Brief: List of checklist items

# Feedback 
We welcome feedback to any checklist item at craig.mayer2 ‘at’ nih.gov


# List of recommendations 

```
1	Data Format
1.1	Share person table in CDISC or OMOP format
1.2	For improved integration of research and routine healthcare data (e.g., from Electronic Health Record system or healthcare billing data), group data and data elements into relevant data domains (e.g., medication history, laboratory results history, medical procedure history)
1.3	Use consistent relative time (start counting at day 1, not 0)
1.4	Utilize ClinicalTrials.gov fields for uploading study protocol, empty case report forms, statistical analysis plan and study URL link
1.5	Provide basic summary results using results registry component of Clinicaltrials.gov
1.6	Share Case Report Forms in non-PDF, machine-readable format.
1.7	If you considered formally defined research Common Data Elements at study design (more common for studies initiated after 2015), provide a spreadsheet file that lists all CDEs utilized by your study. Include unique CDE identifiers (e.g., PhenX VariableID).
1.8	Use data formats that can be natively loaded (without add-ons) into multiple statistical platforms
2	Data Sharing
2.1	Use ClinicalTrials.gov registry to document your study
2.2	Do not limit study metadata to the legally required elements. Also populate optional elements (such as data sharing metadata)
2.3	On ClinicalTrials.gov, if you answer Yes to share_ipd_data, do not leave the data_sharing_plan text empty
2.4	If Individual Participant Data is shared on a data sharing platform, update the ClinicalTrials.gov record with the URL link to the data.
3	Study design
3.1	At study design time and when resources allow, adopt previously defined applicable common data elements
4	Case Report Forms
4.1	Share all Case Report Forms used in a study
5	Data dictionary
5.1	Provide data dictionary documentation separate from de-identified individual participant data. Since it contains no participant level data, do not require local ethical approval as a condition of releasing the data dictionary (avoid a requestwall for data dictionary).
5.2	Share a data dictionary as soon as possible. Do not wait until the data collection is complete.
5.3	Provide data dictionary in a single, machine-readable file.
5.4	For each data element, provide a data type (such as numeric, date, string, categorical)
5.5	For categorical data elements, provide a list of permissible values and distinguish when numerical code or string code is a code for a permissible value (versus actual number or string)
5.6	Link data elements (including individual permissible values) to applicable routine healthcare terminologies or research common data elements
5.7	Provide complete data dictionary (all elements in data are listed in a dictionary) and all types of applicable dictionaries (date elements, forms [or groupings], and permissible values)
5.8	Include sufficient description for data elements
5.9	Use identifiers (unique where applicable) for data element, forms and permissible values.
6	Data de-identification
6.1	Provide data de-identification notes
7	Choice of a Data Sharing platform
7.1	Choose a platform that allows download of all studies available on the platform
7.2	Choose a platform that supports batch request (ability to request multiple studies with one request)
```





