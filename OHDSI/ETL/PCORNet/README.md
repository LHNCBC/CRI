# ETL code to transform PCORNEt into OMOP 

see file 01-R-based-ETL.R (stage 1) and file 02-SQL-updates.sql (stage 2)


# Notes
- see comments in the file, each OMOP table is in a separate section
- assumes data that fits into memory, works with data.frames (stage 1)
- in stage 2, the code uses a set of SQL update statements to modify the data 
