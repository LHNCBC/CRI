# ETL code to transform PCORNEt into OMOP 

all code is in R file
There are 2 stages 
Stage 1 is strictly in R
Stage 2 (also subsumed in the R scrip) - we run update SQL statements

# Notes
- see comments in the file, each OMOP table is in a separate section
- assumes data that fits into memory, works with data.frames (stage 1)
- in stage 2, the code uses a set of SQL update statements to modify the data 
