# This is an OLD VERSION
For the most recent version visit: 
[r4AOU Full Github page](https://github.com/cmayer2/r4aou)

# r4aou
R package for tasks inside All of Us Workbench  (AllofUs or AOU)


# Install package

    library(devtools)
    install_github('cmayer2/r4aou')
    
# Sample use

    aou_tables()

# Initial code for notebook

      library(purrr);library(tidyverse);library(magrittr);library(devtools);library(stringr);library(bigrquery);library(glue)
      options(scipen=999) #disable scientific notation

      dataset = Sys.getenv('WORKSPACE_CDR')
      dataset
      cdmDatabaseSchema <- dataset

      billing=Sys.getenv('GOOGLE_PROJECT')
      billing


    
    

