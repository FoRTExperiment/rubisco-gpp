# Function to read eddy covariance data and return a formatted dataframe
# Created November 2020 | Stephanie Pennington

library(readr)
library(lubridate)

# Function should read in a file path (character) for a Fluxnet site and return a dataframe with timestamp and GPP

read_gpp <- function(file) {

  x <- read_csv(file)
  x %>% 
    select(TIMESTAMP, GPP_NT_VUT_50) %>% 
    rename(Timestamp = TIMESTAMP, GPP = GPP_NT_VUT_50) %>% #im not sure which gpp to use right now, so this is a placeholder
    mutate(Timestamp = as_date(as.character(Timestamp), format = "%Y%m%d"))
  
}
