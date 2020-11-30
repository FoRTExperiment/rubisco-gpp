# Function to read eddy covariance data and return a formatted dataframe
# Created November 2020 | Stephanie Pennington

library(readr)
library(lubridate)
library(dplyr)

# Function should read in a folder path (character) for a Fluxnet site and return a dataframe with timestamp and GPP

read_gpp <- function(folder) {
  
  # Locate Fluxnet file in given folder path
  file_path <- list.files(path = folder, pattern = "FLUXNET", full.names = TRUE)
  
  x <- read_csv(file_path)
  x %>% 
    select(TIMESTAMP, GPP_NT_VUT_50) %>% #started with only timestamp and gpp, can add more variables later
    rename(Timestamp = TIMESTAMP, GPP = GPP_NT_VUT_50) %>% #im not sure which gpp to use right now, so this is a placeholder
    mutate(Timestamp = as_date(as.character(Timestamp), format = "%Y%m%d"))
  
}

# NIRv code commented out for now, until data format get standardized
# read_nirv <- function(folder) {
#   
#   file_path <- list.files(path = folder, pattern = "NIRv", full.names = TRUE)
#   
#   x <- read_csv(file_path)
#   x %>% 
#     select(X1, nirv) %>% 
#     rename(Timestamp = X1, NIRv = nirv) %>% 
#     mutate(Timestamp = as_date(Timestamp, format = "%Y%m%d"))
#   
# }

