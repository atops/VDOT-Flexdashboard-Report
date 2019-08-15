library(readxl)
library(dplyr)
library(purrr)
library(feather)

tmc_corridors_fn <- "~/Code/VDOT/VDOT-Flexdashboard-Report/TMC_Corridors_2019-05-07.xlsx"

excel_sheets(tmc_corridors_fn) %>% 
    set_names() %>% 
    map(read_excel, path = tmc_corridors_fn) %>% 
    bind_rows() %>%
    write_feather("tmc_routes_.feather")

