
# Monthly_Report_Package_init.R

library(yaml)
library(glue)
library(future)


print(glue("{Sys.time()} Starting Package Script"))

source("Monthly_Report_Functions.R")

if (interactive()) {
    plan(multisession)
} else {
    plan(multicore)
}

usable_cores <- get_usable_cores()
doParallel::registerDoParallel(cores = usable_cores)


corridors <- s3read_using(
    function(x) get_corridors(x, filter_signals = TRUE),
    object = conf$corridors_filename_s3,
    bucket = conf$bucket
)

all_corridors <- s3read_using(
    function(x) get_corridors(x, filter_signals = FALSE),
    object = conf$corridors_filename_s3,
    bucket = conf$bucket
)


signals_list <- unique(corridors$SignalID)

# This is in testing as of 8/26
subcorridors <- corridors %>% 
    filter(!is.na(Subcorridor)) %>%
    select(-Zone_Group) %>% 
    rename(
        Zone_Group = Zone, 
        Zone = Corridor, 
        Corridor = Subcorridor) 


conn <- get_athena_connection(conf)

# cam_config <- get_cam_config(
#     object = conf$cctv_config_filename, 
#     bucket = conf$bucket,
#     corridors = all_corridors)


usable_cores <- get_usable_cores()
doParallel::registerDoParallel(cores = usable_cores)


# # ###########################################################################

# # Package everything up for Monthly Report back 13 months

#----- DEFINE DATE RANGE FOR CALCULATIONS ------------------------------------#

# report_end_date <- floor_date(today() - days(6), unit = "months")
report_end_date <- Sys.Date() - days(1)
report_start_date <- floor_date(report_end_date - months(12), unit = "months")

calcs_end_date <- Sys.Date() - days(1)
if (conf$calcs_start_date == "auto") {
    if (day(Sys.Date()) < 15) {
        calcs_start_date <- Sys.Date() - months(1)
    } else {
        calcs_start_date <- Sys.Date()
    }
    day(calcs_start_date) <- 1
} else {
    calcs_start_date <- conf$calcs_start_date
}

round_to_tuesday <- function(date_) {
    if (is.null(date_)) {
        return (NULL)
    }
    if (is.character(date_)) {
        date_ <- ymd(date_)
    }
    date_ - wday(date_) + 3
}

wk_calcs_start_date <- round_to_tuesday(calcs_start_date)

dates <- seq(ymd(report_start_date), ymd(report_end_date), by = "1 month")
month_abbrs <- get_month_abbrs(report_start_date, report_end_date)

report_start_date <- as.character(report_start_date)
report_end_date <- as.character(report_end_date)
print(glue("{Sys.time()} Week Calcs Start Date: {wk_calcs_start_date}"))
print(glue("{Sys.time()} Calcs Start Date: {calcs_start_date}"))
print(glue("{Sys.time()} Report End Date: {report_end_date}"))

date_range <- seq(ymd(report_start_date), ymd(report_end_date), by = "1 day")
date_range_str <- paste0("{", paste0(as.character(date_range), collapse = ","), "}")

#options(warn = 2) # Turn warnings into errors we can run a traceback on. For debugging only.
