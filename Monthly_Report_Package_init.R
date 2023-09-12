
# Monthly_Report_Package_init.R

source("renv/activate.R")


source("write_sigops_to_db.R")
source("Monthly_Report_Functions.R")
source("Classes.R")


print(glue("{Sys.time()} Starting Package Script"))


if (interactive()) {
    plan(multisession)
} else {
    plan(multicore)
}

usable_cores <- get_usable_cores()
doParallel::registerDoParallel(cores = usable_cores)


aurora <- get_aurora_connection()

corridors <- dbReadTable(aurora, "Corridors")
all_corridors <- dbReadTable(aurora, "AllCorridors")


conn <- get_atspm_connection(cred)
signals_list <- dbGetQuery(conn, "SELECT SignalID from Signals") %>% pull(SignalID)
dbDisconnect(conn)


subcorridors <- corridors %>% 
    filter(!is.na(Subcorridor)) %>%
    select(-Zone_Group) %>% 
    rename(
        Zone_Group = Zone, 
        Zone = Corridor, 
        Corridor = Subcorridor) 



usable_cores <- get_usable_cores()
doParallel::registerDoParallel(cores = usable_cores)


# # ###########################################################################

# # Package everything up for Monthly Report back 15 months

#----- DEFINE DATE RANGE FOR CALCULATIONS ------------------------------------#

report_end_date <- Sys.Date() - days(1)
report_start_date <- floor_date(report_end_date - months(15), unit = "months")
report_start_date <- max(as_date("2022-09-29"), report_start_date)

if (conf$report_end_date == "yesterday") {
    report_end_date <- Sys.Date() - days(1)
} else {
    report_end_date <- conf$report_end_date
}

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
