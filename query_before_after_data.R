
source("Monthly_Report_UI_Functions.R")
source("Before-After.R")



metric <- travel_time_index
level <- "corridor"
zone_group <- "US 50 (Fairfax)"
corridor <-  "US 50 (Fairfax)"
before_start_date <- as_date("2022-05-01")
before_end_date <- as_date("2022-06-30")
after_start_date <- as_date("2022-07-01")
after_end_date <- as_date("2022-08-30")

level <- "corridor"
zone_group <- "Northern Region"
corridor <- "All Corridors"

current_month <- as_date("2022-08-01")


df <- query_before_after_data(
    metric, level, zone_group, corridor, 
    before_start_date, before_end_date, 
    after_start_date, after_end_date)

before_df <- df$before
after_df <- df$after

get_before_after_line_plot(before_df, after_df, metric)




metric_x <- detector_uptime
metric_y <- comm_uptime

dfx <- query_before_after_data(
    metric_x, level, zone_group, corridor, 
    before_start_date, before_end_date, 
    after_start_date, after_end_date)
dfy <- query_before_after_data(
    metric_y, level, zone_group, corridor, 
    before_start_date, before_end_date, 
    after_start_date, after_end_date)




uptime_multiplot(
    detector_uptime, 
    level = level, zone_group = zone_group, month = current_month)
