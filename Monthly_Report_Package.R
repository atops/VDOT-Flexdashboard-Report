
# Monthly_Report_Package.R

print(Sys.time())

library(yaml)

if (Sys.info()["sysname"] == "Windows") {
    working_directory <- file.path(dirname(path.expand("~")), "Code", "GDOT", "GDOT-Flexdashboard-Report")
    
} else if (Sys.info()["sysname"] == "Linux") {
    working_directory <- file.path("~", "Code", "GDOT", "GDOT-Flexdashboard-Report")
    
} else {
    stop("Unknown operating system.")
}
setwd(working_directory)

source("Monthly_Report_Functions.R")
conf <- read_yaml("Monthly_Report_calcs.yaml")


corridors <- feather::read_feather(conf$corridors_filename) 
signals_list <- corridors$SignalID[!is.na(corridors$SignalID)]

# # ###########################################################################

f <- function(prefix, month_abbrs, daily = FALSE, combine = TRUE) {
    
    cl <- makeCluster(3)
    clusterExport(cl, c("prefix"),
                  envir = environment())
    x <- parLapply(cl, month_abbrs, function(month_abbr) {
        library(fst)
        library(dplyr)
        library(tidyr)
        library(lubridate)
        
        if (daily == TRUE) {
            fns <- list.files(pattern = paste0(prefix, month_abbr, "-\\d{2}.fst"))
            df <- lapply(fns, read_fst) %>% 
                bind_rows() %>% 
                mutate(SignalID = factor(SignalID)) %>% 
                as_tibble()
        } else {
            fn <- paste0(prefix, month_abbr, ".fst")
            df <- read_fst(fn) %>%
                mutate(SignalID = factor(SignalID)) %>%
                as_tibble() 
        }
        df
    })
    stopCluster(cl)
    
    if (combine == TRUE) {
        x <- bind_rows(x)
        #x <- bind_rows_keep_factors(x)
    }
    x
}

# # Package everything up for Monthly Report back 13 months

#----- DEFINE DATE RANGE FOR CALCULATIONS ------------------------------------#

report_start_date <- conf$report_start_date
if (conf$report_end_date == "yesterday") {
    report_end_date <- Sys.Date() - days(1)
} else {
    report_end_date <- conf$report_end_date
}
dates <- seq(ymd(report_start_date), ymd(report_end_date), by = "1 month")
month_abbrs <- get_month_abbrs(report_start_date, report_end_date)

print(month_abbrs)


# # TRAVEL TIME AND BUFFER TIME INDEXES #######################################

print("Travel Times")

# Eventually (soon) this will be replaced to get tti and pti from summary tables
# calculated from RITIS API
fs <- list.files(path = "Inrix/For_Monthly_Report", 
                 pattern = "TWTh.csv$",
                 recursive = TRUE, 
                 full.names = TRUE)
fns <- fs[fs < "Inrix/For_Monthly_Report/2018-01"] # old format: files before 2018-01
tt <- get_tt_csv(fns)
tti <- tt$tti 
pti <- tt$pti 

fns2 <- list.files(path = "Inrix/For_Monthly_Report",
                   pattern = "tt_.*_summary.csv",
                   recursive = FALSE,
                   full.names = TRUE)

tt2 <- lapply(fns2, read_csv) %>% bind_rows()
#tt <- lapply(fns, read_csv) %>% bind_rows() %>% mutate(Corridor = factor(Corridor))
#tti <- tt %>% select(-pti)
#pti <- tt %>% select(-tti)

tti <- tt2 %>% 
    select(-pti) %>% 
    bind_rows(tti) %>% 
    mutate(Corridor = factor(Corridor)) %>%
    arrange(Corridor, Hour)
    

pti <- tt2 %>% 
    select(-tti) %>% 
    bind_rows(pti) %>% 
    mutate(Corridor = factor(Corridor)) %>%
    arrange(Corridor, Hour)



write_fst(tti, "tti.fst")
write_fst(pti, "pti.fst")

rm(tt)
gc()


# # DETECTOR UPTIME ###########################################################

print("Detector Uptime")

ddu <- f("ddu_", month_abbrs)
daily_detector_uptime <- split(ddu, ddu$setback)

avg_daily_detector_uptime <- get_avg_daily_detector_uptime(daily_detector_uptime)
cor_avg_daily_detector_uptime <- get_cor_avg_daily_detector_uptime(avg_daily_detector_uptime, corridors)

# mdu_all <- get_monthly_avg_by_day(ddu, "uptime", "all")
# mdu_sb <- get_monthly_avg_by_day(filter(ddu, setback == "Setback"), "uptime", "all")
# mdu_pr <- get_monthly_avg_by_day(filter(ddu, setback == "Presence"), "uptime", "all")

monthly_detector_uptime <- get_monthly_detector_uptime(avg_daily_detector_uptime)
cor_monthly_detector_uptime <- get_cor_monthly_detector_uptime(avg_daily_detector_uptime, corridors)

saveRDS(avg_daily_detector_uptime, "avg_daily_detector_uptime.rds")
saveRDS(monthly_detector_uptime, "monthly_detector_uptime.rds")

saveRDS(cor_avg_daily_detector_uptime, "cor_avg_daily_detector_uptime.rds")
saveRDS(cor_monthly_detector_uptime, "cor_monthly_detector_uptime.rds")

rm(ddu)
rm(daily_detector_uptime)
rm(avg_daily_detector_uptime)
rm(monthly_detector_uptime)
rm(cor_avg_daily_detector_uptime)
rm(cor_monthly_detector_uptime)
gc()

# GET COMMUNICATIONS UPTIME ###################################################

print("Communication Uptime")

cu <- f("cu_", month_abbrs) 

daily_comm_uptime <- get_daily_avg(cu, "uptime", peak_only = FALSE) 
cor_daily_comm_uptime <- get_cor_weekly_avg_by_day(daily_comm_uptime, corridors, "uptime")

weekly_comm_uptime <- get_weekly_avg_by_day(cu, "uptime", peak_only = FALSE)
cor_weekly_comm_uptime <- get_cor_weekly_avg_by_day(weekly_comm_uptime, corridors, "uptime")

monthly_comm_uptime <- get_monthly_avg_by_day(cu, "uptime", peak_only = FALSE)
cor_monthly_comm_uptime <- get_cor_monthly_avg_by_day(monthly_comm_uptime, corridors, "uptime")


saveRDS(daily_comm_uptime, "daily_comm_uptime.rds")
saveRDS(cor_daily_comm_uptime, "cor_daily_comm_uptime.rds")

saveRDS(weekly_comm_uptime, "weekly_comm_uptime.rds")
saveRDS(cor_weekly_comm_uptime, "cor_weekly_comm_uptime.rds")

saveRDS(monthly_comm_uptime, "monthly_comm_uptime.rds")
saveRDS(cor_monthly_comm_uptime, "cor_monthly_comm_uptime.rds")

rm(cu)
rm(daily_comm_uptime)
rm(cor_daily_comm_uptime)
rm(weekly_comm_uptime)
rm(cor_weekly_comm_uptime)
rm(monthly_comm_uptime)
rm(cor_monthly_comm_uptime)
gc()

# DAILY VOLUMES ###############################################################

print("Daily Volumes")

vpd <- f("vpd_", month_abbrs)
weekly_vpd <- get_weekly_vpd(vpd)

# Group into corridors --------------------------------------------------------
cor_weekly_vpd <- get_cor_weekly_vpd(weekly_vpd, corridors)

# Monthly volumes for bar charts and % change ---------------------------------
monthly_vpd <- get_monthly_vpd(vpd)

# Group into corridors
cor_monthly_vpd <- get_cor_monthly_vpd(monthly_vpd, corridors)

# Monthly % change from previous month by corridor ----------------------------
saveRDS(weekly_vpd, "weekly_vpd.rds")
saveRDS(monthly_vpd, "monthly_vpd.rds")
saveRDS(cor_weekly_vpd, "cor_weekly_vpd.rds")
saveRDS(cor_monthly_vpd, "cor_monthly_vpd.rds")

rm(vpd)
rm(weekly_vpd)
rm(monthly_vpd)
rm(cor_weekly_vpd)
rm(cor_monthly_vpd)
gc()

# HOURLY VOLUMES ##############################################################

print("Hourly Volumes")

vph <- f("vph_", month_abbrs)
weekly_vph <- get_weekly_vph(mutate(vph, CallPhase = 2)) # Hack because next function needs a CallPhase
weekly_vph_peak <- get_weekly_vph_peak(weekly_vph)

# Group into corridors --------------------------------------------------------
cor_weekly_vph <- get_cor_weekly_vph(weekly_vph, corridors)
cor_weekly_vph_peak <- get_cor_weekly_vph_peak(cor_weekly_vph)

monthly_vph <- get_monthly_vph(vph)
monthly_vph_peak <- get_monthly_vph_peak(monthly_vph)

# Hourly volumes by Corridor --------------------------------------------------
cor_monthly_vph <- get_cor_monthly_vph(monthly_vph, corridors)
cor_monthly_vph_peak <- get_cor_monthly_vph_peak(cor_monthly_vph)

saveRDS(weekly_vph, "weekly_vph.rds")
saveRDS(monthly_vph, "monthly_vph.rds")
saveRDS(cor_weekly_vph, "cor_weekly_vph.rds")
saveRDS(cor_monthly_vph, "cor_monthly_vph.rds")

saveRDS(weekly_vph_peak, "weekly_vph_peak.rds")
saveRDS(monthly_vph_peak, "monthly_vph_peak.rds")
saveRDS(cor_weekly_vph_peak, "cor_weekly_vph_peak.rds")
saveRDS(cor_monthly_vph_peak, "cor_monthly_vph_peak.rds")

rm(vph)
rm(weekly_vph)
rm(monthly_vph)
rm(cor_weekly_vph)
#rm(cor_monthly_vph)
rm(weekly_vph_peak)
rm(monthly_vph_peak)
rm(cor_weekly_vph_peak)
rm(cor_monthly_vph_peak)
gc()

# DAILY THROUGHPUT ############################################################

print("Daily Throughput")

throughput <- f("tp_", month_abbrs)
weekly_throughput <- get_weekly_thruput(throughput)

# Group into corridors --------------------------------------------------------
cor_weekly_throughput <- get_cor_weekly_thruput(weekly_throughput, corridors)

# Monthly throughput for bar charts and % change ---------------------------------
monthly_throughput <- get_monthly_thruput(throughput)

# Group into corridors
cor_monthly_throughput <- get_cor_monthly_thruput(monthly_throughput, corridors)

# Monthly % change from previous month by corridor ----------------------------
#cor_mo_pct_throughput <- get_cor_monthly_pct_change_thruput(cor_monthly_throughput)

saveRDS(weekly_throughput, "weekly_throughput.rds")
saveRDS(monthly_throughput, "monthly_throughput.rds")
saveRDS(cor_weekly_throughput, "cor_weekly_throughput.rds")
saveRDS(cor_monthly_throughput, "cor_monthly_throughput.rds")

rm(throughput)
rm(weekly_throughput)
rm(cor_weekly_throughput)
rm(monthly_throughput)
rm(cor_monthly_throughput)
gc()

# DAILY ARRIVALS ON GREEN #####################################################

print("Daily AOG")

aog <- f("aog_", month_abbrs, combine = TRUE)
daily_aog <- get_daily_aog(aog)

weekly_aog_by_day <- get_weekly_aog_by_day(aog)

cor_weekly_aog_by_day <- get_cor_weekly_aog_by_day(weekly_aog_by_day, corridors)

monthly_aog_by_day <- get_monthly_aog_by_day(aog)

cor_monthly_aog_by_day <- get_cor_monthly_aog_by_day(monthly_aog_by_day, corridors)

saveRDS(weekly_aog_by_day, "weekly_aog_by_day.rds")
saveRDS(monthly_aog_by_day, "monthly_aog_by_day.rds")
saveRDS(cor_weekly_aog_by_day, "cor_weekly_aog_by_day.rds")
saveRDS(cor_monthly_aog_by_day, "cor_monthly_aog_by_day.rds")

rm(daily_aog)
rm(weekly_aog_by_day)
rm(monthly_aog_by_day)
rm(cor_weekly_aog_by_day)
rm(cor_monthly_aog_by_day)
gc()

# HOURLY ARRIVALS ON GREEN ####################################################

print("Hourly AOG")

aog_by_hr <- get_aog_by_hr(aog)
monthly_aog_by_hr <- get_monthly_aog_by_hr(aog_by_hr)

# Hourly volumes by Corridor --------------------------------------------------
cor_monthly_aog_by_hr <- get_cor_monthly_aog_by_hr(monthly_aog_by_hr, corridors)

cor_monthly_aog_peak <- get_cor_monthly_aog_peak(cor_monthly_aog_by_hr)

saveRDS(monthly_aog_by_hr, "monthly_aog_by_hr.rds")
saveRDS(cor_monthly_aog_by_hr, "cor_monthly_aog_by_hr.rds")

rm(aog)
rm(aog_by_hr)
rm(cor_monthly_aog_peak)
rm(monthly_aog_by_hr)
rm(cor_monthly_aog_by_hr)
gc()

# DAILY SPLIT FAILURES #####################################################

print("Daily Split Failures")

## --- Move all of this to Monthly_Report_Calcs.R --

#sf_filenames <- list.files(pattern = "sf_\\d{4}-\\d{2}-\\d{2}\\.feather")

# sf_filenames <- lapply(month_abbrs, function(month_abbr) {
#     list.files(pattern = paste0("sf_", month_abbr, "-\\d{2}\\.feather"))
#     }) %>% unlist()
# 
# wds <- lubridate::wday(sub(pattern = "sf_(.*)\\.feather", "\\1", sf_filenames), label = TRUE)
# twr <- sapply(wds, function(x) {x %in% c("Tue", "Wed", "Thu")})
# sf_filenames <- sf_filenames[twr]
# 
# cl <- makeCluster(3)
# clusterExport(cl, c("get_sf", "week"),
#               envir = environment())
# sf <- parLapply(cl, sf_filenames, function(fn) { 
#     library(feather)
#     library(dplyr)
#     library(lubridate)
#     
#     get_sf(read_feather(fn)) %>% mutate(Week = week(Date))
#     }) %>% bind_rows()
# stopCluster(cl)

## --- ------------------------------------------ --

sf <- f("sf_", month_abbrs)
wsf <- get_weekly_sf_by_day(sf)
cor_wsf <- get_cor_weekly_sf_by_day(wsf, corridors)

monthly_sfd <- get_monthly_sf_by_day(sf)
cor_monthly_sfd <- get_cor_monthly_sf_by_day(monthly_sfd, corridors)

saveRDS(wsf, "wsf.rds")
saveRDS(monthly_sfd, "monthly_sfd.rds")
saveRDS(cor_wsf, "cor_wsf.rds")
saveRDS(cor_monthly_sfd, "cor_monthly_sfd.rds")


rm(wsf)
rm(monthly_sfd)
rm(cor_wsf)
rm(cor_monthly_sfd)
gc()

# HOURLY SPLIT FAILURES #######################################################

print("Hourly Split Failures")

sfh <- get_sf_by_hr(sf)

msfh <- get_monthly_sf_by_hr(sfh)

# Hourly volumes by Corridor --------------------------------------------------
cor_msfh <- get_cor_monthly_sf_by_hr(msfh, corridors)

saveRDS(msfh, "msfh.rds")
saveRDS(cor_msfh, "cor_msfh.rds")

rm(sf)
rm(sfh)
rm(msfh)
rm(cor_msfh)
gc()

# DAILY QUEUE SPILLBACK #######################################################

print("Daily Queue Spillback")

qs <- f("qs_", month_abbrs)
wqs <- get_weekly_qs_by_day(qs)
cor_wqs <- get_cor_weekly_qs_by_day(wqs, corridors)

monthly_qsd <- get_monthly_qs_by_day(qs)
cor_monthly_qsd <- get_cor_monthly_qs_by_day(monthly_qsd, corridors)

saveRDS(wqs, "wqs.rds")
saveRDS(monthly_qsd, "monthly_qsd.rds")
saveRDS(cor_wqs, "cor_wqs.rds")
saveRDS(cor_monthly_qsd, "cor_monthly_qsd.rds")


# HOURLY QUEUE SPILLBACK ######################################################

print("Hourly Queue Spillback")

qsh <- get_qs_by_hr(qs)
mqsh <- get_monthly_qs_by_hr(qsh)

# Hourly volumes by Corridor --------------------------------------------------
cor_mqsh <- get_cor_monthly_qs_by_hr(mqsh, corridors)

saveRDS(mqsh, "mqsh.rds")
saveRDS(cor_mqsh, "cor_mqsh.rds")

rm(qs)
rm(wqs)
rm(monthly_qsd)
rm(cor_wqs)
rm(cor_monthly_qsd)
rm(qsh)
rm(mqsh)
rm(cor_mqsh)
gc()

# TRAVEL TIME AND BUFFER TIME INDEXES #########################################

tti <- read_fst("tti.fst")
pti <- read_fst("pti.fst")

cor_monthly_tti_by_hr <- get_cor_monthly_ti_by_hr(tti, cor_monthly_vph, corridors)
cor_monthly_pti_by_hr <- get_cor_monthly_ti_by_hr(pti, cor_monthly_vph, corridors)

cor_monthly_tti <- get_cor_monthly_tti(cor_monthly_tti_by_hr, corridors)
cor_monthly_pti <- get_cor_monthly_pti(cor_monthly_pti_by_hr, corridors)

saveRDS(cor_monthly_tti, "cor_monthly_tti.rds")
saveRDS(cor_monthly_tti_by_hr, "cor_monthly_tti_by_hr.rds")

saveRDS(cor_monthly_pti, "cor_monthly_pti.rds")
saveRDS(cor_monthly_pti_by_hr, "cor_monthly_pti_by_hr.rds")

rm(tti)
rm(pti)
rm(cor_monthly_tti)
rm(cor_monthly_tti_by_hr)
rm(cor_monthly_pti)
rm(cor_monthly_pti_by_hr)

# DETECTOR UPTIME AS REPORTED BY FIELD ENGINEERS ##############################

print("Uptimes")

# # VEH, PED, CCTV UPTIME - AS REPORTED BY FIELD ENGINEERS via EXCEL

fns <- list.files(path = "Excel Monthly Reports/2017_12", recursive = TRUE, full.names = TRUE)

mrs_veh_xl <- get_veh_uptime_from_xl_monthly_reports(fns, corridors)
mrs_ped_xl <- get_ped_uptime_from_xl_monthly_reports(fns, corridors)



xl_uptime_fns <- file.path(conf$xl_uptime$path, conf$xl_uptime$filenames)
xl_uptime_mos <- conf$xl_uptime$months

man_xl <- purrr::map2(xl_uptime_fns,
                      xl_uptime_mos,
                      get_det_uptime_from_manual_xl) %>%
    bind_rows() %>%
    mutate(Zone_Group = factor(Zone_Group))

man_veh_xl <- man_xl %>% filter(Type == "Vehicle") %>% select(-Type)
man_ped_xl <- man_xl %>% filter(Type == "Pedestrian") %>% select(-Type)


cor_monthly_xl_veh_uptime <- get_cor_monthly_xl_uptime(bind_rows(mrs_veh_xl, man_veh_xl))
cor_monthly_xl_ped_uptime <- get_cor_monthly_xl_uptime(bind_rows(mrs_ped_xl, man_ped_xl))



mrs_cctv_xl <- get_cctv_uptime_from_xl_monthly_reports(fns, corridors)

# January data. One-time only.
man_cctv_xl_2018_01 <- read_excel("Vehicle and Ped Detector Info/January Vehicle and Ped Detector Info.xlsx", sheet = "cor_cctv") %>%
    transmute(Zone_Group = Zone_Group,
              Corridor = Corridor,
              Month = ymd("2018-01-01"),
              up = as.integer(up),
              num = as.integer(num),
              uptime = uptime)


cam_config <- read.csv(conf$cctv_config_filename) %>% as_tibble() %>% #"../camera_ids_2018-10-22.csv"
    separate(col = CamID, into = c("CameraID", "Location"), sep = ": ") %>%
    mutate(As_of_Date = ymd(As_of_Date))


# CCTV image size variance by CameraID and Date
#  -> reduce to 1 for Size > 0, 0 otherwise

# Expanded out to include all available cameras on all days
#  up/uptime is 0 if no data
daily_cctv_uptime_511 <- read_feather(conf$cctv_parsed_filename) %>% #"parsed_cctv.feather"
    filter(Date > "2018-02-02" & Size > 0) %>%
    mutate(up = 1, num = 1) %>%
    select(-Size) %>%
    distinct() %>%
    
    # Expanded out to include all available cameras on all days
    complete(CameraID, Date = full_seq(Date, 1), fill = list(up = 0, num = 1)) %>%
    
    mutate(uptime = up/num) %>%
    
    left_join(select(cam_config, -Location)) %>% 
    filter(Date >= As_of_Date & Corridor != "") %>%
    select(-As_of_Date) %>%
    mutate(CameraID = factor(CameraID))


# Find the days where uptime across the board is very low (close to 0)
#  This is symptomatic of a problem with the acquisition rather than the camreras themselves
bad_days <- daily_cctv_uptime_511 %>% 
    group_by(Date) %>% 
    summarize(sup = sum(up),
              snum = sum(num),
              suptime = sum(up)/sum(num)) %>% 
    filter(suptime < 0.2)

# Filter out the bad days, add zone group
cor_daily_cctv_uptime_511 <- daily_cctv_uptime_511 %>%
    filter(!Date %in% bad_days$Date) %>%
    group_by(Corridor, Date) %>%
    summarize(up = sum(up),
              num = sum(num)) %>%
    mutate(uptime = up/num) %>%
    ungroup() %>%
    left_join(distinct(corridors, Corridor, Zone_Group))

# this output doesn't filter bad days because we want it to display all days
#  on the website. Add zone group.
daily_cctv_uptime <- daily_cctv_uptime_511 %>%
    left_join(distinct(select(corridors, Corridor, Zone_Group))) %>%
    mutate(CameraID = factor(CameraID), 
           Corridor = factor(Corridor),
           Zone_Group = factor(Zone_Group)) 


cor_daily_cctv_uptime <- bind_rows(mrs_cctv_xl, 
                                   man_cctv_xl_2018_01, 
                                   select(cor_daily_cctv_uptime_511,
                                          Zone_Group, Corridor, Month = Date, up, num, uptime)) %>%
    get_cor_monthly_xl_uptime() %>%
    rename(Date = Month) %>%
    select(-Zone_Group) %>%
    left_join(distinct(corridors, Corridor, Zone_Group)) %>%
    mutate(Zone_Group = factor(if_else(is.na(Zone_Group), Corridor, Zone_Group)),
           Corridor = factor(Corridor))



cor_weekly_cctv_uptime_511 <- get_cor_weekly_cctv_uptime(cor_daily_cctv_uptime_511)

cor_weekly_cctv_uptime <- bind_rows(mrs_cctv_xl,
                                    man_cctv_xl_2018_01,
                                    select(cor_weekly_cctv_uptime_511,
                                           Zone_Group, Corridor, Month = Date, up, num, uptime)) %>%
    get_cor_monthly_xl_uptime() %>%
    rename(Date = Month)



cor_monthly_cctv_uptime_511 <- get_cor_monthly_cctv_uptime(cor_daily_cctv_uptime_511)

cor_monthly_cctv_uptime <- bind_rows(mrs_cctv_xl, 
                                     man_cctv_xl_2018_01, 
                                     cor_monthly_cctv_uptime_511) %>%
    get_cor_monthly_xl_uptime()






saveRDS(cor_monthly_xl_veh_uptime, "cor_monthly_xl_veh_uptime.rds")
saveRDS(cor_monthly_xl_ped_uptime, "cor_monthly_xl_ped_uptime.rds")


saveRDS(daily_cctv_uptime, "daily_cctv_uptime.rds")

saveRDS(cor_monthly_cctv_uptime, "cor_monthly_cctv_uptime.rds")

saveRDS(cor_daily_cctv_uptime, "cor_daily_cctv_uptime.rds")
saveRDS(cor_weekly_cctv_uptime, "cor_weekly_cctv_uptime.rds")






# ACTIVITIES ##############################

print("TEAMS")

# New version from API result
teams <- get_teams_tasks(conf$teams_tasks_filename) %>%
    
    filter(`Date Reported` >= ymd(report_start_date) &
               `Date Reported` <= ymd(report_end_date))


saveRDS(teams, "teams.rds")
#------------------------------------------------------------------------------


type_table <- get_outstanding_events(teams, "Task_Type") %>%
    mutate(Task_Type = if_else(Task_Type == "", "Unknown", Task_Type)) %>%
    group_by(Zone_Group, Task_Type, Month) %>%
    summarize_all(sum) %>%
    ungroup() %>%
    mutate(Task_Type = factor(Task_Type))

subtype_table <- get_outstanding_events(teams, "Task_Subtype") %>%
    mutate(Task_Subtype = if_else(Task_Subtype == "", "Unknown", Task_Subtype)) %>%
    group_by(Zone_Group, Task_Subtype, Month) %>%
    summarize_all(sum) %>%
    ungroup() %>%
    mutate(Task_Subtype = factor(Task_Subtype))

source_table <- get_outstanding_events(teams, "Task_Source") %>%
    mutate(Task_Source = if_else(Task_Source == "", "Unknown", Task_Source)) %>%
    group_by(Zone_Group, Task_Source, Month) %>%
    summarize_all(sum) %>%
    ungroup() %>%
    mutate(Task_Source = factor(Task_Source))

priority_table <- get_outstanding_events(teams, "Priority") %>%
    group_by(Zone_Group, Priority, Month) %>%
    summarize_all(sum) %>%
    ungroup() %>%
    mutate(Priority = factor(Priority))

all_teams_table <- get_outstanding_events(teams, "All") %>%
    group_by(Zone_Group, All, Month) %>%
    summarize_all(sum) %>%
    ungroup() %>%
    mutate(All = factor(All))

teams_tables <- list("type" = type_table,
                     "subtype" = subtype_table,
                     "source" = source_table,
                     "priority" = priority_table,
                     "all" = all_teams_table)

saveRDS(teams_tables, "teams_tables.rds")


cor_monthly_events <- teams_tables$all %>%
    ungroup() %>%
    transmute(Corridor = Zone_Group,
              Zone_Group = Zone_Group,
              Month = Month,
              Reported = Rep,
              Resolved = Res,
              Outstanding = outstanding) %>%
    arrange(Corridor, Zone_Group, Month) %>%
    group_by(Corridor, Zone_Group) %>%
    mutate(delta.rep = (Reported - lag(Reported))/lag(Reported),
           delta.res = (Resolved - lag(Resolved))/lag(Resolved),
           delta.out = (Outstanding - lag(Outstanding))/lag(Outstanding))

saveRDS(cor_monthly_events, "cor_monthly_events.rds")






# Package up for Flexdashboard

print("Package for Monthly Report")

sigify <- function(df, cor_df, corridors) {
    
    df_ <- df %>% left_join(distinct(corridors, SignalID, Corridor, Name)) %>%
        rename(Zone_Group = Corridor, Corridor = SignalID) %>%
        ungroup() %>%
        mutate(Corridor = factor(Corridor))
    
    cor_df_ <- cor_df %>%
        filter(Corridor %in% unique(df_$Zone_Group)) %>%
        mutate(Zone_Group = Corridor)
    
    br <- bind_rows(df_, cor_df_)
    
    if ("Month" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Month)
    } else if ("Hour" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Hour)
    } else if ("Date" %in% names(br)) {
        br %>% arrange(Zone_Group, Corridor, Date)
    }
}




cor <- list()
cor$dy <- list("du" = readRDS("cor_avg_daily_detector_uptime.rds"),
               "cu" = readRDS("cor_daily_comm_uptime.rds"),
               "cctv" = readRDS("cor_daily_cctv_uptime.rds"))
cor$wk <- list("vpd" = readRDS("cor_weekly_vpd.rds"),
               "vph" = readRDS("cor_weekly_vph.rds"),
               "vphp" = readRDS("cor_weekly_vph_peak.rds"),
               "tp" = readRDS("cor_weekly_throughput.rds"),
               "aog" = readRDS("cor_weekly_aog_by_day.rds"),
               "qs" = readRDS("cor_wqs.rds"),
               "sf" = readRDS("cor_wsf.rds"),
               "cu" = readRDS("cor_weekly_comm_uptime.rds"),
               "cctv" =  readRDS("cor_weekly_cctv_uptime.rds"))
cor$mo <- list("vpd" = readRDS("cor_monthly_vpd.rds"),
               "vph" = readRDS("cor_monthly_vph.rds"),
               "vphp" = readRDS("cor_monthly_vph_peak.rds"),
               "tp" = readRDS("cor_monthly_throughput.rds"),
               "aogd" = readRDS("cor_monthly_aog_by_day.rds"),
               "aogh" = readRDS("cor_monthly_aog_by_hr.rds"),
               "qsd" = readRDS("cor_monthly_qsd.rds"),
               "qsh" = readRDS("cor_mqsh.rds"),
               "sfd" = readRDS("cor_monthly_sfd.rds"),
               "sfh" = readRDS("cor_msfh.rds"),
               "tti" = readRDS("cor_monthly_tti.rds"),
               "ttih" = readRDS("cor_monthly_tti_by_hr.rds"),
               "pti" = readRDS("cor_monthly_pti.rds"),
               "ptih" = readRDS("cor_monthly_pti_by_hr.rds"),
               "du" = readRDS("cor_monthly_detector_uptime.rds"),
               "cu" = readRDS("cor_monthly_comm_uptime.rds"),
               "veh" = readRDS("cor_monthly_xl_veh_uptime.rds"),
               "ped" = readRDS("cor_monthly_xl_ped_uptime.rds"),
               "cctv" = readRDS("cor_monthly_cctv_uptime.rds"),
               "events" = readRDS("cor_monthly_events.rds"))
cor$qu <- list("vpd" = get_quarterly(cor$mo$vpd, "vpd"),
               "vph" = data.frame(), #get_quarterly(cor$mo$vph, "vph"),
               "vphpa" = get_quarterly(cor$mo$vphp$am, "vph"),
               "vphpp" = get_quarterly(cor$mo$vphp$pm, "vph"),
               "tp" = get_quarterly(cor$mo$tp, "vph"),
               "aogd" = get_quarterly(cor$mo$aogd, "aog", "vol"),
               "qsd" = get_quarterly(cor$mo$qsd, "qs_freq"),
               "sfd" = get_quarterly(cor$mo$sfd, "sf_freq"),
               "tti" = get_quarterly(cor$mo$tti, "tti"),
               "pti" = get_quarterly(cor$mo$pti, "pti"),
               "du" = get_quarterly(cor$mo$du, "uptime.all"),
               "cu" = get_quarterly(cor$mo$cu, "uptime"),
               "veh" = get_quarterly(cor$mo$veh, "uptime", "num"),
               "ped" = get_quarterly(cor$mo$ped, "uptime", "num"),
               "cctv" = get_quarterly(cor$mo$cctv, "uptime", "num"),
               "reported" = get_quarterly(cor$mo$events, "Reported"),
               "resolved" =  get_quarterly(cor$mo$events, "Resolved"),
               "outstanding" = get_quarterly(cor$mo$events, "Outstanding", operation = "latest"))

sig <- list()
sig$dy <- list("du" = sigify(readRDS("avg_daily_detector_uptime.rds"), cor$dy$du, corridors),
               "cu" = sigify(readRDS("daily_comm_uptime.rds"), cor$dy$cu, corridors),
               "cctv" = readRDS("daily_cctv_uptime.rds"))
sig$wk <- list("vpd" = sigify(readRDS("weekly_vpd.rds"), cor$wk$vpd, corridors),
               "vph" = sigify(readRDS("weekly_vph.rds"), cor$wk$vph, corridors),
               "vphp" = purrr::map2(readRDS("weekly_vph_peak.rds"), cor$wk$vphp,
                                    function(x, y) { sigify(x, y, corridors) }),
               "tp" = sigify(readRDS("weekly_throughput.rds"), cor$wk$tp, corridors),
               "aog" = sigify(readRDS("weekly_aog_by_day.rds"), cor$wk$aog, corridors),
               "qs" = sigify(readRDS("wqs.rds"), cor$wk$qs, corridors),
               "sf" = sigify(readRDS("wsf.rds"), cor$wk$sf, corridors),
               "cu" = sigify(readRDS("weekly_comm_uptime.rds"), cor$wk$cu, corridors),
               "cctv" = readRDS("weekly_cctv_uptime.rds"))
sig$mo <- list("vpd" = sigify(readRDS("monthly_vpd.rds"), cor$mo$vpd, corridors),
               "vph" = sigify(readRDS("monthly_vph.rds"), cor$mo$vph, corridors),
               "vphp" = purrr::map2(readRDS("monthly_vph_peak.rds"), cor$mo$vphp,
                                    function(x, y) { sigify(x, y, corridors) }),
               "tp" = sigify(readRDS("monthly_throughput.rds"), cor$mo$tp, corridors),
               "aogd" = sigify(readRDS("monthly_aog_by_day.rds"), cor$mo$aogd, corridors),
               "aogh" = sigify(readRDS("monthly_aog_by_hr.rds"), cor$mo$aogh, corridors),
               "qsd" = sigify(readRDS("monthly_qsd.rds"), cor$mo$qsd, corridors),
               "qsh" = sigify(readRDS("mqsh.rds"), cor$mo$qsh, corridors),
               "sfd" = sigify(readRDS("monthly_sfd.rds"), cor$mo$sfd, corridors),
               "sfh" = sigify(readRDS("msfh.rds"), cor$mo$sfh, corridors),
               "tti" = data.frame(),
               "pti" = data.frame(),
               "du" = sigify(readRDS("monthly_detector_uptime.rds"), cor$mo$du, corridors),
               "cu" = sigify(readRDS("monthly_comm_uptime.rds"), cor$mo$cu, corridors),
               "cctv" = readRDS("monthly_cctv_uptime.rds"))

#saveRDS(cor, "cor.rds")
#saveRDS(sig, "sig.rds")


# Bring April data back from April Report.
#  Because the database went wonky.
# ---- Loop through all filenames and cor/sig objects from April
cor4 <- readRDS("2018-04 Report Files/cor.rds")
sig4 <- readRDS("2018-04 Report Files/sig.rds")

cor$dy$du <- patch_april(cor$dy$du, cor4$dy$du)
#cor$dy$cu <- patch_april(cor$dy$cu, cor4$dy$cu)
cor$wk$vpd <- patch_april(cor$wk$vpd, cor4$wk$vpd)
cor$wk$vph <- patch_april(cor$wk$vph, cor4$wk$vph)
cor$wk$vphp <- patch_april(cor$wk$vphp, cor4$wk$vphp)
cor$wk$tp <- patch_april(cor$wk$tp, cor4$wk$tp)
cor$wk$aog <- patch_april(cor$wk$aog, cor4$wk$aog)
cor$wk$qs <- patch_april(cor$wk$qs, cor4$wk$qs)
cor$wk$sf <- patch_april(cor$wk$sf, cor4$wk$sf)
cor$wk$cu <- patch_april(cor$wk$cu, cor4$wk$cu)
sig$dy$du <- patch_april(sig$dy$du, sig4$dy$du)
#sig$dy$cu <- patch_april(sig$dy$cu, sig4$dy$cu)
sig$wk$vpd <- patch_april(sig$wk$vpd, sig4$wk$vpd)
sig$wk$vph <- patch_april(sig$wk$vph, sig4$wk$vph)
sig$wk$vphp <- patch_april(sig$wk$vphp, sig4$wk$vphp)
sig$wk$tp <- patch_april(sig$wk$tp, sig4$wk$tp)
sig$wk$aog <- patch_april(sig$wk$aog, sig4$wk$aog)
sig$wk$qs <- patch_april(sig$wk$qs, sig4$wk$qs)
sig$wk$sf <- patch_april(sig$wk$sf, sig4$wk$sf)
#sig$wk$cu <- patch_april(sig$wk$cu, sig4$wk$cu)
cor$mo$vpd <- patch_april(cor$mo$vpd, cor4$mo$vpd)
cor$mo$vph <- patch_april(cor$mo$vph, cor4$mo$vph)
cor$mo$vphp <- patch_april(cor$mo$vphp, cor4$mo$vphp)
cor$mo$tp <- patch_april(cor$mo$tp, cor4$mo$tp)
cor$mo$aogd <- patch_april(cor$mo$aogd, cor4$mo$aogd)
cor$mo$aogh <- patch_april(cor$mo$aogh, cor4$mo$aogh)
cor$mo$qsd <- patch_april(cor$mo$qsd, cor4$mo$qsd)
cor$mo$qsh <- patch_april(cor$mo$qsh, cor4$mo$qsh)
cor$mo$sfd <- patch_april(cor$mo$sfd, cor4$mo$sfd)
cor$mo$sfh <- patch_april(cor$mo$sfh, cor4$mo$sfh)
cor$mo$du <- patch_april(cor$mo$du, cor4$mo$du)
#cor$mo$cu <- patch_april(cor$mo$cu, cor4$mo$cu)
sig$mo$vpd <- patch_april(sig$mo$vpd, sig4$mo$vpd)
sig$mo$vph <- patch_april(sig$mo$vph, sig4$mo$vph)
sig$mo$vphp <- patch_april(sig$mo$vphp, sig4$mo$vphp)
sig$mo$tp <- patch_april(sig$mo$tp, sig4$mo$tp)
sig$mo$aogd <- patch_april(sig$mo$aogd, sig4$mo$aogd)
sig$mo$aogh <- patch_april(sig$mo$aogh, sig4$mo$aogh)
sig$mo$qsd <- patch_april(sig$mo$qsd, sig4$mo$qsd)
sig$mo$qsh <- patch_april(sig$mo$qsh, sig4$mo$qsh)
sig$mo$sfd <- patch_april(sig$mo$sfd, sig4$mo$sfd)
sig$mo$sfh <- patch_april(sig$mo$sfh, sig4$mo$sfh)
#sig$mo$du <- patch_april(sig$mo$du, sig4$mo$du)
sig$mo$cu <- patch_april(sig$mo$cu, sig4$mo$cu)


saveRDS(cor, "cor.rds")
saveRDS(sig, "sig.rds")


aws.s3::put_object(file = "cor.rds", 
                   object = "cor.rds", 
                   bucket = "gdot-devices")
aws.s3::put_object(file = "sig.rds", 
                   object = "sig.rds", 
                   bucket = "gdot-devices")
aws.s3::put_object(file = "teams_tables.rds", 
                   object = "teams_tables.rds", 
                   bucket = "gdot-devices")

db_build_data_for_signal_dashboard(month_abbrs = month_abbrs[length(month_abbrs)], 
                                   corridors = corridors, 
                                   pth = 'Signal_Dashboards', 
                                   upload_to_s3 = TRUE)
