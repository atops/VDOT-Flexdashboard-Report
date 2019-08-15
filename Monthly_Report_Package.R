# Monthly_Report_Package.R

library(yaml)
library(glue)

print(glue("{Sys.time()} Starting Package Script"))


if (Sys.info()["sysname"] == "Windows") {
    working_directory <- file.path(dirname(path.expand("~")), "Code", "VDOT", "VDOT-Flexdashboard-Report")
    
} else if (Sys.info()["sysname"] == "Linux") {
    working_directory <- file.path("~", "Code", "VDOT", "VDOT-Flexdashboard-Report")
    
} else {
    stop("Unknown operating system.")
}
setwd(working_directory)

source("Monthly_Report_Functions.R")
conf <- read_yaml("Monthly_Report.yaml")


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
            if (length(fns) > 0) {
		    df <- lapply(fns, read_fst) %>% 
                    bind_rows() %>% 
                    mutate(SignalID = factor(SignalID)) %>% 
                    as_tibble()
	    } else {
		df <- data.frame()
	    }
        } else {
            fn <- paste0(prefix, month_abbr, ".fst")
	    if (file.exists(fn)) {
                df <- read_fst(fn) %>%
                    mutate(SignalID = factor(SignalID)) %>%
                    as_tibble()
	    } else {
		df <- data.frame()
	    }

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


# # DETECTOR UPTIME ###########################################################

print(glue("{Sys.time()} Detector Uptime [1 of 19]"))

tryCatch({          
    ddu <- f("ddu_", month_abbrs)

    avg_daily_detector_uptime <- get_avg_daily_detector_uptime(ddu)
    cor_avg_daily_detector_uptime <- get_cor_avg_daily_detector_uptime(avg_daily_detector_uptime, corridors)

    monthly_detector_uptime <- get_monthly_detector_uptime(avg_daily_detector_uptime)
    cor_monthly_detector_uptime <- get_cor_monthly_detector_uptime(avg_daily_detector_uptime, corridors)

    saveRDS(avg_daily_detector_uptime, "avg_daily_detector_uptime.rds")
    saveRDS(monthly_detector_uptime, "monthly_detector_uptime.rds")

    saveRDS(cor_avg_daily_detector_uptime, "cor_avg_daily_detector_uptime.rds")
    saveRDS(cor_monthly_detector_uptime, "cor_monthly_detector_uptime.rds")

    #rm(ddu)
    #rm(daily_detector_uptime)
    rm(avg_daily_detector_uptime)
    rm(monthly_detector_uptime)
    rm(cor_avg_daily_detector_uptime)
    rm(cor_monthly_detector_uptime)
    gc()

}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})    

# DAILY PEDESTRIAN DETECTOR UPTIME ###############################################

print(glue("{Sys.time()} Ped Detector Uptime [2 of 20]"))

tryCatch({
    papd <- f("papd_", month_abbrs)
    # papd <- s3_read_parquet_parallel(bucket = 'gdot-spm', 'ped_actuations_pd', report_start_date, report_end_date, signals_list) %>%
    #     replace_na((list(CallPhase = 0))) %>%
    #     mutate(SignalID = factor(SignalID),
    #            CallPhase = factor(CallPhase),
    #            Date = date(Date))
    
    pau <- get_pau(papd)
    
    # pau <- lapply(month_abbrs, function(yyyy_mm) {
    #     sd <- ymd(paste0(yyyy_mm, "-01"))
    #     ed <- sd + months(1) - days(1)
    #     ed <- min(ed, ymd(report_end_date))
    #     
    #     df <- s3_read_parquet_parallel('ped_actuations_pd', as.character(sd), as.character(ed), signals_list, 'gdot-spm')
    #     if (nrow(df) > 0) {
    #         df <- df %>%
    #             replace_na((list(CallPhase = 0))) %>%
    #             mutate(SignalID = factor(SignalID),
    #                    CallPhase = factor(CallPhase),
    #                    Date = date(Date)) %>% 
    #             get_pau()
    #     } else {
    #         df <- data.frame()
    #     }
    #     df
    #         
    # }) %>% bind_rows()
    
    daily_pa_uptime <- get_daily_avg(pau, "uptime", peak_only = FALSE)
    cor_daily_pa_uptime <- get_cor_weekly_avg_by_day(daily_pa_uptime, corridors, "uptime")
    
    weekly_pa_uptime <- get_weekly_avg_by_day(pau, "uptime", peak_only = FALSE)
    cor_weekly_pa_uptime <- get_cor_weekly_avg_by_day(weekly_pa_uptime, corridors, "uptime")
    
    monthly_pa_uptime <- get_monthly_avg_by_day(pau, "uptime", "all", peak_only = FALSE)
    cor_monthly_pa_uptime <- get_cor_monthly_avg_by_day(monthly_pa_uptime, corridors, "uptime")
    
    saveRDS(pau, "pa_uptime.rds")
    
    saveRDS(daily_pa_uptime, "daily_pa_uptime.rds")
    saveRDS(cor_daily_pa_uptime, "cor_daily_pa_uptime.rds")
    
    saveRDS(weekly_pa_uptime, "weekly_pa_uptime.rds")
    saveRDS(cor_weekly_pa_uptime, "cor_weekly_pa_uptime.rds")
    
    saveRDS(monthly_pa_uptime, "monthly_pa_uptime.rds")
    saveRDS(cor_monthly_pa_uptime, "cor_monthly_pa_uptime.rds")
    
    rm(papd)
    rm(pau)
    rm(daily_pa_uptime)
    rm(cor_daily_pa_uptime)
    rm(weekly_pa_uptime)
    rm(cor_weekly_pa_uptime)
    rm(monthly_pa_uptime)
    rm(cor_monthly_pa_uptime)
    # gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})


# GET COMMUNICATIONS UPTIME ###################################################

print(glue("{Sys.time()} Communication Uptime [2 of 19]"))

tryCatch({
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

}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})

<<<<<<< HEAD
=======
print(glue("{Sys.time()} Daily Volumes [3 of 19]"))
>>>>>>> 7673cc95a2daa422724e9e2e4c047a9c6349a375


# DAILY VOLUMES ###############################################################

print(glue("{Sys.time()} Daily Volumes [3 of 19]"))

tryCatch({
    vpd <- f("vpd_", month_abbrs)
    #  vpd <- s3_read_parquet_parallel(bucket = 'gdot-spm', 'vehicles_pd', report_start_date, report_end_date, signals_list) %>%
    #     mutate(SignalID = factor(SignalID),
    #            CallPhase = factor(CallPhase),
    #            Date = date(Date))
    
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
    # gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})



# HOURLY VOLUMES ##############################################################

print(glue("{Sys.time()} Hourly Volumes [4 of 19]"))
<<<<<<< HEAD

tryCatch({          
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
=======
>>>>>>> 7673cc95a2daa422724e9e2e4c047a9c6349a375

    
    
    
    
    
    
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})





# DAILY PEDESTRIAN ACTIVATIONS ################################################

print(glue("{Sys.time()} Daily Pedestrian Activations [6 of 20]"))

tryCatch({
    papd <- f("papd_", month_abbrs)
    # papd <- s3_read_parquet(bucket = 'gdot-spm', 'ped_actuations_pd', report_start_date, report_end_date, signals_list) %>%
    #     replace_na((list(CallPhase = 0))) %>%
    #     mutate(SignalID = factor(SignalID),
    #            CallPhase = factor(CallPhase),
    #            Date = date(Date))
    
    weekly_papd <- get_weekly_papd(papd)
    
    # Group into corridors --------------------------------------------------------
    cor_weekly_papd <- get_cor_weekly_papd(weekly_papd, corridors)
    
    # Monthly volumes for bar charts and % change ---------------------------------
    monthly_papd <- get_monthly_papd(papd)
    
    # Group into corridors
    cor_monthly_papd <- get_cor_monthly_papd(monthly_papd, corridors)
    
    # Monthly % change from previous month by corridor ----------------------------
    saveRDS(weekly_papd, "weekly_papd.rds")
    saveRDS(monthly_papd, "monthly_papd.rds")
    saveRDS(cor_weekly_papd, "cor_weekly_papd.rds")
    saveRDS(cor_monthly_papd, "cor_monthly_papd.rds")
    
    rm(papd)
    rm(weekly_papd)
    rm(monthly_papd)
    rm(cor_weekly_papd)
    rm(cor_monthly_papd)
    # gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})



# HOURLY PEDESTRIAN ACTIVATIONS ###############################################

print(glue("{Sys.time()} Hourly Pedestrian Activations [7 of 20]"))

tryCatch({
    paph <- f("paph_", month_abbrs)
    # paph <- s3_read_parquet(bucket = 'gdot-spm', 'ped_actuations_ph', report_start_date, report_end_date, signals_list) %>%
    #     mutate(SignalID = factor(SignalID),
    #            Date = date(Date))
    
    
    weekly_paph <- get_weekly_paph(mutate(paph, CallPhase = 2)) # Hack because next function needs a CallPhase
    #weekly_paph_peak <- get_weekly_paph_peak(weekly_paph)
    
    # Group into corridors --------------------------------------------------------
    cor_weekly_paph <- get_cor_weekly_paph(weekly_paph, corridors)
    #cor_weekly_paph_peak <- get_cor_weekly_vph_peak(cor_weekly_paph)
    
    monthly_paph <- get_monthly_paph(paph)
    #monthly_paph_peak <- get_monthly_vph_peak(monthly_paph)
    
    # Hourly volumes by Corridor --------------------------------------------------
    cor_monthly_paph <- get_cor_monthly_paph(monthly_paph, corridors)
    #cor_monthly_paph_peak <- get_cor_monthly_vph_peak(cor_monthly_paph)
    
    saveRDS(weekly_paph, "weekly_paph.rds")
    saveRDS(monthly_paph, "monthly_paph.rds")
    saveRDS(cor_weekly_paph, "cor_weekly_paph.rds")
    saveRDS(cor_monthly_paph, "cor_monthly_paph.rds")
    
    #saveRDS(weekly_paph_peak, "weekly_paph_peak.rds")
    #saveRDS(monthly_paph_peak, "monthly_paph_peak.rds")
    #saveRDS(cor_weekly_paph_peak, "cor_weekly_paph_peak.rds")
    #saveRDS(cor_monthly_paph_peak, "cor_monthly_paph_peak.rds")
    
    rm(paph)
    rm(weekly_paph)
    rm(monthly_paph)
    rm(cor_weekly_paph)
    rm(cor_monthly_paph)
    #rm(weekly_paph_peak)
    #rm(monthly_paph_peak)
    #rm(cor_weekly_paph_peak)
    #rm(cor_monthly_paph_peak)
    # gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})





# DAILY THROUGHPUT ############################################################

print(glue("{Sys.time()} Daily Throughput [7 of 19]"))
<<<<<<< HEAD

tryCatch({          
    throughput <- f("tp_", month_abbrs)
    weekly_throughput <- get_weekly_thruput(throughput)

    # Group into corridors --------------------------------------------------------
    cor_weekly_throughput <- get_cor_weekly_thruput(weekly_throughput, corridors)
=======
>>>>>>> 7673cc95a2daa422724e9e2e4c047a9c6349a375

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

}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})

<<<<<<< HEAD
=======
saveRDS(monthly_throughput, "monthly_throughput.rds")
saveRDS(cor_weekly_throughput, "cor_weekly_throughput.rds")
saveRDS(cor_monthly_throughput, "cor_monthly_throughput.rds")
>>>>>>> 7673cc95a2daa422724e9e2e4c047a9c6349a375


# DAILY ARRIVALS ON GREEN #####################################################

print(glue("{Sys.time()} Daily AOG [8 of 19]"))
<<<<<<< HEAD

tryCatch({          
    aog <- f("aog_", month_abbrs, combine = TRUE)
    daily_aog <- get_daily_aog(aog)

    weekly_aog_by_day <- get_weekly_aog_by_day(aog)

    cor_weekly_aog_by_day <- get_cor_weekly_aog_by_day(weekly_aog_by_day, corridors)
=======
>>>>>>> 7673cc95a2daa422724e9e2e4c047a9c6349a375

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

}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})



# HOURLY ARRIVALS ON GREEN ####################################################

print(glue("{Sys.time()} Hourly AOG [9 of 19]"))
<<<<<<< HEAD

tryCatch({
    aog_by_hr <- get_aog_by_hr(aog)
    monthly_aog_by_hr <- get_monthly_aog_by_hr(aog_by_hr)

    # Hourly volumes by Corridor --------------------------------------------------
    cor_monthly_aog_by_hr <- get_cor_monthly_aog_by_hr(monthly_aog_by_hr, corridors)

    cor_monthly_aog_peak <- get_cor_monthly_aog_peak(cor_monthly_aog_by_hr)

    saveRDS(monthly_aog_by_hr, "monthly_aog_by_hr.rds")
    saveRDS(cor_monthly_aog_by_hr, "cor_monthly_aog_by_hr.rds")

    #rm(aog)
    rm(aog_by_hr)
    rm(cor_monthly_aog_peak)
    rm(monthly_aog_by_hr)
    rm(cor_monthly_aog_by_hr)
    gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})



# DAILY PROGRESSION RATIO #####################################################

print(glue("{Sys.time()} Daily Progression Ratio [10.1 of 20]"))

tryCatch({
    
    daily_pr <- get_daily_pr(aog)
    
    weekly_pr_by_day <- get_weekly_pr_by_day(aog)
    
    cor_weekly_pr_by_day <- get_cor_weekly_pr_by_day(weekly_pr_by_day, corridors)
    
    monthly_pr_by_day <- get_monthly_pr_by_day(aog)
    
    cor_monthly_pr_by_day <- get_cor_monthly_pr_by_day(monthly_pr_by_day, corridors)
    
    saveRDS(weekly_pr_by_day, "weekly_pr_by_day.rds")
    saveRDS(monthly_pr_by_day, "monthly_pr_by_day.rds")
    saveRDS(cor_weekly_pr_by_day, "cor_weekly_pr_by_day.rds")
    saveRDS(cor_monthly_pr_by_day, "cor_monthly_pr_by_day.rds")
    
    rm(daily_pr)
    rm(weekly_pr_by_day)
    rm(monthly_pr_by_day)
    rm(cor_weekly_pr_by_day)
    rm(cor_monthly_pr_by_day)
    # gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})


=======
>>>>>>> 7673cc95a2daa422724e9e2e4c047a9c6349a375

# HOURLY PROGESSION RATIO ####################################################

print(glue("{Sys.time()} Hourly Progression Ratio [10.2 of 20]"))

tryCatch({
    pr_by_hr <- get_pr_by_hr(aog)
    monthly_pr_by_hr <- get_monthly_pr_by_hr(pr_by_hr)
    
    # Hourly volumes by Corridor --------------------------------------------------
    cor_monthly_pr_by_hr <- get_cor_monthly_pr_by_hr(monthly_pr_by_hr, corridors)
    
    #cor_monthly_pr_peak <- get_cor_monthly_pr_peak(cor_monthly_pr_by_hr)
    
    saveRDS(monthly_pr_by_hr, "monthly_pr_by_hr.rds")
    saveRDS(cor_monthly_pr_by_hr, "cor_monthly_pr_by_hr.rds")
    
    #rm(aog)
    rm(pr_by_hr)
    #rm(cor_monthly_pr_peak)
    rm(monthly_pr_by_hr)
    rm(cor_monthly_pr_by_hr)
    gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})





# DAILY SPLIT FAILURES #####################################################

<<<<<<< HEAD
tryCatch({          
    print(glue("{Sys.time()} Daily Split Failures [10 of 19]"))
=======
print(glue("{Sys.time()} Daily Split Failures [10 of 19]"))
>>>>>>> 7673cc95a2daa422724e9e2e4c047a9c6349a375

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

}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})

# HOURLY SPLIT FAILURES #######################################################

print(glue("{Sys.time()} Hourly Split Failures [11 of 19]"))
<<<<<<< HEAD

tryCatch({
    sfh <- get_sf_by_hr(sf)

    msfh <- get_monthly_sf_by_hr(sfh)
=======
>>>>>>> 7673cc95a2daa422724e9e2e4c047a9c6349a375

    # Hourly volumes by Corridor --------------------------------------------------
    cor_msfh <- get_cor_monthly_sf_by_hr(msfh, corridors)

    saveRDS(msfh, "msfh.rds")
    saveRDS(cor_msfh, "cor_msfh.rds")

    rm(sf)
    rm(sfh)
    rm(msfh)
    rm(cor_msfh)
    gc()
    
}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})



# DAILY QUEUE SPILLBACK #######################################################

print(glue("{Sys.time()} Daily Queue Spillback [12 of 19]"))
<<<<<<< HEAD

tryCatch({          
    qs <- f("qs_", month_abbrs)
    wqs <- get_weekly_qs_by_day(qs)
    cor_wqs <- get_cor_weekly_qs_by_day(wqs, corridors)

    monthly_qsd <- get_monthly_qs_by_day(qs)
    cor_monthly_qsd <- get_cor_monthly_qs_by_day(monthly_qsd, corridors)

    saveRDS(wqs, "wqs.rds")
    saveRDS(monthly_qsd, "monthly_qsd.rds")
    saveRDS(cor_wqs, "cor_wqs.rds")
    saveRDS(cor_monthly_qsd, "cor_monthly_qsd.rds")
=======
>>>>>>> 7673cc95a2daa422724e9e2e4c047a9c6349a375

}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})




# HOURLY QUEUE SPILLBACK ######################################################

print(glue("{Sys.time()} Hourly Queue Spillback [13 of 19]"))
<<<<<<< HEAD

tryCatch({          
    qsh <- get_qs_by_hr(qs)
    mqsh <- get_monthly_qs_by_hr(qsh)

    # Hourly volumes by Corridor --------------------------------------------------
    cor_mqsh <- get_cor_monthly_qs_by_hr(mqsh, corridors)

    saveRDS(mqsh, "mqsh.rds")
    saveRDS(cor_mqsh, "cor_mqsh.rds")
=======
>>>>>>> 7673cc95a2daa422724e9e2e4c047a9c6349a375

    rm(qs)
    rm(wqs)
    rm(monthly_qsd)
    rm(cor_wqs)
    rm(cor_monthly_qsd)
    rm(qsh)
    rm(mqsh)
    rm(cor_mqsh)
    gc()

}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})



# TRAVEL TIME AND BUFFER TIME INDEXES #########################################

print(glue("{Sys.time()} Travel Time Indexes [14 of 19]"))

<<<<<<< HEAD
tryCatch({
    fns <- list.files(path = "Inrix/For_Monthly_Report", 
                      pattern = "tt_.*_summary.csv$",
                      full.names = TRUE)
=======
# Raw data from massive data downloader file
fns <- list.files(path = "Inrix/For_Monthly_Report", 
                  pattern = "tt_.*_summary.csv$",
                  full.names = TRUE)
>>>>>>> 7673cc95a2daa422724e9e2e4c047a9c6349a375


    tt <- lapply(fns, read_csv) %>%
        bind_rows() %>%
        mutate(Corridor = factor(Corridor)) %>%
        arrange(Corridor, Hour)

    tti <- tt %>%
        select(-pti)

    pti <- tt %>%
        select(-tti)

    #tmc_corridors <- read_feather(conf$tmc_filename) %>% 
    #    select(tmc_code = tmc, Corridor, miles)
    #tt <- get_tt(fns, tmc_corridors)
    #tti <- tt$tti 
    #pti <- tt$pti 







    cor_monthly_tti_by_hr <- get_cor_monthly_ti_by_hr(tti, cor_monthly_vph, corridors)
    cor_monthly_pti_by_hr <- get_cor_monthly_ti_by_hr(pti, cor_monthly_vph, corridors)

    cor_monthly_tti <- get_cor_monthly_tti(cor_monthly_tti_by_hr, corridors)
    cor_monthly_pti <- get_cor_monthly_pti(cor_monthly_pti_by_hr, corridors)

    write_fst(tti, "tti.fst")
    write_fst(pti, "pti.fst")

    saveRDS(cor_monthly_tti, "cor_monthly_tti.rds")
    saveRDS(cor_monthly_tti_by_hr, "cor_monthly_tti_by_hr.rds")

    saveRDS(cor_monthly_pti, "cor_monthly_pti.rds")
    saveRDS(cor_monthly_pti_by_hr, "cor_monthly_pti_by_hr.rds")

    rm(tt)
    rm(tti)
    rm(pti)
    rm(cor_monthly_tti)
    rm(cor_monthly_tti_by_hr)
    rm(cor_monthly_pti)
    rm(cor_monthly_pti_by_hr)
    gc()

}, error = function(e) {
    print('ENCOUNTERED AN ERROR:')
    print(e)
})



# Package up for Flexdashboard

print(glue("{Sys.time()} Package for Monthly Report [17 of 19]"))

sigify <- function(df, cor_df, corridors) {
    
    df_ <- df %>% left_join(distinct(corridors, SignalID, Corridor, Name)) %>%
        rename(Signal_Group = Corridor, Corridor = SignalID) %>%
        ungroup() %>%
        mutate(Corridor = factor(Corridor))
    
    cor_df_ <- cor_df %>%
        filter(Corridor %in% unique(df_$Signal_Group)) %>%
        mutate(Signal_Group = Corridor)
    
    br <- bind_rows(df_, cor_df_)
    
    if ("Month" %in% names(br)) {
        br %>% arrange(Signal_Group, Corridor, Month)
    } else if ("Hour" %in% names(br)) {
        br %>% arrange(Signal_Group, Corridor, Hour)
    } else if ("Date" %in% names(br)) {
        br %>% arrange(Signal_Group, Corridor, Date)
    }
}




cor <- list()
cor$dy <- list("du" = readRDS("cor_avg_daily_detector_uptime.rds"),
               "cu" = readRDS("cor_daily_comm_uptime.rds"))
               #"pau" = readRDS("cor_daily_pa_uptime.rds"))
               #"cctv" = readRDS("cor_daily_cctv_uptime.rds"))
cor$wk <- list("vpd" = readRDS("cor_weekly_vpd.rds"),
               "vph" = readRDS("cor_weekly_vph.rds"),
               "vphp" = readRDS("cor_weekly_vph_peak.rds"),
               "papd" = readRDS("cor_weekly_papd.rds"),
               "paph" = readRDS( "cor_weekly_paph.rds"),
               "tp" = readRDS("cor_weekly_throughput.rds"),
               "aog" = readRDS("cor_weekly_aog_by_day.rds"),
               "pr" = readRDS("cor_weekly_pr_by_day.rds"),
               "qs" = readRDS("cor_wqs.rds"),
               "sf" = readRDS("cor_wsf.rds"),
               "cu" = readRDS("cor_weekly_comm_uptime.rds"))
               #"pau" = readRDS("cor_weekly_pa_uptime.rds"))                                                               
               #"cctv" =  readRDS("cor_weekly_cctv_uptime.rds"))
cor$mo <- list("vpd" = readRDS("cor_monthly_vpd.rds"),
               "vph" = readRDS("cor_monthly_vph.rds"),
               "vphp" = readRDS("cor_monthly_vph_peak.rds"),
               "papd" = readRDS("cor_monthly_papd.rds"),
               "paph" = readRDS("cor_monthly_paph.rds"),
               "tp" = readRDS("cor_monthly_throughput.rds"),
               "aogd" = readRDS("cor_monthly_aog_by_day.rds"),
               "aogh" = readRDS("cor_monthly_aog_by_hr.rds"),
               "prd" = readRDS("cor_monthly_pr_by_day.rds"),
               "prh" = readRDS("cor_monthly_pr_by_hr.rds"),
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
               #"pau" = readRDS("cor_monthly_pa_uptime.rds"),
               "veh" = readRDS("cor_monthly_detector_uptime.rds"),
               "ped" = data.frame())
               #"cctv" = readRDS("cor_monthly_cctv_uptime.rds"),
               #"events" = readRDS("cor_monthly_events.rds"))
cor$qu <- list("vpd" = get_quarterly(cor$mo$vpd, "vpd"),
               "vph" = data.frame(),
               "vphpa" = get_quarterly(cor$mo$vphp$am, "vph"),
               "vphpp" = get_quarterly(cor$mo$vphp$pm, "vph"),
               "tp" = get_quarterly(cor$mo$tp, "vph"),
               "aogd" = get_quarterly(cor$mo$aogd, "aog", "vol"),
               "prd" = get_quarterly(cor$mo$prd, "pr", "vol"),
               "qsd" = get_quarterly(cor$mo$qsd, "qs_freq"),
               "sfd" = get_quarterly(cor$mo$sfd, "sf_freq"),
               "tti" = get_quarterly(cor$mo$tti, "tti"),
               "pti" = get_quarterly(cor$mo$pti, "pti"),
               "du" = get_quarterly(cor$mo$du, "uptime.all"),
               "cu" = get_quarterly(cor$mo$cu, "uptime"),
<<<<<<< HEAD
               #"pau" = get_quarterly(cor$mo$pau, "uptime"),
=======
>>>>>>> 7673cc95a2daa422724e9e2e4c047a9c6349a375
               "veh" = get_quarterly(cor$mo$veh, "uptime.all"),
               "ped" = data.frame()) #get_quarterly(cor$mo$ped, "uptime", "num"), # -- Need to update
               #"cctv" = get_quarterly(cor$mo$cctv, "uptime", "num"),
               #"reported" = get_quarterly(cor$mo$events, "Reported"),
               #"resolved" =  get_quarterly(cor$mo$events, "Resolved"),
               #"outstanding" = get_quarterly(cor$mo$events, "Outstanding", operation = "latest"))

sig <- list()
sig$dy <- list("du" = sigify(readRDS("avg_daily_detector_uptime.rds"), cor$dy$du, corridors),
               "cu" = sigify(readRDS("daily_comm_uptime.rds"), cor$dy$cu, corridors))
               #"pau" = sigify(readRDS("daily_pa_uptime.rds"), cor$dy$pau, corridors))
               #"cctv" = readRDS("daily_cctv_uptime.rds"))
sig$wk <- list("vpd" = sigify(readRDS("weekly_vpd.rds"), cor$wk$vpd, corridors),
               "vph" = sigify(readRDS("weekly_vph.rds"), cor$wk$vph, corridors),
               "vphp" = purrr::map2(readRDS("weekly_vph_peak.rds"), cor$wk$vphp,
                                    function(x, y) { sigify(x, y, corridors) }),
               "papd" = sigify(readRDS("weekly_papd.rds"), cor$wk$papd, corridors),
               "paph" = sigify(readRDS("weekly_paph.rds"), cor$wk$paph, corridors),
               "tp" = sigify(readRDS("weekly_throughput.rds"), cor$wk$tp, corridors),
               "aog" = sigify(readRDS("weekly_aog_by_day.rds"), cor$wk$aog, corridors),
               "pr" = sigify(readRDS("weekly_pr_by_day.rds"), cor$wk$pr, corridors),
               "qs" = sigify(readRDS("wqs.rds"), cor$wk$qs, corridors),
               "sf" = sigify(readRDS("wsf.rds"), cor$wk$sf, corridors),
               "cu" = sigify(readRDS("weekly_comm_uptime.rds"), cor$wk$cu, corridors))
               #"pau" = sigify(readRDS("weekly_pa_uptime.rds"), cor$wk$pau, corridors))
               #"cctv" = readRDS("weekly_cctv_uptime.rds"))
sig$mo <- list("vpd" = sigify(readRDS("monthly_vpd.rds"), cor$mo$vpd, corridors),
               "vph" = sigify(readRDS("monthly_vph.rds"), cor$mo$vph, corridors),
               "vphp" = purrr::map2(readRDS("monthly_vph_peak.rds"), cor$mo$vphp,
                                    function(x, y) { sigify(x, y, corridors) }),
               "papd" = sigify(readRDS("monthly_papd.rds"), cor$mo$papd, corridors),
               "paph" = sigify(readRDS("monthly_paph.rds"), cor$mo$paph, corridors),
               "tp" = sigify(readRDS("monthly_throughput.rds"), cor$mo$tp, corridors),
               "aogd" = sigify(readRDS("monthly_aog_by_day.rds"), cor$mo$aogd, corridors),
               "aogh" = sigify(readRDS("monthly_aog_by_hr.rds"), cor$mo$aogh, corridors),
               "prd" = sigify(readRDS("monthly_pr_by_day.rds"), cor$mo$prd, corridors),
               "prh" = sigify(readRDS("monthly_pr_by_hr.rds"), cor$mo$prh, corridors),
               "qsd" = sigify(readRDS("monthly_qsd.rds"), cor$mo$qsd, corridors),
               "qsh" = sigify(readRDS("mqsh.rds"), cor$mo$qsh, corridors),
               "sfd" = sigify(readRDS("monthly_sfd.rds"), cor$mo$sfd, corridors),
               "sfh" = sigify(readRDS("msfh.rds"), cor$mo$sfh, corridors),
               "tti" = data.frame(),
               "pti" = data.frame(),
               "du" = sigify(readRDS("monthly_detector_uptime.rds"), cor$mo$du, corridors),
               "cu" = sigify(readRDS("monthly_comm_uptime.rds"), cor$mo$cu, corridors))
               #"cctv" = readRDS("monthly_cctv_uptime.rds"))

saveRDS(cor, "cor.rds")
saveRDS(sig, "sig.rds")


print(glue("{Sys.time()} upload to S3 [18 of 19]"))

aws.s3::put_object(file = "cor.rds", 
                   object = "cor.rds", 
                   bucket = "vdot-spm"
                   )
aws.s3::put_object(file = "sig.rds", 
                   object = "sig.rds", 
                   bucket = "vdot-spm"
                   )

# print(glue("{Sys.time()} build signal_dashboards [19 of 19]"))

print(glue("{Sys.time()} build signal_dashboards [19 of 19]"))

db_build_data_for_signal_dashboard(month_abbrs = month_abbrs[length(month_abbrs)], 
                                   corridors = corridors, 
                                   pth = 'Signal_Dashboards', 
                                   upload_to_s3 = TRUE)
