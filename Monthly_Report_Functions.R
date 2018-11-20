
# Monthly_Report_Functions.R
Sys.unsetenv("JAVA_HOME")
library(DBI)
library(RJDBC)			  
library(readxl)
library(readr)
library(dplyr)
library(tidyr)
library(stringr)
library(purrr)
library(lubridate)
library(glue)
library(data.table)
library(feather)
library(fst)
library(parallel)
library(pool)
library(httr)
library(aws.s3)
library(sf)
library(yaml)

library(plotly)
library(crosstalk)

library(reticulate)

if (Sys.info()["sysname"] == "Windows") {
    python_path <- file.path(dirname(path.expand("~")), "Anaconda3", "python.exe")
    
} else if (Sys.info()["sysname"] == "Linux") {
    python_path <- file.path("~", "miniconda3", "bin", "python")

} else {
    stop("Unknown operating system.")
}

use_python(python_path)

# Colorbrewer Paired Palette Colors
LIGHT_BLUE = "#A6CEE3";   BLUE = "#1F78B4"
LIGHT_GREEN = "#B2DF8A";  GREEN = "#33A02C"
LIGHT_RED = "#FB9A99";    RED = "#E31A1C"
LIGHT_ORANGE = "#FDBF6F"; ORANGE = "#FF7F00"
LIGHT_PURPLE = "#CAB2D6"; PURPLE = "#6A3D9A"
LIGHT_BROWN = "#FFFF99";  BROWN = "#B15928"

RED2 = "#e41a1c"
VDOT_BLUE = "#005da9"
#GDOT_BLUE = "#256194"

SUN = 1; MON = 2; TUE = 3; WED = 4; THU = 5; FRI = 6; SAT = 7

AM_PEAK_HOURS = c(6,7,8,9); PM_PEAK_HOURS = c(15,16,17,18)

aws_conf <- read_yaml("Monthly_Report_AWS.yaml")
Sys.setenv("AWS_ACCESS_KEY_ID" = aws_conf$AWS_ACCESS_KEY_ID,
           "AWS_SECRET_ACCESS_KEY" = aws_conf$AWS_SECRET_ACCESS_KEY,
           "AWS_DEFAULT_REGION" = aws_conf$AWS_DEFAULT_REGION)

if (Sys.info()["nodename"] %in% c("Larry")) { # The SAM or Larry # VDOT -- update if we need a proxy
    print(Sys.info()["nodename"]) 
} else { # shinyapps.io
    Sys.setenv(TZ="America/New_York")
}

SPM_BUCKET = "vdot-spm"

get_atspm_connection <- function() {
    
    if (Sys.info()["sysname"] == "Windows") {
        
        dbConnect(odbc::odbc(),
                  dsn = "vdot_atspm",
                  uid = Sys.getenv("VDOT_ATSPM_USERNAME"),
                  pwd = Sys.getenv("VDOT_ATSPM_PASSWORD"))
        
    } else if (Sys.info()["sysname"] == "Linux") {
        
        dbConnect(odbc::odbc(),  
                  driver = "FreeTDS",
                  server = Sys.getenv("VDOT_ATSPM_SERVER_INSTANCE"),
                  #database = Sys.getenv("VDOT_ATSPM_DB"),
                  port = 1433,
		          uid = Sys.getenv("VDOT_ATSPM_USERNAME"),
                  pwd = Sys.getenv("VDOT_ATSPM_PASSWORD"))
    }
} 


week <- function(d) {
    d0 <- ymd("2016-12-25")
    as.integer(trunc((ymd(d) - d0)/dweeks(1)))
}

get_month_abbrs <- function(start_date, end_date) {
    start_date <- ymd(start_date)
    day(start_date) <- 1
    end_date <- ymd(end_date)
    
    sapply(seq(start_date, end_date, by = "1 month"), as.character)
    
    #sapply(seq(ymd(start_date), ymd(end_date), by = "1 month"),
    #                  function(date_) { 
    #                      d <- ymd(date_)
    #                      y <- year(d)
    #                      m <- month(d)
    #                      s <- paste(y, sprintf("%02d", m), sep = "-")
    #                  })
}

# Takes a list of dataframe or mulitple dataframes as list(df1, df2, df3)
#  and binds rows while keeping factors
bind_rows_keep_factors <- function(...) {
    ## Identify all factors
    factors <- unique(unlist(
        map(..., ~ select_if(..., is.factor) %>% names())
    ))
    ## Bind dataframes, convert characters back to factors
    suppressWarnings(bind_rows(...)) %>% 
        mutate_at(vars(one_of(factors)), factor)  
}

match_type <- function(val, val_type_to_match) {
    eval(parse(text=paste0('as.',class(val_type_to_match), "(", val, ")")))
}



write_fst_ <- function(df, fn, append = FALSE) {
    if (append == TRUE & file.exists(fn)) {
        
        df_ <- read_fst(fn)
        df_ <- bind_rows_keep_factors(list(df, df_)) %>% distinct()
    } else {
        df_ <- df
    }
    write_fst(df_, fn)
}

get_corridors <- function(corr_fn) {
    readxl::read_xlsx(corr_fn) %>% 
        tidyr::unite(Name, c(`Main Street Name`, `Side Street Name`), sep = ' @ ') %>%
        transmute(SignalID = factor(SignalID), #SignalID=factor(`GDOT MaxView Device ID`), # VDOT -- update
                  Signal_Group = factor(`Signal Group`),
                  Corridor = factor(Corridor),
                  Milepost = as.numeric(Milepost),
                  Agency = Agency,
                  Name = Name,
                  Asof = date(Asof)) %>% 
        filter(grepl("^\\d.*", SignalID)) %>%
        filter(!is.na(Corridor)) %>%
        mutate(Description = paste(SignalID, Name, sep = ": "))
}

get_tmc_routes <- function(pth = "Inrix/For_Monthly_Report/2018-09/") { # VDOT -- Need to update pth

    fns <- list.files(pth, pattern = "*.zip", recursive = TRUE)
    
    df <- lapply(fns, function(fn) {
        read_csv(unz(file.path(pth, fn), "TMC_Identification.csv")) %>% 
            mutate(Corridor = get_corridor_name(fn))
    }) %>% bind_rows()
    df
}

# New as of 11/8/18
get_det_config <- function(date_) {
    fn <- glue('ATSPM_Det_Config_Good_{date_}.feather')
    if (!file.exists(fn)) {
	fn <- glue('ATSPM_Det_Config_Good.feather')
    }
    read_feather(fn) %>%
        transmute(SignalID = as.character(SignalID), 
                  Detector = as.integer(Detector), 
                  CallPhase = as.integer(CallPhase),
                  #CallPhase.atspm = as.integer(CallPhase_atspm),
                  #CallPhase.maxtime = as.integer(CallPhase_maxtime),
                  TimeFromStopBar = TimeFromStopBar)
}


get_unique_timestamps <- function(df) {
    df %>% 
        select(Timestamp) %>% 
        
        distinct() %>%
        mutate(SignalID = 0) %>%
        select(SignalID, Timestamp)
}

get_gaps <- function(df, signals_list) {
    
    start_date <- floor_date((df %>% 
                                  arrange(Timestamp) %>% 
                                  head(1))$Timestamp)
    end_date <- floor_date((df %>% 
                                arrange(Timestamp) %>% 
                                tail(1))$Timestamp + days(1))
    
    bookends <- expand.grid(SignalID = signals_list, 
                            Timestamp = c(start_date, start_date + days(1)))
    
    ts <- df %>% 
        collect() %>%
        distinct(SignalID, Timestamp) %>%

        bind_rows(., bookends) %>%
        distinct() %>%
        mutate(Date = date(Timestamp)) %>%
        arrange(SignalID, Timestamp) %>%
        
        group_by(SignalID, Date) %>%
        mutate(span = as.numeric(Timestamp - lag(Timestamp), units = "mins")) %>% 
        drop_na() %>%
        mutate(span = if_else(span > 15, span, 0)) %>%
        
        group_by(SignalID, Date) %>% 
        summarize(uptime = 1 - sum(span, na.rm = TRUE)/(60 * 24)) %>%
        ungroup()
}

get_counts5 <- function(df, det_config, units = "hours", date_, TWR_only = FALSE) {
    
    if (lubridate::wday(date_, label = TRUE) %in% c("Tue", "Wed", "Thu") || (TWR_only == FALSE)) {
        
        df %>%
            filter(EventCode == 82) %>%
            mutate(Timeperiod = floor_date(Timestamp, unit = units)) %>%
            group_by(SignalID, Detector = EventParam, Timeperiod) %>%
            summarize(vol = n()) %>%
            ungroup() %>%
            
            left_join(det_config, by = c("SignalID", "Detector")) %>%
            
            select(SignalID, Timeperiod, Detector, CallPhase, vol) %>%
            mutate(SignalID = factor(SignalID),
                   Detector = factor(Detector))
    } else {
        data.frame()
    }
}

get_counts2 <- function(date_, uptime = TRUE, counts = TRUE) {
    

    start_date <- date_
    end_time <- format(date(date_) + days(1) - seconds(0.1), "%Y-%m-%d %H:%M:%S.9")
    
    if (counts == TRUE) {
        det_config <- get_det_config(start_date) %>%
            select(SignalID, Detector, CallPhase)
    }

    uptime_sig <- data.frame()
    gaps_all <- data.frame()
    
    counts_1hr_csv_fn <- glue("counts_1hr_{start_date}.csv")
    counts_15min_csv_fn <- glue("counts_15min_{start_date}.csv")
    
    if (file.exists(counts_1hr_csv_fn)) {
	file.remove(counts_1hr_csv_fn)
    }
    if (file.exists(counts_15min_csv_fn)) {
	file.remove(counts_15min_csv_fn)
    }
        
    n <- length(signals_list)
    i <- 10
    splits <- rep(1:ceiling(n/i), each = i, length.out = n)
    
    lapply(split(signals_list, splits), function(signals_sublist) {
        
        gc()
        
        print(signals_sublist)
        
        signals_string <- paste(glue("'{signals_sublist}'"), collapse = ",")
        
        query = glue("SELECT * FROM Controller_Event_Log 
                     WHERE Timestamp BETWEEN '{start_date}' AND '{end_time}'
                     AND EventCode NOT IN (43,44) 
                     AND SignalID in ({signals_string})")
        
        
        
        conn <- get_atspm_connection()
        
        df <- dbGetQuery(conn, query)
        print(head(df))
        
        dbDisconnect(conn)
        
        if (uptime == TRUE) {

            uptime_sig <<- bind_rows(uptime_sig, get_gaps(df, signals_sublist))
            gaps_all <<- bind_rows(gaps_all, get_unique_timestamps(df)) %>% distinct()
        }
        
	    if (counts == TRUE) {
             
            # get 1hr counts
            counts_1hr <- get_counts5(df, 
                                      det_config, 
                                      units = "hours",
                                      date_ = date_,
                                      TWR_only = FALSE)
            write_csv(counts_1hr, 
                      counts_1hr_csv_fn, #glue("counts_1hr_{start_date}.csv"), 
                      append = TRUE)
            
            # get 15min counts
            counts_15min <- get_counts5(df, 
                                        det_config, 
                                        units = "15 minutes",
                                        date_ = date_,
                                        TWR_only = TRUE)
            if (nrow(counts_15min) > 0) {
                write_csv(counts_15min, 
                          counts_15min_csv_fn, #glue("counts_15min_{start_date}.csv"), 
                          append = TRUE)
            }
        }
    })
    gc()
    
    
    print("Reducing data. Second pass.")
    
    if (uptime == TRUE) {
        
        # Second pass on distinct data. Reduce to comm uptime for signals_sublist
        uptime_all <- get_gaps(gaps_all, signals_list = c(0)) %>%
            select(-SignalID, uptime_all = uptime)
        
        uptime_sig %>%
            ungroup() %>%
            left_join(uptime_all) %>%
            mutate(SignalID = factor(SignalID),
                   CallPhase = factor(0),
                   uptime = uptime + (1 - uptime_all),
                   Date_Hour = ymd_hms(paste(start_date, "00:00:00")),
                   Date = date(start_date),
                   DOW = wday(start_date), 
                   Week = week(start_date)) %>%
            select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, uptime) %>%
            write_fst(., glue("cu_{start_date}.fst"))
    }
    
    if (counts == TRUE) {
        
        read_csv(glue("counts_1hr_{start_date}.csv"),
                 col_names = c("SignalID", "Timeperiod", "Detector", "CallPhase", "vol")) %>%
            mutate(SignalID = factor(SignalID),
                   Detector = factor(Detector),
                   CallPhase = factor(CallPhase)) %>%
        write_fst(., glue("counts_1hr_{start_date}.fst"))
        
        file.remove(counts_1hr_csv_fn) #glue("counts_1hr_{start_date}.csv"))
        
        
        if (file.exists(counts_15min_csv_fn)) { #glue("counts_15min_{start_date}.csv"))) {
            
            read_csv(counts_15min_csv_fn, #glue("counts_15min_{start_date}.csv"),
                     col_names = c("SignalID", "Timeperiod", "Detector", "CallPhase", "vol")) %>%
                mutate(SignalID = factor(SignalID),
                       Detector = factor(Detector),
                       CallPhase = factor(CallPhase)) %>%
                write_fst(., glue("counts_15min_TWR_{start_date}.fst"))
            
            file.remove(counts_15min_csv_fn) #glue("counts_15min_{start_date}.csv"))
        }
        
    }
}

# revised version of get_filtered_counts. Needs more testing.
get_filtered_counts <- function(counts, interval = "1 hour") { # interval (e.g., "1 hour", "15 min")
    
    counts <- counts %>%
        mutate(SignalID = factor(SignalID),
               Detector = factor(Detector),
               CallPhase = factor(CallPhase)) %>%
        filter(!is.na(CallPhase))

    # Identify detectors/phases from detector config file. Expand.
    #  This ensures all detectors are included in the bad detectors calculation.
    all_days <- unique(date(counts$Timeperiod))
    det_config <- lapply(all_days, function(d) {
        all_timeperiods <- seq(ymd_hms(paste(d, "00:00:00")), ymd_hms(paste(d, "23:59:00")), by = "1 hour")
        fn <- glue("ATSPM_Det_Config_Good_{d}.feather")
    	if (!file.exists(fn)) {
	    fn <- glue("ATSPM_Det_Config_Good.feather")
	}
	read_feather(fn) %>% 
            transmute(SignalID = factor(SignalID), 
                      Detector = factor(Detector), 
                      CallPhase = factor(CallPhase)) %>% 
            expand(nesting(SignalID, Detector, CallPhase), Timeperiod = all_timeperiods)
    }) %>% bind_rows() %>%
        transmute(SignalID = factor(SignalID),
                  Timeperiod = Timeperiod,
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase)) 
    
    
    expanded_counts <- full_join(det_config, counts) %>%
        transmute(SignalID = factor(SignalID), 
                  Date = date(Timeperiod),
                  Timeperiod = Timeperiod,
                  Detector = factor(Detector), 
                  CallPhase = factor(CallPhase),
                  vol = vol,
                  vol0 = ifelse(is.na(vol), 0, vol)) %>%
        group_by(SignalID, CallPhase, Detector) %>% 
        arrange(SignalID, CallPhase, Detector, Timeperiod) %>% 
        mutate(delta_vol = vol0 - lag(vol0),
               Good = ifelse(is.na(vol) | 
                                 vol > 1000 | 
                                 is.na(delta_vol) | 
                                 abs(delta_vol) > 500 | 
                                 abs(delta_vol) == 0,
                             0, 1)) %>%
        select(-vol0)

    
    # bad day = any of the following:
    #    too many bad hours (60%) based on the above criteria
    #    mean absolute change in hourly volume > 200 
    bad_days <- expanded_counts %>% 
        filter(hour(Timeperiod) >= 5) %>%
        group_by(SignalID, CallPhase, Detector, Date = date(Timeperiod)) %>% 
        summarize(Good = sum(Good, na.rm = TRUE), 
                  All = n(), 
                  Pct_Good = as.integer(sum(Good, na.rm = TRUE)/n()*100),
                  mean_abs_delta = mean(abs(delta_vol), na.rm = TRUE)) %>% 
        
        # manually calibrated
        mutate(Good_Day = as.integer(ifelse(Pct_Good >= 70 & mean_abs_delta < 200, 1, 0))) %>%
        select(SignalID, CallPhase, Detector, Date, mean_abs_delta, Good_Day)
    
    # counts with the bad days taken out
    filtered_counts <- left_join(expanded_counts, bad_days) %>%
        mutate(vol = ifelse(Good_Day==1, vol, NA),
               Month_Hour = Timeperiod - days(day(Timeperiod) - 1),
               Hour = Month_Hour - months(month(Month_Hour) - 1))
    
    filtered_counts
}

get_adjusted_counts <- function(filtered_counts) {
    
    filtered_counts <- mutate(filtered_counts, DOW = wday(Timeperiod))
    
    # signal|callphase|timeperiod where there is at least one na in vol
    na_vol_counts <- filtered_counts %>% 
        group_by(SignalID, CallPhase, Timeperiod) %>% 
        summarize(na.vol = sum(is.na(vol))) %>% 
        filter(na.vol > 0)
    
    # exclude above from ph_contr calc (below)
    fc <- anti_join(filtered_counts, na_vol_counts)
    
    # phase contribution factors--fraction of phase volume a detector contributes
    ph_contr <- fc %>% 
        group_by(SignalID, CallPhase, Detector) %>% 
        summarize(vol = sum(vol, na.rm = TRUE)) %>% 
        group_by(SignalID, CallPhase) %>% 
        mutate(Ph_Contr = vol/sum(vol, na.rm = TRUE)) %>% 
        select(-vol) %>% ungroup()
        
    # SignalID | CallPhase | Detector | Ph_Contr
    

    # fill in missing detectors from other detectors on that phase
    fc_phc <- left_join(filtered_counts, ph_contr) %>% 
        # fill in missing detectors from other detectors on that phase
        group_by(SignalID, Timeperiod, CallPhase) %>%
        mutate(mvol = mean(vol/Ph_Contr, na.rm = TRUE)) %>% ungroup()

    fc_phc$vol[is.na(fc_phc$vol)] <- as.integer(fc_phc$mvol[is.na(fc_phc$vol)] * fc_phc$Ph_Contr[is.na(fc_phc$vol)])
    
    #hourly volumes over the month to fill in missing data for all detectors in a phase
    mo_hrly_vols <- fc_phc %>%
        group_by(SignalID, CallPhase, Detector, DOW, Month_Hour) %>% 
        summarize(Hourly_Volume = median(vol, na.rm = TRUE)) %>%
        ungroup()
    # SignalID | Call.Phase | Detector | Month_Hour | Volume(median)
        
    # fill in missing detectors by hour and day of week volume in the month
    left_join(fc_phc, mo_hrly_vols) %>%
        ungroup() %>%
        mutate(vol = if_else(is.na(vol), as.integer(Hourly_Volume), as.integer(vol))) %>%
        
        select(SignalID, CallPhase, Detector, Timeperiod, vol) %>%
        
        filter(!is.na(vol))
    
    # SignalID | CallPhase | Timeperiod | Detector | vol 
}


get_spm_data <- function(start_date, end_date, signals_list, table, TWR_only=TRUE) {
    
    conn <- get_atspm_connection()
    
    if (TWR_only==TRUE) {
        query_where <- "WHERE DATEPART(dw, CycleStart) in (3,4,5)"
    } else {
        query_where <- ""
    }
    
    query <- paste("SELECT * FROM", table, query_where)
    
    df <- tbl(conn, sql(query)) 
    
    end_date1 <- as.character(ymd(end_date) + days(1))
    
    dplyr::filter(df, CycleStart >= start_date & CycleStart < end_date1 &
                      SignalID %in% signals_list)
}
get_spm_data_aws <- function(start_date, end_date, signals_list, table, TWR_only=TRUE) {
    
    drv <- JDBC(driverClass = "com.simba.athena.jdbc.Driver",
                classPath = "../../AthenaJDBC42_2.0.2.jar",
                identifier.quote = "'")
    
    if (Sys.info()["nodename"] == "GOTO3213490") { # The SAM

        conn <- dbConnect(drv, "jdbc:awsathena://athena.us-east-1.amazonaws.com:443/",
                          s3_staging_dir = 's3://gdot-spm-athena', # VDOT -- update
                          user = Sys.getenv("AWS_ACCESS_KEY_ID"),
                          password = Sys.getenv("AWS_SECRET_ACCESS_KEY"),
                          ProxyHost = "gdot-enterprise",
                          ProxyPort = "8080",
                          ProxyUID = Sys.getenv("GDOT_USERNAME"),
                          ProxyPWD = Sys.getenv("GDOT_PASSWORD"))
    } else {
        
        conn <- dbConnect(drv, "jdbc:awsathena://athena.us-east-1.amazonaws.com:443/",
                          s3_staging_dir = 's3://gdot-spm-athena', # VDOT -- update
                          user = Sys.getenv("AWS_ACCESS_KEY_ID"),
                          password = Sys.getenv("AWS_SECRET_ACCESS_KEY"))
    }


    
    if (TWR_only == TRUE) {
        query_where <- "WHERE date_format(date_parse(date, '%Y-%m-%d'), '%W') in ('Tuesday','Wednesday','Thursday')"
    } else {
        query_where <- ""
    }
    
    query <- paste("SELECT * FROM", paste0("gdot_spm.", tolower(table)), query_where)
													  
    df <- tbl(conn, sql(query))
    
    end_date1 <- ymd(end_date) + days(1)
    
    signals_list <- as.integer(signals_list)
								 
    dplyr::filter(df, date >= start_date & date < end_date1) # &
                      #signalid %in% signals_list)
}
# Query Cycle Data
get_cycle_data <- function(start_date, end_date, signals_list) {
    get_spm_data_aws(start_date, end_date, signals_list, table="CycleData")
}
# Query Detection Events
get_detection_events <- function(start_date, end_date, signals_list) {
    get_spm_data_aws(start_date, end_date, signals_list, table="DetectionEvents")
}


get_detector_uptime <- function(filtered_counts_1hr) {
    filtered_counts_1hr %>%
        ungroup() %>%
        distinct(Date, SignalID, Detector, Good_Day) %>%
        complete(nesting(SignalID, Detector), Date = seq(min(Date), max(Date), by="day")) %>%
        arrange(Date, SignalID, Detector)
}

get_bad_detectors <- function(filtered_counts_1hr) {
    get_detector_uptime(filtered_counts_1hr) %>%
        filter(Good_Day == 0)
}



# Volume VPD
get_vpd <- function(counts) {
    
    counts %>%
        filter(CallPhase %in% c(2,6)) %>% # sum over Phases 2,6 # added 4/24/18
        mutate(DOW = wday(Timeperiod), 
               Week = week(date(Timeperiod)),
               Date = date(Timeperiod)) %>% 
        group_by(SignalID, CallPhase, Week, DOW, Date) %>% 
        summarize(vpd = sum(vol, na.rm = TRUE))
    
    # SignalID | CallPhase | Week | DOW | Date | vpd
}

# SPM Throughput
get_thruput <- function(counts) {
    counts %>%
        mutate(DOW = wday(Date), 
               Week = week(Date)) %>%
        group_by(SignalID, Week, DOW, Date, Timeperiod) %>%
        summarize(vph = sum(vol, na.rm = TRUE)) %>%
        
        group_by(SignalID, Week, DOW, Date) %>%
        summarize(vph = quantile(vph, probs=c(0.95), na.rm = TRUE) * 4) %>%
        ungroup() %>%
        
        arrange(SignalID, Date) %>%
        mutate(CallPhase = 0) %>%
        select(SignalID, CallPhase, Date, Week, DOW, vph)
    
    # SignalID | CallPhase | Date | Week | DOW | vph
}

# SPM Arrivals on Green -- modified for use with dbplyr on AWS Athena
get_aog <- function(cycle_data) {
    
    df <- cycle_data %>% 
        filter(Phase %in% c(2,6)) %>%
        group_by(SignalID, Phase, CycleStart, EventCode) %>% 
        summarize(Volume = sum(Volume, na.rm = TRUE)) %>% 
        group_by(SignalID, Phase, CycleStart) %>% 
        mutate(Total_Volume = sum(Volume, na.rm = TRUE),
               Total_Volume = ifelse(Total_Volume == 0, 1, Total_Volume),
               CallPhase = Phase) %>% 
        filter(EventCode == 1) %>%

						 
        group_by(SignalID = SignalID, CallPhase, 
                 Hour = date_trunc('hour', CycleStart)) %>%
        summarize(vol = sum(Total_Volume, na.rm = TRUE),
                  aog = sum(Volume, na.rm = TRUE)/sum(Total_Volume, na.rm = TRUE)) %>%
        collect %>% 
        ungroup() %>%
        
        mutate(SignalID = factor(SignalID),
               vol = as.integer(vol),
               CallPhase = factor(CallPhase),
               Date_Hour = lubridate::ymd_hms(Hour),
               Date = date(Date_Hour),
               DOW = wday(Date), 
               Week = week(Date)) %>%
        #ungroup() %>%
        select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, aog, vol)

    # SignalID | CallPhase | Date_Hour | Date | Hour | aog | vol
}
get_daily_aog <- function(aog) {
    
    aog %>%
        ungroup() %>%
        mutate(DOW = wday(Date), 
               Week = week(Date),
               CallPhase = factor(CallPhase)) %>%
        filter(DOW %in% c(TUE,WED,THU) & (hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))) %>%
        group_by(SignalID, CallPhase, Date, Week, DOW) %>%
        summarize(aog = weighted.mean(aog, vol, na.rm = TRUE), vol = sum(vol, na.rm = TRUE))
    
    # SignalID | CallPhase | Date | Week | DOW | aog | vol
}

# SPM Split Failures
get_sf <- function(df) {
    df %>% mutate(SignalID = factor(SignalID),
                 CallPhase = factor(Phase),
                 Date_Hour = lubridate::ymd_hms(Hour),
                 Date = date(Date_Hour),
                 DOW = wday(Date), 
                 Week = week(Date)) %>%
    select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, sf, cycles, sf_freq) %>%
    as_tibble()
    
    # SignalID | CallPhase | Date | Date_Hour | DOW | Week | sf | cycles | sf_freq
}

# SPM Queue Spillback
get_qs <- function(detection_events) {
    
    qs <- detection_events %>% 
        filter(Phase %in% c(2,6)) %>%
        
        group_by(SignalID,
                 CallPhase = Phase,
                 CycleStart) %>%
        summarize(occ = approx_percentile(DetDuration, 0.95)) %>%
        ungroup() %>%
        group_by(SignalID, 
                 CallPhase,
                 Hour = date_trunc('hour', CycleStart)) %>%
        summarize(cycles = n(), 
                  qs = count_if(occ > 3)) %>%
        collect() %>%
        ungroup() %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase),
               Date_Hour = ymd_hms(Hour),
               Date = date(lubridate::floor_date(Date_Hour, unit="days")),
               DOW = wday(Date_Hour),
               Week = week(Date),
               qs = as.integer(qs),
               cycles = as.integer(cycles),
               qs_freq = as.double(qs)/as.double(cycles)) %>%
        select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, qs, cycles, qs_freq)
    
    dates <- seq(min(qs$Date), max(qs$Date), by = "1 day")
    
    dc <- lapply(dates, function(d) {
        fn <- glue("../ATSPM_Det_Config_Good_{d}.feather")
        read_feather(fn) %>% mutate(Date = ymd(d))
    }) %>% 
        bind_rows %>% 
        as_tibble() %>% 
        select(Date, SignalID, CallPhase, TimeFromStopBar) %>% # = TimeFromStopBar.atspm) %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase))
    
    qs %>% left_join(dc) %>% filter(TimeFromStopBar > 0)
    
    # SignalID | CallPhase | Date | Date_Hour | DOW | Week | qs | cycles | qs_freq
}

get_tt_csv <- function(fns) {

    dfs <- lapply(fns, function(f) {
        data.table::fread(f) %>%
            mutate(measurement_tstamp = ymd_hms(measurement_tstamp),
                   travel_time_seconds = travel_time_minutes * 60,
                   Corridor = get_corridor_name(f)) %>%
            filter(wday(measurement_tstamp) %in% c(TUE,WED,THU)) %>%
            mutate(miles = speed /3600 * travel_time_seconds,
                   ref_sec = miles/reference_speed * 3600) %>%
            group_by(Corridor = factor(Corridor), measurement_tstamp) %>%
            summarize(travel_time_seconds = sum(travel_time_seconds, na.rm = TRUE),
                      ref_sec = sum(ref_sec, na.rm = TRUE),
                      miles = sum(miles, na.rm = TRUE)) %>%
            ungroup()
    })
    df <- bind_rows(dfs) %>%
        group_by(Corridor = factor(Corridor),
                 measurement_tstamp) %>%
        summarize(travel_time_seconds = sum(travel_time_seconds, na.rm = TRUE),
                  ref_sec = sum(ref_sec, na.rm = TRUE),
                  miles = sum(miles, na.rm = TRUE)) %>%
        ungroup() %>%
        mutate(tti = travel_time_seconds/ref_sec,
               date_hour = floor_date(measurement_tstamp, "hours"),
               hour = date_hour - days(day(date_hour) - 1)) %>%
        group_by(Corridor, hour) %>%
        summarize(tti = mean(travel_time_seconds/ref_sec, na.rm = TRUE),
                  pti = quantile(travel_time_seconds, c(0.90))/mean(ref_sec, na.rm = TRUE)) %>%
        ungroup()

    tti <- select(df, Corridor, Hour = hour, tti) %>% as_tibble()
    pti <- select(df, Corridor, Hour = hour, pti) %>% as_tibble()

    list("tti" = tti, "pti" = pti)

    # Corridor | hour | idx | value
}



# -- Generic Aggregation Functions
get_Tuesdays <- function(df) {
    dates_ <- seq(min(df$Date) - days(6), max(df$Date) + days(6), by = "days")
    tuesdays <- dates_[wday(dates_) == 3]
    
    tuesdays <- pmax(min(df$Date), tuesdays) # unsure of this. need to test
    
    data.frame(Week = week(tuesdays), Date = tuesdays)
}

weighted_mean_by_corridor_ <- function(df, per_, corridors, var_, wt_=NULL) {
    
    per_ <- as.name(per_)
    
    gdf <- left_join(df, corridors) %>%
        mutate(Corridor = factor(Corridor)) %>%
        group_by(Zone, Corridor, Zone_Group, !!per_) 
    
    if (is.null(wt_)) {
        gdf %>% 
            summarize(!!var_ := mean(!!var_, na.rm = TRUE)) %>%
            mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
            ungroup() %>%
            select(Zone, Corridor, Zone_Group, !!per_, !!var_, delta)
    } else {
        gdf %>% 
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                      !!wt_ := sum(!!wt_, na.rm = TRUE)) %>%
            mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
            ungroup() %>%
            select(Zone, Corridor, Zone_Group, !!per_, !!var_, !!wt_, delta)
    }
}
group_corridor_by_ <- function(df, per_, var_, wt_, corr_grp) {
    df %>%
        group_by(!!per_) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                  !!wt_ := sum(!!wt_, na.rm = TRUE)) %>%
        mutate(Corridor = factor(corr_grp)) %>%
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
        mutate(Zone_Group = corr_grp) %>% 
        select(Corridor, Zone_Group, !!per_, !!var_, !!wt_, delta) 
}
group_corridor_by_sum_ <- function(df, per_, var_, wt_, corr_grp) {
    df %>%
        group_by(!!per_) %>%
        summarize(!!var_ := sum(!!var_, na.rm = TRUE)) %>%
        mutate(Corridor = factor(corr_grp)) %>%
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
        mutate(Zone_Group = corr_grp) %>% 
        select(Corridor, Zone_Group, !!per_, !!var_, delta) 
}

group_corridor_by_date <- function(df, var_, wt_, corr_grp) {
    group_corridor_by_(df, as.name("Date"), var_, wt_, corr_grp)
}
group_corridor_by_month <- function(df, var_, wt_, corr_grp) {
    group_corridor_by_(df, as.name("Month"), var_, wt_, corr_grp)
}
group_corridor_by_hour <- function(df, var_, wt_, corr_grp) {
    group_corridor_by_(df, as.name("Hour"), var_, wt_, corr_grp)
}

group_corridors_ <- function(df, per_, var_, wt_, gr_ = group_corridor_by_) {
    
    per_ <- as.name(per_)
    
    zgs <- lapply(c("RTOP1", "RTOP2", "D1", "D2", "D3", "D4", "D5", "D6", "Zone 7", "Cobb County"), function(zg) { # VDOT -- update
        df %>%
            filter(Zone_Group == zg) %>%
            gr_(per_, var_, wt_, zg)
    })
    all_rtop_df_out <- df %>%
        filter(Zone_Group %in% c("RTOP1", "RTOP2")) %>% # VDOT -- update
        gr_(per_, var_, wt_, "All RTOP")

    dplyr::bind_rows(select_(df, "Corridor", "Zone_Group", per_, var_, wt_, "delta"),
                     zgs,
                     all_rtop_df_out) %>%
        mutate(Corridor = factor(Corridor))
}

get_daily_avg <- function(df, var_, wt_ = "ones", peak_only = FALSE) {
    
    var_ <- as.name(var_)
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    
    if (peak_only == TRUE) {
        df <- df %>%
            filter((hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS)))
    }

    df %>%
        group_by(SignalID, Date) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_, na.rm = TRUE)) %>% # Sum of phases 2,6
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
        select(SignalID, Date, !!var_, !!wt_, delta)
    
    # SignalID | Date | var_ | wt_ | delta
}
get_daily_sum <- function(df, var_, per_) {
    
    var_ <- as.name(var_)
    per_ <- as.name(per_)
    
    
    df %>%
        complete(nesting(SignalID, CallPhase), !!var_ := full_seq(!!var_, 1)) %>%
        group_by(SignalID, !!per_) %>% 
        summarize(!!var_ := sum(!!var_, na.rm = TRUE)) %>%
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
        select(SignalID, !!per_, !!var_, delta)
}

get_weekly_sum_by_day <- function(df, var_) {
    
    var_ <- as.name(var_)
    
    Tuesdays <- get_Tuesdays(df)
    
    df %>%
        group_by(SignalID, CallPhase, Week) %>% 
        summarize(!!var_ := mean(!!var_, na.rm = TRUE)) %>% # Mean over 3 days in the week
        
        group_by(SignalID, Week) %>% 
        summarize(!!var_ := sum(!!var_, na.rm = TRUE)) %>% # Sum of phases 2,6
        
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
        left_join(Tuesdays) %>%
        select(SignalID, Date, Week, !!var_, delta)
    
    # SignalID | Date | var_
}
get_weekly_avg_by_day <- function(df, var_, wt_ = "ones", peak_only = TRUE) {
    
    var_ <- as.name(var_)
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    
    Tuesdays <- get_Tuesdays(df)
    
    if (peak_only == TRUE) {
        df <- df %>%
            filter((hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS)))
    }
    
    df %>%
        complete(nesting(SignalID, CallPhase), Week = full_seq(Week, 1)) %>%
        group_by(SignalID, CallPhase, Week) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                  !!wt_ := sum(!!wt_, na.rm = TRUE)) %>% # Mean over 3 days in the week
        
        group_by(SignalID, Week) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_, na.rm = TRUE)) %>% # Sum of phases 2,6
        
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
        left_join(Tuesdays) %>%
        select(SignalID, Date, Week, !!var_, !!wt_, delta)
    
    # SignalID | Date | vpd
}
get_cor_weekly_avg_by_day <- function(df, corridors, var_, wt_ = "ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    cor_df_out <- weighted_mean_by_corridor_(df, "Date", corridors, var_, wt_) %>%
        filter(!is.nan(!!var_))
    
    group_corridors_(cor_df_out, "Date", var_, wt_) %>%
        mutate(Week = week(Date))
}
get_monthly_avg_by_day <- function(df, var_, wt_ = NULL, peak_only = FALSE) {
    
    var_ <- as.name(var_)
    
    if (peak_only == TRUE) {
        df <- df %>%
            filter((hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS)))
    }
    
    current_month <- max(df$Date)
    
    gdf <- df %>%
        mutate(Month = Date - days(day(Date) - 1)) %>%
        group_by(SignalID, CallPhase) %>%
        complete(nesting(SignalID, CallPhase), Month = seq(min(Month), current_month, by = "1 month")) %>%
        group_by(SignalID, CallPhase, Month)
        
    if (is.null(wt_)) {
        rdf <- gdf %>%
            summarize(!!var_ := mean(!!var_, na.rm = TRUE)) %>%
            group_by(SignalID, Month) %>%
            summarize(!!var_ := sum(!!var_, na.rm = TRUE)) %>% # Sum over Phases (2,6)
            mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_))
    } else {
        wt_ <- as.name(wt_)
        rdf <- gdf %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                      !!wt_ := sum(!!wt_, na.rm = TRUE)) %>%
            group_by(SignalID, Month) %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean over Phases(2,6)
                      !!wt_ := sum(!!wt_, na.rm = TRUE)) %>%
            mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_))
    }
    rdf
}
get_cor_monthly_avg_by_day <- function(df, corridors, var_, wt_="ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    cor_df_out <- weighted_mean_by_corridor_(df, "Month", corridors, var_, wt_) %>%
        filter(!is.nan(!!var_))

    group_corridors_(cor_df_out, "Month", var_, wt_)
}

get_weekly_avg_by_hr <- function(df, var_, wt_ = NULL) {
    
    var_ <- as.name(var_)
    
    Tuesdays <- df %>% 
        mutate(Date = date(Hour)) %>%
        get_Tuesdays()
    
    df_ <- left_join(df, Tuesdays) %>%
        filter(!is.na(Date))
    year(df_$Hour) <- year(df_$Date)
    month(df_$Hour) <- month(df_$Date)
    day(df_$Hour) <- day(df_$Date)
    
    gdf <- df_ %>%
        group_by(SignalID, CallPhase, Week, Hour) 
    
    if (is.null(wt_)) {
        gdf %>%
            summarize(!!var_ := mean(!!var_, na.rm = TRUE)) %>% # Mean over 3 days in the week
            group_by(SignalID, Week, Hour) %>% 
            summarize(!!var_ := mean(!!var_, na.rm = TRUE)) %>% # Mean of phases 2,6
            mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
            select(SignalID, Hour, Week, !!var_, delta)
    } else {
        wt_ <- as.name(wt_)
        gdf %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                      !!wt_ := sum(!!wt_, na.rm = TRUE)) %>% # Mean over 3 days in the week
            group_by(SignalID, Week, Hour) %>% 
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                      !!wt_ := sum(!!wt_, na.rm = TRUE)) %>% # Sum of phases 2,6
            mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_)) %>%
            select(SignalID, Hour, Week, !!var_, !!wt_, delta)
    }
}
get_cor_weekly_avg_by_hr <- function(df, corridors, var_, wt_="ones") {

    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    cor_df_out <- weighted_mean_by_corridor_(df, "Hour", corridors, var_, wt_)
    
    group_corridors_(cor_df_out, "Hour", var_, wt_)
}

get_sum_by_hr <- function(df, var_) {
    
    var_ <- as.name(var_)

    df %>%  
        mutate(DOW = wday(Timeperiod),
               Week = week(date(Timeperiod)),
               Hour = floor_date(Timeperiod, unit = '1 hour')) %>%
        group_by(SignalID, CallPhase, Week, DOW, Hour) %>%
        summarize(!!var_ := sum(!!var_, na.rm = TRUE)) %>%
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_))
    
    # SignalID | CallPhase | Week | DOW | Hour | var_ | delta
    
} ## unused. untested

get_avg_by_hr <- function(df, var_, wt_=NULL) {
    
    df_ <- as.data.table(df)

    df_[, c("DOW", "Week", "Hour") := list(wday(Date_Hour), 
                                           week(date(Date_Hour)), 
                                           floor_date(Date_Hour, unit = '1 hour'))]
    if (is.null(wt_)) {
        ret <- df_[, .(mean(get(var_)), 1), 
                   by = .(SignalID, CallPhase, Week, DOW, Hour)]
    } else {
        ret <- df_[, .(weighted.mean(get(var_), get(wt_), na.rm = TRUE),
                       sum(get(wt_), na.rm = TRUE)), 
                   by = .(SignalID, CallPhase, Week, DOW, Hour)]
        ret <- ret[, c((var_), (wt_)) := .(V1, V2)][, -(V1:V2)]
        ret <- ret[, delta := (get(var_) - shift(get(var_), 1, type = "lag"))/shift(get(var_), 1, type = "lag"),
            by = .(SignalID, CallPhase)]
    }
    ret
}

get_monthly_avg_by_hr <- function(df, var_, wt_ = "ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    df %>% 
        group_by(SignalID, Hour) %>% 
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                  !!wt_ := sum(!!wt_, na.rm = TRUE)) %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                  !!wt_ := sum(!!wt_, na.rm = TRUE)) %>%
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_))
    
    # SignalID | CallPhase | Hour | vph
}
get_cor_monthly_avg_by_hr <- function(df, corridors, var_, wt_ = "ones") {
    
    if (wt_ == "ones") {
        wt_ <- as.name(wt_)
        df <- mutate(df, !!wt_ := 1)
    } else {
        wt_ <- as.name(wt_)
    }
    var_ <- as.name(var_)
    
    cor_df_out <- weighted_mean_by_corridor_(df, "Hour", corridors, var_, wt_)
    group_corridors_(cor_df_out, "Hour", var_, wt_)
}

# -- end Generic Aggregation Functions

get_daily_detector_uptime <- function(filtered_counts) {
    
    bad_comms <- filtered_counts %>%
        group_by(SignalID, Timeperiod) %>%
        summarize(vol = sum(vol, na.rm = TRUE)) %>%
        dplyr::filter(vol == 0) %>%
        select(-vol)
    fc <- anti_join(filtered_counts, bad_comms)
    
    ddu <- fc %>% 
        mutate(Date_Hour = Timeperiod,
               Date = date(Date_Hour)) %>%
        select(SignalID, CallPhase, Detector, Date, Date_Hour, Good_Day) %>%
        ungroup() %>%
        mutate(setback = ifelse(CallPhase %in% c(2,6), "Setback", "Presence"),
               setback = factor(setback),
               SignalID = factor(SignalID)) %>%
        group_by(SignalID, CallPhase, Date, Date_Hour, setback) %>%
        summarize(uptime = as.double(sum(Good_Day, na.rm = TRUE))/as.double(n()), 
                  all = as.double(n()))
    split(ddu, ddu$setback)
}
get_avg_daily_detector_uptime <- function(daily_detector_uptime) {
    
    sb_daily_uptime <- get_daily_avg(daily_detector_uptime$Setback, "uptime", "all", peak_only = FALSE)
    pr_daily_uptime <- get_daily_avg(daily_detector_uptime$Presence, "uptime", "all", peak_only = FALSE)
    
    full_join(sb_daily_uptime, pr_daily_uptime, 
              by = c("SignalID", "Date"), 
              suffix = c(".sb", ".pr")) %>%
        rowwise() %>%
        mutate(uptime.all = weighted.mean(c(uptime.sb, uptime.pr), c(all.sb, all.pr), na.rm = TRUE)) %>%
        select(-starts_with("delta")) %>% 
        ungroup()
}
get_cor_avg_daily_detector_uptime <- function(avg_daily_detector_uptime, corridors) {
    
    cor_daily_sb_uptime <- avg_daily_detector_uptime %>% 
        filter(!is.na(uptime.sb)) %>%
        get_cor_weekly_avg_by_day(corridors, "uptime.sb", "all.sb")
    
    cor_daily_pr_uptime <- avg_daily_detector_uptime %>% 
        filter(!is.na(uptime.pr)) %>%
        get_cor_weekly_avg_by_day(corridors, "uptime.pr", "all.pr")
    
    cor_daily_all_uptime <- avg_daily_detector_uptime %>% 
        filter(!is.na(uptime.all)) %>%
        get_cor_weekly_avg_by_day(corridors, "uptime.all", "ones")
    
    
    full_join(select(cor_daily_sb_uptime, -c(all.sb, delta)),
              select(cor_daily_pr_uptime, -c(all.pr, delta))) %>%
        left_join(select(cor_daily_all_uptime, -c(ones, delta))) %>%
        mutate(Zone_Group = factor(Zone_Group))
}

get_weekly_vpd <- function(vpd) {
    vpd <- filter(vpd, DOW %in% c(TUE,WED,THU)) 
    get_weekly_sum_by_day(vpd, "vpd")
}
get_weekly_thruput <- function(throughput) {
    get_weekly_sum_by_day(throughput, "vph")
}
get_weekly_aog_by_day <- function(daily_aog) {
    get_weekly_avg_by_day(daily_aog, "aog", "vol", peak_only = TRUE)
}
get_weekly_sf_by_day <- function(sf) {
    get_weekly_avg_by_day(sf, "sf_freq", "cycles", peak_only = TRUE)
}
get_weekly_qs_by_day <- function(qs) {
    get_weekly_avg_by_day(qs, "qs_freq", "cycles", peak_only = TRUE)
}

get_cor_weekly_vpd <- function(weekly_vpd, corridors) {
    get_cor_weekly_avg_by_day(weekly_vpd, corridors, "vpd")
}
get_cor_weekly_thruput <- function(weekly_throughput, corridors) {
    get_cor_weekly_avg_by_day(weekly_throughput, corridors, "vph")
}
get_cor_weekly_aog_by_day <- function(weekly_aog, corridors) {
    get_cor_weekly_avg_by_day(weekly_aog, corridors, "aog", "vol")
}
get_cor_weekly_sf_by_day <- function(weekly_sf, corridors) {
    get_cor_weekly_avg_by_day(weekly_sf, corridors, "sf_freq", "cycles")
}
get_cor_weekly_qs_by_day <- function(weekly_qs, corridors) {
    get_cor_weekly_avg_by_day(weekly_qs, corridors, "qs_freq", "cycles")
}

get_monthly_vpd <- function(vpd) {
    vpd <- filter(vpd, DOW %in% c(TUE,WED,THU)) 
    get_monthly_avg_by_day(vpd, "vpd", peak_only = FALSE)
}
get_monthly_thruput <- function(throughput) {
    get_monthly_avg_by_day(throughput, "vph", peak_only = FALSE)
}
get_monthly_aog_by_day <- function(daily_aog) {
    get_monthly_avg_by_day(daily_aog, "aog", "vol", peak_only = TRUE)
}
get_monthly_sf_by_day <- function(sf) {
    get_monthly_avg_by_day(sf, "sf_freq", "cycles", peak_only = TRUE)
}
get_monthly_qs_by_day <- function(qs) {
    get_monthly_avg_by_day(qs, "qs_freq", "cycles", peak_only = TRUE)
}

get_cor_monthly_vpd <- function(monthly_vpd, corridors) {
    get_cor_monthly_avg_by_day(monthly_vpd, corridors, "vpd")
}
get_cor_monthly_thruput <- function(monthly_throughput, corridors) {
    get_cor_monthly_avg_by_day(monthly_throughput, corridors, "vph")
}
get_cor_monthly_aog_by_day <- function(monthly_aog_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_aog_by_day, corridors, "aog", "vol")
}
get_cor_monthly_sf_by_day <- function(monthly_sf_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_sf_by_day, corridors, "sf_freq", "cycles")
}
get_cor_monthly_qs_by_day <- function(monthly_qs_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_qs_by_day, corridors, "qs_freq", "cycles")
}
get_cor_monthly_tti <- function(cor_monthly_tti_by_hr, corridors) {
    cor_monthly_tti_by_hr %>% 
        mutate(Month = as_date(Hour)) %>%
        filter(!Corridor %in% c("RTOP1", "RTOP2", "All RTOP", "D5", "Zone 7")) %>% # VDOT -- update
        get_cor_monthly_avg_by_day(corridors, "tti", "pct")
}
get_cor_monthly_pti <- function(cor_monthly_pti_by_hr, corridors) {
    cor_monthly_pti_by_hr %>% mutate(Month = as_date(Hour))  %>%
        filter(!Corridor %in% c("RTOP1", "RTOP2", "All RTOP", "D5", "Zone 7")) %>%
        get_cor_monthly_avg_by_day(corridors, "pti", "pct")
}

get_monthly_detector_uptime <- function(avg_daily_detector_uptime) {
    avg_daily_detector_uptime %>% 
        ungroup() %>%
        rowwise() %>%
        mutate(num.all = sum(all.sb, all.pr, na.rm = TRUE)) %>%
        ungroup() %>%
        mutate(CallPhase = 0) %>%
        get_monthly_avg_by_day("uptime.all", "num.all") %>%
        arrange(SignalID, Month)
}

get_cor_monthly_detector_uptime <- function(avg_daily_detector_uptime, corridors) {
    cor_daily_sb_uptime <- avg_daily_detector_uptime %>% 
        filter(!is.na(uptime.sb)) %>%
        mutate(Month = Date - days(day(Date) - 1)) %>%
        get_cor_monthly_avg_by_day(corridors, "uptime.sb", "all.sb")
    cor_daily_pr_uptime <- avg_daily_detector_uptime %>% 
        filter(!is.na(uptime.pr)) %>%
        mutate(Month = Date - days(day(Date) - 1)) %>%
        get_cor_monthly_avg_by_day(corridors, "uptime.pr", "all.pr")
    cor_daily_all_uptime <- avg_daily_detector_uptime %>% 
        filter(!is.na(uptime.all)) %>%
        mutate(Month = Date - days(day(Date) - 1)) %>%
        get_cor_monthly_avg_by_day(corridors, "uptime.all", "ones")
    
    full_join(select(cor_daily_sb_uptime, -c(all.sb, delta)),
              select(cor_daily_pr_uptime, -c(all.pr, delta))) %>%
        left_join(select(cor_daily_all_uptime, -c(ones, delta))) %>%
        mutate(Corridor = factor(Corridor),
               Zone_Group = factor(Zone_Group))
}

get_vph <- function(counts) {
    df <- counts %>% mutate(Date_Hour = floor_date(Timeperiod, "1 hour"))
    get_sum_by_hr(df, "vol") %>% 
        filter(CallPhase %in% c(2,6)) %>% # sum over Phases 2,6
        group_by(SignalID, Week, DOW, Hour) %>% 
        summarize(vph = sum(vol, na.rm = TRUE))
    
    # SignalID | CallPhase | Week | DOW | Hour | aog | vph
}
get_aog_by_hr <- function(aog) {
    get_avg_by_hr(aog, "aog", "vol")
    
    # SignalID | CallPhase | Week | DOW | Hour | aog | vol
}
get_sf_by_hr <- function(sf) {
    get_avg_by_hr(sf, "sf_freq", "cycles")
}
get_qs_by_hr <- function(qs) {
    get_avg_by_hr(qs, "qs_freq", "cycles")
}

get_monthly_vph <- function(vph) {
    
    vph %>% 
        ungroup() %>%
        filter(DOW %in% c(TUE,WED,THU)) %>%
        group_by(SignalID, Hour) %>% summarize(vph = sum(vph, na.rm = TRUE)) %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(vph = mean(vph, na.rm = TRUE))
    
    # SignalID | CallPhase | Hour | vph
}
get_monthly_aog_by_hr <- function(aog_by_hr) {
    
    aog_by_hr %>% 
        group_by(SignalID, Hour) %>% 
        summarize(aog = weighted.mean(aog, vol, na.rm = TRUE), vol = sum(vol, na.rm = TRUE)) %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(aog = weighted.mean(aog, vol, na.rm = TRUE), vol = sum(vol, na.rm = TRUE))
    
    # SignalID | CallPhase | Hour | vph
}
get_monthly_sf_by_hr <- function(sf_by_hr) {
    
    get_monthly_aog_by_hr(rename(sf_by_hr, aog = sf_freq, vol = cycles)) %>%
        rename(sf_freq = aog, cycles = vol)
}
get_monthly_qs_by_hr <- function(qs_by_hr) {
    
    get_monthly_aog_by_hr(rename(qs_by_hr, aog = qs_freq, vol = cycles)) %>%
        rename(qs_freq = aog, cycles = vol)
}

get_cor_monthly_vph <- function(monthly_vph, corridors) {
    get_cor_monthly_avg_by_hr(monthly_vph, corridors, "vph")
    # Corridor | Hour | vph
}
get_cor_monthly_aog_by_hr <- function(monthly_aog_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_aog_by_hr, corridors, "aog", "vol")
    # Corridor | Hour | vph
}
get_cor_monthly_sf_by_hr <- function(monthly_sf_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_sf_by_hr, corridors, "sf_freq", "cycles")
}
get_cor_monthly_qs_by_hr <- function(monthly_qs_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_qs_by_hr, corridors, "qs_freq", "cycles")
}
get_cor_monthly_ti_by_hr <- function(ti, cor_monthly_vph, corridors) {
    
    # Get share of volume (as pct) by hour over the day, for whole dataset
    day_dist <- cor_monthly_vph %>% 
        group_by(Corridor, month(Hour)) %>% 
        mutate(pct = vph/sum(vph, na.rm = TRUE)) %>% 
        group_by(Corridor, Zone_Group, hr = hour(Hour)) %>% 
        summarize(pct = mean(pct, na.rm = TRUE))
    
    df <- left_join(ti, corridors %>% distinct(Corridor, Zone_Group)) %>%
        mutate(hr = hour(Hour)) %>%
        left_join(day_dist) %>%
        ungroup() %>%
        tidyr::replace_na(list(pct = 1))

    if ("tti" %in% colnames(ti)) {
        tindx = "tti"
    } else if ("pti" %in% colnames(ti)) {
        tindx = "pti"
    } else {
        tindx = "oops"
    }
    
    get_cor_monthly_avg_by_hr(df, corridors, tindx, "pct")
}
# No longer used. Generalized the previous function for both tti and pti
get_cor_monthly_pti_by_hr <- function(pti, cor_monthly_vph, corridors) {

    df <- left_join(pti, corridors %>% distinct(Corridor, Zone_Group)) %>%
        left_join(select(cor_monthly_vph, -Zone_Group)) %>%
        ungroup() %>%
        tidyr::replace_na(list(vph = 1))
    
    get_cor_monthly_avg_by_hr(df, corridors, "pti", "vph")
}

get_weekly_vph <- function(vph) {
    vph <- filter(vph, DOW %in% c(TUE,WED,THU))
    get_weekly_avg_by_hr(vph, "vph")
}
get_cor_weekly_vph <- function(weekly_vph, corridors) {
    get_cor_weekly_avg_by_hr(weekly_vph, corridors, "vph")
}
get_cor_weekly_vph_peak <- function(cor_weekly_vph) {
    dfs <- get_cor_monthly_vph_peak(cor_weekly_vph)
    lapply(dfs, function(x) {dplyr::rename(x, Date = Month)})
}
get_weekly_vph_peak <- function(weekly_vph) {
    dfs <- get_monthly_vph_peak(weekly_vph)
    lapply(dfs, function(x) {dplyr::rename(x, Date = Month)})
}

get_monthly_vph_peak <- function(monthly_vph) {
    
    am <- dplyr::filter(monthly_vph, hour(Hour) %in% AM_PEAK_HOURS) %>% 
        mutate(Date = date(Hour)) %>%
        get_daily_avg("vph") %>%
        rename(Month = Date)
    
    pm <- dplyr::filter(monthly_vph, hour(Hour) %in% PM_PEAK_HOURS) %>% 
        mutate(Date = date(Hour)) %>%
        get_daily_avg("vph") %>%
        rename(Month = Date)
    
    list("am" = as_tibble(am), "pm" = as_tibble(pm))
}

# vph during peak periods
get_cor_monthly_vph_peak <- function(cor_monthly_vph) {
    
    am <- dplyr::filter(cor_monthly_vph, hour(Hour) %in% AM_PEAK_HOURS) %>% 
        mutate(Month = date(Hour)) %>%
        weighted_mean_by_corridor_("Month", corridors, as.name("vph"))

    pm <- dplyr::filter(cor_monthly_vph, hour(Hour) %in% PM_PEAK_HOURS) %>%
        mutate(Month = date(Hour)) %>%
        weighted_mean_by_corridor_("Month", corridors, as.name("vph"))

    list("am" = as_tibble(am), "pm" = as_tibble(pm))
}
# aog during peak periods -- unused
get_cor_monthly_aog_peak <- function(cor_monthly_aog_by_hr) {
    
    am <- dplyr::filter(cor_monthly_aog_by_hr, hour(Hour) %in% AM_PEAK_HOURS) %>% 
        mutate(Date = date(Hour)) %>%
        weighted_mean_by_corridor_("Date", corridors, as.name("aog"), as.name("vol"))

    pm <- dplyr::filter(cor_monthly_aog_by_hr, hour(Hour) %in% PM_PEAK_HOURS) %>%
        mutate(Date = date(Hour)) %>% 
        weighted_mean_by_corridor_("Date", corridors, as.name("aog"), as.name("vol"))
    
    list("am" = as_tibble(am), "pm" = as_tibble(pm))
}


get_cor_weekly_cctv_uptime <- function(daily_cctv_uptime) {
    
    df <- daily_cctv_uptime %>% 
        mutate(DOW = wday(Date), 
               Week = week(Date))
    
    Tuesdays <- get_Tuesdays(df)
    
    df %>% 
        select(-Date) %>% 
        left_join(Tuesdays) %>% 
        group_by(Date, Corridor, Zone_Group) %>% 
        summarize(up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE),
                  uptime = sum(up, na.rm = TRUE)/sum(num, na.rm = TRUE)) %>%
        ungroup()
}
get_cor_monthly_cctv_uptime <- function(daily_cctv_uptime) {
    
    daily_cctv_uptime %>% 
        mutate(Month = Date - days(day(Date) - 1)) %>% 
        group_by(Month, Corridor, Zone_Group) %>% 
        summarize(up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE),
                  uptime = sum(up, na.rm = TRUE)/sum(num, na.rm = TRUE)) #%>%
        #get_cor_monthly_xl_uptime()
}



# Cross filter Daily Volumes Chart. For Monthly Report ------------------------
get_vpd_plot <- function(cor_weekly_vpd, cor_monthly_vpd) {
    
    sdw <- SharedData$new(cor_weekly_vpd, ~Corridor, group = "grp")
    sdm <- SharedData$new(dplyr::filter(cor_monthly_vpd, month(Month)==10), ~Corridor, group = "grp")
    
    font_ <- list(family = "Ubuntu")
    
    base <- plot_ly(sdw, color = I("gray")) %>%
        group_by(Corridor)
    base_m <- plot_ly(sdm, color = I("gray")) %>%
        group_by(Corridor)
    
    p1 <- base_m %>%
        summarise(vpd = mean(vpd, na.rm = TRUE)) %>% # This has to be just the current month's vpd
        arrange(vpd) %>%
        add_bars(x = ~vpd, 
                 y = ~factor(Corridor, levels = Corridor),
                 text = ~scales::comma_format()(as.integer(vpd)),
                 textposition = "inside",
                 textfont=list(color="white"),
                 hoverinfo = "none") %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = "October Volume (vpd)", zeroline = FALSE),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = font_,
            margin = list(pad = 4)
        )
    p2 <- base %>%
        add_lines(x = ~Date, 
                  y = ~vpd, 
                  alpha = 0.3) %>%
        layout(xaxis = list(title = "Vehicles/day"),
               showlegend = FALSE)
    
    subplot(p1, p2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
        layout(margin = list(l = 80),
               title = "Volume (veh/day) Trend") %>%
        highlight(color = "#256194", opacityDim = 0.9, defaultValues = c("All"),
                  selected = attrs_selected(insidetextfont=list(color="white"), 
                                            textposition = "inside"))
}
# Cross filter Hourly Volumes Chart. For Monthly Report -----------------------
get_vphpl_plot <- function(df, group_name, chart_title, bar_subtitle, mo) {
    
    sdw <- SharedData$new(df, ~Corridor, group = group_name)
    sdm <- SharedData$new(dplyr::filter(df, month(Date) == mo), ~Corridor, group = group_name)
    
    font_ <- list(family = "Ubuntu")
    
    base <- plot_ly(sdw, color = I("gray")) %>%
        group_by(Corridor)
    base_m <- plot_ly(sdm, color = I("gray")) %>%
        group_by(Corridor)
    
    p1 <- base_m %>%
        summarise(vphpl = mean(vphpl, na.rm = TRUE)) %>% # This has to be just the current month's vphpl
        arrange(vphpl) %>%
        add_bars(x = ~vphpl, 
                 y = ~factor(Corridor, levels = Corridor),
                 text = ~scales::comma_format()(as.integer(vphpl)),
                 textposition = "inside",
                 textfont=list(color="white"),
                 hoverinfo = "none") %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = bar_subtitle, zeroline = FALSE),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = font_,
            margin = list(pad = 4)
        )
    p2 <- base %>%
        add_lines(x = ~Date, y = ~vphpl, alpha = 0.3) %>%
        layout(xaxis = list(title = "Date"),
               showlegend = FALSE,
               annotations = list(text = chart_title,
                                  font = list(family = "Ubuntu",
                                              size = 12),
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1,
                                  showarrow = FALSE)
        )
    
    
    subplot(p1, p2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
        layout(margin = list(l = 80, r = 40)) %>%
        highlight(color = "#256194", opacityDim = 0.9, defaultValues = c("All"),
                  selected = attrs_selected(insidetextfont=list(color="white"), textposition = "inside"))
}
get_vph_peak_plot <- function(df, group_name, chart_title, bar_subtitle, mo) {
    
    sdw <- SharedData$new(df, ~Corridor, group = group_name)
    sdm <- SharedData$new(dplyr::filter(df, month(Date) == mo), ~Corridor, group = group_name)
    
    font_ <- list(family = "Ubuntu")
    
    base <- plot_ly(sdw, color = I("gray")) %>%
        group_by(Corridor)
    base_m <- plot_ly(sdm, color = I("gray")) %>%
        group_by(Corridor)
    
    p1 <- base_m %>%
        summarise(vph = mean(vph, na.rm = TRUE)) %>% # This has to be just the current month's vph
        arrange(vph) %>%
        add_bars(x = ~vph, 
                 y = ~factor(Corridor, levels = Corridor),
                 text = ~scales::comma_format()(as.integer(vph)),
                 textposition = "inside",
                 textfont=list(color="white"),
                 hoverinfo = "none") %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = bar_subtitle, zeroline = FALSE),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = font_,
            margin = list(pad = 4)
        )
    p2 <- base %>%
        add_lines(x = ~Date, y = ~vph, alpha = 0.3) %>%
        layout(xaxis = list(title = "Date"),
               showlegend = FALSE,
               annotations = list(text = chart_title,
                                  font = list(family = "Ubuntu",
                                              size = 12),
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1,
                                  showarrow = FALSE)
        )
    
    
    subplot(p1, p2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
        layout(margin = list(l = 80, r = 40)) %>%
        highlight(color = "#256194", opacityDim = 0.9, defaultValues = c("All"),
                  selected = attrs_selected(insidetextfont=list(color="white"), textposition = "inside"))
}


# Convert Monthly data to quarterly for Quarterly Report ####
get_quarterly <- function(monthly_df, var_, wt_="ones", operation = "avg") {
    
    if (wt_ == "ones" & !"ones" %in% names(monthly_df)) {
        monthly_df <- monthly_df %>% mutate(ones = 1)
    }
    
    var_ <- as.name(var_)
    wt_ <- as.name(wt_)
    
    quarterly_df <- monthly_df %>% 
        group_by(Corridor, 
                 Zone_Group,
                 Quarter = as.character(lubridate::quarter(Month, with_year = TRUE)))
    if (operation == "avg") {
        quarterly_df <- quarterly_df %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), 
                      !!wt_ := sum(!!wt_, na.rm = TRUE))
    } else if (operation == "sum") {
        quarterly_df <- quarterly_df %>%
            summarize(!!var_ := sum(!!var_, na.rm = TRUE))
    } else if (operation == "latest") {
        quarterly_df <- monthly_df %>% 
            group_by(Corridor, 
                     Zone_Group,
                     Quarter = as.character(lubridate::quarter(Month, with_year = TRUE))) %>%
            filter(Month == max(Month)) %>%
            select(Corridor, 
                   Zone_Group,
                   Quarter,
                   !!var_,
                   !!wt_) %>%
            group_by(Corridor, Zone_Group) %>% arrange(Zone_Group, Corridor, Quarter)
    }
    
    quarterly_df %>%
        mutate(delta = ((!!var_) - lag(!!var_))/lag(!!var_))
}


readRDS_multiple <- function(pattern) {
    lf <- list.files(pattern = pattern)
    lf <- lf[grepl(".rds", lf)]
    bind_rows(lapply(lf, readRDS))
}


db_build_data_for_signal_dashboard <- function(month_abbrs, corridors, pth = '.', 
                                               upload_to_s3 = FALSE) {
    
    insert_to_db <- function(prefix, month_abbr, dbtable, signalids, daily) {
        result <- tryCatch({
            df <- f(prefix, month_abbr, daily = daily)
            
            lapply(signalids, function(sid) {
                fn <- file.path(pth, glue("{sid}.db"))
                print(c(prefix, fn))
                conn <- dbConnect(RSQLite::SQLite(), fn)
                dbWriteTable(conn, dbtable, filter(df, SignalID == sid), append = TRUE)
                dbDisconnect(conn)
            })
            
        }, error = function(e) {
            print(e)
            print(glue("No data for {prefix}{month_abbr}"))
        })
    }

    signalids <- levels(corridors$SignalID)
    
    lapply(signalids, function(sid) { 
        file.remove(file.path(pth, glue("{sid}.db"))) 
    })
    
    lapply(month_abbrs, function(month_abbr) {
        
        print(month_abbr)
        
        insert_to_db(prefix = "counts_1hr_", month_abbr, dbtable = "rc", signalids, daily = TRUE)
        insert_to_db(prefix = "filtered_counts_1hr_", month_abbr, dbtable = "fc", signalids, daily = FALSE)
        insert_to_db(prefix = "ddu_", month_abbr, dbtable = "ddu", signalids, daily = FALSE)
        insert_to_db(prefix = "cu_", month_abbr, dbtable = "cu", signalids, daily = FALSE)
        insert_to_db(prefix = "vpd_", month_abbr, dbtable = "vpd", signalids, daily = FALSE)
        insert_to_db(prefix = "vph_", month_abbr, dbtable = "vph", signalids, daily = FALSE)
        insert_to_db(prefix = "tp_", month_abbr, dbtable = "tp", signalids, daily = FALSE)
        insert_to_db(prefix = "aog_", month_abbr, dbtable = "aog", signalids, daily = FALSE)
        insert_to_db(prefix = "sf_", month_abbr, dbtable = "sf", signalids, daily = FALSE)
        insert_to_db(prefix = "qs_", month_abbr, dbtable = "qs", signalids, daily = FALSE)
    })
    
    lapply(signalids, function(sid) {
        aws.s3::put_object(file = file.path(pth, glue("{sid}.db")),
                           object = glue("signal_dashboards/{sid}.db"),
                           bucket = "gdot-devices")
    })
}

