

get_counts <- function(df, det_config, units = "hours", date_, event_code = 82, TWR_only = FALSE) {
    
    if (lubridate::wday(date_, label = TRUE) %in% c("Tue", "Wed", "Thu") || (TWR_only == FALSE)) {
        
        df <- df %>%
            filter(eventcode == event_code)
        
        # Group by hour using Athena/Presto SQL
        if (units == "hours") {
            df <- df %>%
                group_by(timeperiod = date_trunc('hour', timestamp),  # timeperiod = dateadd(HOUR, datediff(HOUR, 0, timestamp), 0),
                         signalid,
                         eventparam)
            
            # Group by 15 minute interval using Athena/Presto SQL
        } else if (units == "15min") {
            df <- df %>%
                mutate(timeperiod = date_trunc('minute', timestamp)) %>%
                group_by(timeperiod = date_add('second',
                                               as.integer(-1 * mod(to_unixtime(timeperiod), 15*60)),
                                               timeperiod),
                         # group_by(timeperiod = dateadd(MINUTE, floor(datediff(MINUTE, 0, timestamp)/15.0) * 15, 0),
                         signalid,
                         eventparam)
        }
        
        df <- df %>%
            count() %>%
            ungroup() %>%
            collect() %>%
            transmute(Timeperiod = ymd_hms(timeperiod),
                      SignalID = factor(signalid),
                      Detector = factor(eventparam),
                      vol = as.integer(n)) %>%
            left_join(det_config, by = c("SignalID", "Detector")) %>%
            
            dplyr::select(SignalID, Timeperiod, Detector, CallPhase, vol) %>%
            mutate(SignalID = factor(SignalID),
                   Detector = factor(Detector))
        df
    } else {
        data.frame()
    }
}


get_counts2 <- function(date_, bucket, conf_athena, uptime = TRUE, counts = TRUE) {
    
    # conn <- get_atspm_connection(aws_conf)
    conn <- get_athena_connection(conf_athena)
    
    end_time <- format(date(date_) + days(1) - seconds(0.1), "%Y-%m-%d %H:%M:%S.9")
    
    if (counts == TRUE) {
        det_config <- get_det_config(date_) %>%
            transmute(SignalID = factor(SignalID),
                      Detector = factor(Detector),
                      CallPhase = factor(CallPhase))
        
        ped_config <- get_ped_config(date_) %>%
            transmute(SignalID = factor(SignalID),
                      Detector = factor(Detector),
                      CallPhase = factor(CallPhase))
    }
    
    atspm_query <- sql(glue(paste(
        "select distinct timestamp, signalid, eventcode, eventparam", 
        "from {conf_athena$database}.{conf_athena$atspm_table}", 
        "where date = '{date_}'")))
    
    df <- tbl(conn, atspm_query) %>%
        # select(timestamp = Timestamp, signalid = SignalID, eventcode = EventCode, eventparam = EventParam) %>%
        mutate(signalid = as.integer(signalid)) %>%
        filter(as_date(timestamp) == as_date(date_))
    
    print(paste("-- Get Counts for:", date_, "-----------"))
    
    if (uptime == TRUE) {
        
        # get uptime$sig, uptime$all
        uptime <- get_uptime(df, date_, end_time)
        
        
        # Reduce to comm uptime for signals_sublist
        print(glue("Communications uptime {date_}"))
        
        cu <- uptime$sig %>%
            ungroup() %>%
            left_join(uptime$all, by = c("Date")) %>%
            mutate(SignalID = factor(SignalID),
                   CallPhase = factor(0),
                   uptime = uptime + (1 - uptime_all),
                   Date_Hour = ymd_hms(paste(start_date, "00:00:00")),
                   Date = date(start_date),
                   DOW = wday(start_date),
                   Week = week(start_date)) %>%
            dplyr::select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, uptime)
        #tz(cu$Date_Hour) <- "America/New_York"
        
        s3_upload_parquet(cu, date_,
                          fn = glue("cu_{date_}"),
                          bucket = bucket,
                          table_name = "comm_uptime",
                          conf_athena) #athena_db = conf_athena$database)
    }
    
    if (counts == TRUE) {
        
        counts_1hr_fn <- glue("counts_1hr_{date_}")
        counts_ped_1hr_fn <- glue("counts_ped_1hr_{date_}")
        counts_15min_fn <- glue("counts_15min_TWR_{date_}")
        
        filtered_counts_1hr_fn <- glue("filtered_counts_1hr_{date_}")
        filtered_counts_15min_fn <- glue("filtered_counts_15min_{date_}")
        
        # get 1hr counts
        print("1-hour counts")
        counts_1hr <- get_counts(
            df,
            det_config,
            "hours",
            date_,
            event_code = 82,
            TWR_only = FALSE
        ) %>%
            arrange(SignalID, Detector, Timeperiod)
        
        s3_upload_parquet(counts_1hr, date_,
                          fn = counts_1hr_fn,
                          bucket = bucket,
                          table_name = "counts_1hr",
                          conf_athena = conf_athena)
        
        print("1-hr filtered counts")
        if (nrow(counts_1hr) > 0) {
            filtered_counts_1hr <- get_filtered_counts_3stream(
                counts_1hr,
                interval = "1 hour")
            s3_upload_parquet(filtered_counts_1hr, date_,
                              fn = filtered_counts_1hr_fn,
                              bucket = bucket,
                              table_name = "filtered_counts_1hr",
                              conf_athena = conf_athena)
        }
        
        
        
        # get 1hr ped counts
        print("1-hour pedestrian counts")
        counts_ped_1hr <- get_counts(
            df,
            ped_config,
            "hours",
            date_,
            event_code = 90,
            TWR_only = FALSE
        ) %>%
            arrange(SignalID, Detector, Timeperiod)
        
        s3_upload_parquet(counts_ped_1hr, date_,
                          fn = counts_ped_1hr_fn,
                          bucket = bucket,
                          table_name = "counts_ped_1hr",
                          conf_athena = conf_athena)
        
        
        # get 15min counts
        print("15-minute counts")
        counts_15min <- get_counts(
            df,
            det_config,
            "15min",
            date_,
            event_code = 82,
            TWR_only = FALSE
        ) %>%
            arrange(SignalID, Detector, Timeperiod)
        
        s3_upload_parquet(counts_15min, date_,
                          fn = counts_15min_fn,
                          bucket = bucket,
                          table_name = "counts_15min",
                          conf_athena = conf_athena)
        
        # get 15min filtered counts
        print("15-minute filtered counts")
        if (nrow(counts_15min) > 0) {
            filtered_counts_15min <- get_filtered_counts_3stream(
                counts_15min,
                interval = "15 min")
            s3_upload_parquet(
                filtered_counts_15min,
                date_,
                fn = filtered_counts_15min_fn,
                bucket = bucket,
                table_name = "filtered_counts_15min",
                conf_athena)
        }
        
    }
    
    dbDisconnect(conn)
    #gc()
}



# New version of get_filtered_counts from 4/2/2020.
#  Considers three factors separately and fails a detector if any are flagged:
#  Streak of 5 "flatlined" hours,
#  Five hours exceeding max volume,
#  Mean Absolute Deviation greater than a threshold
get_filtered_counts_3stream <- function(counts, interval = "1 hour") { # interval (e.g., "1 hour", "15 min")
    
    if (interval == "1 hour") {
        max_volume <- 1200
        
        max_delta <- 500
        max_abs_delta <- 200
        
        max_flat <- 5
        hi_vol_pers <- 5
    } else if (interval == "15 min") {
        max_volume <- 300
        
        max_delta <- 125
        max_abs_delta <- 50
        
        max_flat <- 20
        hi_vol_pers <- 20
    } else {
        stop("interval must be '1 hour' or '15 min'")
    }
    
    counts <- counts %>%
        mutate(SignalID = factor(SignalID),
               Detector = factor(Detector),
               CallPhase = factor(CallPhase)) %>%
        filter(!is.na(CallPhase))
    
    # Identify detectors/phases from detector config file. Expand.
    #  This ensures all detectors are included in the bad detectors calculation.
    all_days <- unique(date(counts$Timeperiod))
    det_config <- lapply(all_days, function(d) {
        all_timeperiods <- seq(ymd_hms(paste(d, "00:00:00")), ymd_hms(paste(d, "23:59:00")), by = interval)
        get_det_config(d) %>%
            expand(nesting(SignalID, Detector, CallPhase),
                   Timeperiod = all_timeperiods)
    }) %>% bind_rows() %>%
        transmute(SignalID = factor(SignalID),
                  Timeperiod = Timeperiod,
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase))
    
    expanded_counts <- full_join(
        det_config,
        counts,
        by = c("SignalID", "Timeperiod", "Detector", "CallPhase")
    ) %>%
        transmute(SignalID = factor(SignalID),
                  Date = date(Timeperiod),
                  Timeperiod = Timeperiod,
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  vol = as.double(vol)) %>%
        replace_na(list(vol = 0)) %>%
        arrange(SignalID, CallPhase, Detector, Timeperiod) %>%
        
        group_by(SignalID, Date, CallPhase, Detector) %>%
        mutate(delta_vol = vol - lag(vol),
               mean_abs_delta = as.integer(ceiling(mean(abs(delta_vol), na.rm = TRUE))),
               vol_streak = if_else(hour(Timeperiod) < 5, -1, vol),
               flatlined = streak_run(vol_streak),
               flatlined = if_else(hour(Timeperiod) < 5, 0, as.double(flatlined)),
               flat_flag = max(flatlined, na.rm = TRUE) > max_flat,
               maxvol_flag = sum(vol > max_volume) > hi_vol_pers,
               mad_flag = mean_abs_delta > max_abs_delta) %>%
        
        ungroup() %>%
        select(-vol_streak)
    
    # bad day = any of the following:
    #    flatlined for at least 5 hours (starting at 5am hour)
    #    vol exceeds maximum allowed over 5 different hours
    #    mean absolute delta exceeds 500
    #  - or -
    #    flatlined for at least 20 15-min periods (starting at 5am)
    #    vol exceeds maximum over 20 different 15-min periods
    #    mean absolute delta exeeds 125
    
    expanded_counts %>%
        group_by(
            SignalID, Date, Detector, CallPhase) %>%
        mutate(
            flat_strk = as.integer(max(flatlined)),
            max_vol = as.integer(max(vol, na.rm = TRUE)),
            flat_flag = max(flat_flag),
            maxvol_flag = max(maxvol_flag),
            mad_flag = max(mad_flag)) %>%
        ungroup() %>%
        mutate(Good_Day = if_else(
            flat_flag > 0 | maxvol_flag > 0 | mad_flag > 0,
            as.integer(0),
            as.integer(1))) %>%
        select(
            SignalID:flatlined, flat_strk, flat_flag, maxvol_flag, mad_flag, Good_Day) %>%
        mutate(
            Month_Hour = Timeperiod - days(day(Timeperiod) - 1),
            Hour = Month_Hour - months(month(Month_Hour) - 1),
            vol = if_else(Good_Day==1, vol, as.double(NA)))
}


# Single threaded
get_filtered_counts <- function(counts, interval = "1 hour") { # interval (e.g., "1 hour", "15 min")
    
    if (interval == "1 hour") {
        max_volume <- 3000  # 1000 - increased on 3/19/2020 to accommodate mainline ramp meter detectors
        max_delta <- 500
        max_abs_delta <- 500  # 200 - increased on 3/24 to make less stringent
    } else if (interval == "15 min") {
        max_volume <- 750  #250 - increased on 3/19/2020 to accommodate mainline ramp meter detectors
        max_delta <- 125
        max_abs_delta <- 125  # 50 - increased on 3/24 to make less stringent
    } else {
        stop("interval must be '1 hour' or '15 min'")
    }
    
    counts <- counts %>%
        mutate(SignalID = factor(SignalID),
               Detector = factor(Detector),
               CallPhase = factor(CallPhase)) %>%
        filter(!is.na(CallPhase))
    
    # Identify detectors/phases from detector config file. Expand.
    #  This ensures all detectors are included in the bad detectors calculation.
    all_days <- unique(date(counts$Timeperiod))
    det_config <- lapply(all_days, function(d) {
        all_timeperiods <- seq(ymd_hms(paste(d, "00:00:00")), ymd_hms(paste(d, "23:59:00")), by = interval)
        get_det_config(d) %>%
            expand(nesting(SignalID, Detector, CallPhase),
                   Timeperiod = all_timeperiods)
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
                  vol = as.double(vol),
                  vol0 = if_else(is.na(vol), 0.0, vol)) %>%
        group_by(SignalID, CallPhase, Detector) %>%
        arrange(SignalID, CallPhase, Detector, Timeperiod) %>%
        
        mutate(delta_vol = vol0 - lag(vol0)) %>%
        ungroup() %>%
        mutate(Good = ifelse(is.na(vol) |
                                 vol > max_volume |
                                 is.na(delta_vol) |
                                 abs(delta_vol) > max_delta |
                                 abs(delta_vol) == 0,
                             0, 1)) %>%
        dplyr::select(-vol0) %>%
        ungroup()
    
    
    # bad day = any of the following:
    #    too many bad hours (60%) based on the above criteria
    #    mean absolute change in hourly volume > 200
    bad_days <- expanded_counts %>%
        filter(hour(Timeperiod) >= 5) %>%
        group_by(SignalID, CallPhase, Detector, Date = date(Timeperiod)) %>%
        summarize(Good = sum(Good, na.rm = TRUE),
                  All = n(),
                  Pct_Good = as.integer(sum(Good, na.rm = TRUE)/n()*100),
                  mean_abs_delta = mean(abs(delta_vol), na.rm = TRUE),
                  .groups = "drop") %>%
        ungroup() %>%
        
        # manually calibrated
        mutate(Good_Day = as.integer(ifelse(Pct_Good >= 70 & mean_abs_delta < max_abs_delta, 1, 0))) %>%
        dplyr::select(SignalID, CallPhase, Detector, Date, mean_abs_delta, Good_Day)
    
    # counts with the bad days taken out
    filtered_counts <- left_join(expanded_counts, bad_days) %>%
        mutate(vol = if_else(Good_Day==1, vol, as.double(NA)),
               Month_Hour = Timeperiod - days(day(Timeperiod) - 1),
               Hour = Month_Hour - months(month(Month_Hour) - 1)) %>%
        ungroup()
    
    filtered_counts
}


get_adjusted_counts <- function(filtered_counts) {
    
    usable_cores <- get_usable_cores()
    
    det_config <- lapply(unique(filtered_counts$Date), get_det_config_vol) %>%
        bind_rows() %>%
        mutate(SignalID = factor(SignalID))
    
    filtered_counts %>%
        left_join(det_config, by = c("SignalID", "CallPhase", "Detector", "Date")) %>%
        filter(!is.na(CountPriority)) %>%
        
        split(.$SignalID) %>% mclapply(function(fc) {
            fc <- fc %>%
                mutate(DOW = wday(Timeperiod),
                       vol = as.double(vol))
            
            ## Phase Contribution - The fraction of volume within each phase on each detector
            ## For instance, for a three-lane phase with equal volumes on each lane,
            ## ph_contr would be 0.33, 0.33, 0.33.
            ph_contr <- fc %>%
                group_by(SignalID, CallPhase, Timeperiod) %>%
                mutate(na.vol = sum(is.na(vol))) %>%
                ungroup() %>%
                filter(na.vol == 0) %>%
                dplyr::select(-na.vol) %>%
                
                # phase contribution factors--fraction of phase volume a detector contributes
                group_by(SignalID, Timeperiod, CallPhase) %>%
                mutate(share = vol/sum(vol)) %>%
                filter(share > 0.1, share < 0.9) %>%
                group_by(SignalID, CallPhase, Detector) %>%
                summarize(Ph_Contr = mean(share, na.rm = TRUE), .groups = "drop")
            
            ## Get the expected volume for the phase based on the volumes and phc
            ## for the detectors with volumes (not NA)
            fc_phc <- left_join(fc, ph_contr, by = c("SignalID", "CallPhase", "Detector")) %>%
                group_by(SignalID, Timeperiod, CallPhase) %>%
                mutate(mvol = mean(vol/Ph_Contr, na.rm = TRUE)) %>% ungroup()
            
            ## Fill in the NA detector volumes with the expected volume for the phase
            ## and the phc for the missing detectors within the same timeperiod
            fc_phc$vol[is.na(fc_phc$vol)] <- as.integer(fc_phc$mvol[is.na(fc_phc$vol)] * fc_phc$Ph_Contr[is.na(fc_phc$vol)])
            
            ## Calculate median hourly volumes over the month by DOW
            ## to fill in missing data for all detectors in a phase
            mo_dow_hrly_vols <- fc_phc %>%
                group_by(SignalID, CallPhase, Detector, DOW, Month_Hour) %>%
                summarize(dow_hrly_vol = median(vol, na.rm = TRUE), .groups = "drop")
            # SignalID | Call.Phase | Detector | Month_Hour | Volume(median)
            
            ## Calculate median hourly volumes over the month (for all days, not by DOW)
            ## to fill in missing data for all detectors in a phase
            mo_hrly_vols <- fc_phc %>%
                group_by(SignalID, CallPhase, Detector, Month_Hour) %>%
                summarize(hrly_vol = median(vol, na.rm = TRUE), .groups = "drop")
            
            fc_phc %>%
                # fill in missing detectors by hour and day of week volume in the month
                left_join(mo_dow_hrly_vols,
                          by = (c("SignalID", "CallPhase", "Detector", "DOW", "Month_Hour"))) %>%
                mutate(vol = if_else(is.na(vol), as.integer(dow_hrly_vol), as.integer(vol))) %>%
                
                # fill in remaining missing detectors by hourly volume in the month
                left_join(mo_hrly_vols,
                          by = (c("SignalID", "CallPhase", "Detector", "Month_Hour"))) %>%
                mutate(vol = if_else(is.na(vol), as.integer(hrly_vol), as.integer(vol))) %>%
                
                dplyr::select(SignalID, CallPhase, Detector, Timeperiod, vol) %>%
                
                filter(!is.na(vol))
        }, mc.cores = usable_cores) %>% bind_rows() #ceiling(parallel::detectCores()*1/3)
}


# Variant that splits signals into equally sized chunks
# may be a template for other memory-intenstive functions.
get_adjusted_counts_split <- function(filtered_counts) {
    
    plan(multiprocess)
    usable_cores <- get_usable_cores()
    
    print("Getting detector config...")
    det_config <- lapply(unique(filtered_counts$Date), get_det_config_vol) %>% 
        bind_rows() %>%
        mutate(SignalID = factor(SignalID))
    
    # Define temporary directory and file names
    temp_dir <- tempdir()
    if (!dir.exists(temp_dir)) {
        dir.create(temp_dir)
    }
    temp_file_root <- stringi::stri_rand_strings(1,8)
    temp_path_root <- file.path(temp_dir, temp_file_root)
    print(temp_path_root)
    
    # Join with det_config
    print("Joining with detector configuration...")
    filtered_counts <- filtered_counts %>%
        mutate(
            SignalID = factor(SignalID), 
            CallPhase = factor(CallPhase),
            Detector = factor(Detector)) %>%
        left_join(det_config, by = c("SignalID", "Detector", "CallPhase", "Date")) %>%
        filter(!is.na(CountPriority))
    
    print("Writing to temporary files by SignalID...")
    signalids <- as.character(unique(filtered_counts$SignalID))
    splits <- split(signalids, ceiling(seq_along(signalids)/100))
    lapply(
        names(splits),
        function(i) {
            #print(paste0(temp_file_root, "_", i, ".fst"))
            cat('.')
            filtered_counts %>%
                filter(SignalID %in% splits[[i]]) %>%
                write_fst(paste0(temp_path_root, "_", i, ".fst"))
        })
    cat('.', sep='\n')
    
    file_names <- paste0(temp_path_root, "_", names(splits), ".fst")
    
    # Read in each temporary file and run adjusted counts in parallel. Afterward, clean up.
    print("getting adjusted counts for each SignalID...")
    df <- mclapply(file_names, mc.cores = usable_cores, FUN = function(fn) {
        #df <- lapply(file_names, function(fn) {
        cat('.')
        fc <- read_fst(fn) %>% 
            mutate(DOW = wday(Timeperiod),
                   vol = as.double(vol))
        
        ph_contr <- fc %>%
            group_by(SignalID, CallPhase, Timeperiod) %>% 
            mutate(na.vol = sum(is.na(vol))) %>%
            ungroup() %>% 
            filter(na.vol == 0) %>% 
            dplyr::select(-na.vol) %>% 
            
            # phase contribution factors--fraction of phase volume a detector contributes
            group_by(SignalID, CallPhase, Detector) %>% 
            summarize(vol = sum(vol, na.rm = TRUE),
                      .groups = "drop_last") %>% 
            #group_by(SignalID, CallPhase) %>% 
            mutate(sum_vol = sum(vol, na.rm = TRUE),
                   Ph_Contr = vol/sum_vol) %>% 
            ungroup() %>% 
            dplyr::select(-vol, -sum_vol)
        
        # fill in missing detectors from other detectors on that phase
        fc_phc <- left_join(fc, ph_contr, by = c("SignalID", "CallPhase", "Detector")) %>%
            # fill in missing detectors from other detectors on that phase
            group_by(SignalID, Timeperiod, CallPhase) %>%
            mutate(mvol = mean(vol/Ph_Contr, na.rm = TRUE)) %>% ungroup()
        
        fc_phc$vol[is.na(fc_phc$vol)] <- as.integer(fc_phc$mvol[is.na(fc_phc$vol)] * fc_phc$Ph_Contr[is.na(fc_phc$vol)])
        
        #hourly volumes over the month to fill in missing data for all detectors in a phase
        mo_hrly_vols <- fc_phc %>%
            group_by(SignalID, CallPhase, Detector, DOW, Month_Hour) %>% 
            summarize(Hourly_Volume = median(vol, na.rm = TRUE), .groups = "drop") 
        # SignalID | Call.Phase | Detector | Month_Hour | Volume(median)
        
        # fill in missing detectors by hour and day of week volume in the month
        left_join(fc_phc, 
                  mo_hrly_vols, 
                  by = (c("SignalID", "CallPhase", "Detector", "DOW", "Month_Hour"))) %>% 
            ungroup() %>%
            mutate(vol = if_else(is.na(vol), as.integer(Hourly_Volume), as.integer(vol))) %>%
            
            dplyr::select(SignalID, CallPhase, Detector, Timeperiod, vol) %>%
            
            filter(!is.na(vol))
    }) %>% bind_rows()
    cat('.', sep='\n')
    
    mclapply(file_names, mc.cores = usable_cores, FUN = file.remove)
    file.remove(temp_dir)
    
    df
}


