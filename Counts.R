

get_counts <- function(df, det_config, units = "hours", date_, event_code = 82, TWR_only = FALSE) {

    if (lubridate::wday(date_, label = TRUE) %in% c("Tue", "Wed", "Thu") || (TWR_only == FALSE)) {

        df <- df %>%
            filter(eventcode == event_code)
        
        # Group by hour using Athena/Presto SQL
        if (units == "hours") {
            df <- df %>% 
                group_by(timeperiod = date_trunc('hour', timestamp),
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


get_counts2 <- function(date_, bucket, cred, uptime = TRUE, counts = TRUE) {

    conn <- get_atspm_connection(cred)

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
        "SELECT DISTINCT Timestamp, SignalID, EventCode, EventParam",
        "FROM Controller_Event_Log",
        "WHERE Timestamp >= '{date_}' AND Timestamp < '{date_ + days(1)}'")))

    df <- tbl(conn, atspm_query)

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
                   Date_Hour = as_datetime(paste(start_date, "00:00:00")),
                   Date = date(start_date),
                   DOW = wday(start_date),
                   Week = week(start_date)) %>%
            dplyr::select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, uptime)

        s3_upload_parquet(cu, date_,
                          fn = glue("cu_{date_}"),
                          bucket = bucket,
                          table_name = "comm_uptime",
                          conf = conf)
    }

    if (counts == TRUE) {

        counts_1hr_fn <- glue("counts_1hr_{date_}")
        counts_ped_1hr_fn <- glue("counts_ped_1hr_{date_}")
        counts_15min_fn <- glue("counts_15min_{date_}")
        counts_ped_15min_fn <- glue("counts_ped_15min_{date_}")

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
                          conf = conf)

        print("1-hr filtered counts")
        if (nrow(counts_1hr) > 0) {
            filtered_counts_1hr <- get_filtered_counts_3stream(
                counts_1hr,
                interval = "1 hour")
            s3_upload_parquet(filtered_counts_1hr, date_,
                              fn = filtered_counts_1hr_fn,
                              bucket = bucket,
                              table_name = "filtered_counts_1hr",
                              conf = conf)
            rm(counts_1hr)

            # BAD DETECTORS
            print(glue("detectors: {date_}"))
            get_bad_detectors(filtered_counts_1hr) %>%
                s3_upload_parquet_date_split(
                    bucket = conf$bucket,
                    prefix = "bad_detectors",
                    table_name = "bad_detectors",
                    conf = conf
                )

            # # DAILY DETECTOR UPTIME
            print(glue("ddu: {date_}"))
            get_daily_detector_uptime(filtered_counts_1hr) %>%
                bind_rows() %>%
                s3_upload_parquet_date_split(
                    bucket = conf$bucket,
                    prefix = "ddu",
                    table_name = "detector_uptime_pd",
                    conf = conf
                )
        }



        # get 1hr ped counts
        print("1-hour pedestrian actuation counts")
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
                          conf = conf)

        # PAPD - pedestrian activations per day
        print("papd")
        get_vpd(counts_ped_1hr, mainline_only = FALSE) %>%
            ungroup() %>%
            rename(papd = vpd) %>%
            s3_upload_parquet_date_split(
                bucket = conf$bucket,
                prefix = "papd",
                table_name = "ped_actuations_pd",
                conf = conf
        )

        # PAPH - pedestrian activations per hour
        print("paph")
        get_vph(counts_ped_1hr, interval = "1 hour", mainline_only = FALSE) %>%
            rename(paph = vph) %>%
            s3_upload_parquet_date_split(
                bucket = conf$bucket,
                prefix = "paph",
                table_name = "ped_actuations_ph",
                conf = conf
        )

        rm(counts_ped_1hr)

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
                          conf = conf)

        # get 15min filtered counts
        print("15-minute filtered counts")
        if (nrow(counts_15min) > 0) {
            get_filtered_counts_3stream(
                counts_15min,
                interval = "15 min") %>%
                s3_upload_parquet(
                    date_,
                    fn = filtered_counts_15min_fn,
                    bucket = bucket,
                    table_name = "filtered_counts_15min",
                    conf = conf)
        }

        # get 15min ped counts
        print("15-minute pedestrian actutation counts")
        counts_ped_15min <- get_counts(
            df,
            ped_config,
            "15min",
            date_,
            event_code = 90,
            TWR_only = FALSE
        ) %>%
            arrange(SignalID, Detector, Timeperiod)

        s3_upload_parquet(counts_ped_15min, date_,
                          fn = counts_ped_15min_fn,
                          bucket = bucket,
                          table_name = "counts_ped_15min",
                          conf = conf)

        # PA15 - pedestrian activations per 15min
        print("pa15")
        get_vph(counts_ped_15min, interval = "15 min", mainline_only = FALSE) %>%
            rename(pa15 = vph) %>%
            s3_upload_parquet_date_split(
                bucket = conf$bucket,
                prefix = "pa15",
                table_name = "ped_actuations_15min",
                conf = conf
            )

        rm(counts_ped_15min)

    }
}



# New version of get_filtered_counts from 4/2/2020.
#  Considers three factors separately and fails a detector if any are flagged:
#  Streak of 5 "flatlined" hours,
#  Five hours exceeding max volume,
#  Mean Absolute Deviation greater than a threshold
get_filtered_counts_3stream <- function(counts, interval = "1 hour") { # interval (e.g., "1 hour", "15 min")

    if (interval == "1 hour") {
        max_volume <- 1200  # 1000 - increased on 3/19/2020 (down to 2000 on 3/31) to accommodate mainline ramp meters
        max_volume_mainline <- 3000 # New on 5/21/2020
        max_delta <- 500
        max_abs_delta <- 200  # 200 - increased on 3/24 to make less stringent
        max_abs_delta_mainline <- 500 # New on 5/21/2020
        max_flat <- 5
        hi_vol_pers <- 5
    } else if (interval == "15 min") {
        max_volume <- 300  #250 - increased on 3/19/2020 (down to 500 on 3/31) to accommodate mainline ramp meter detectors
        max_volume_mainline <- 750 # New on 5/21/2020
        max_delta <- 125
        max_abs_delta <- 50  # 50 - increased on 3/24 to make less stringent
        max_abs_delta_mainline <- 125
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
        all_timeperiods <- seq(as_datetime(d), as_datetime(d) + days(1) - seconds(1), by = interval)
        get_det_config(d) %>%
            expand(nesting(SignalID, Detector, CallPhase, ApproachDesc), ## ApproachDesc is new
                   Timeperiod = all_timeperiods)
    }) %>% bind_rows() %>%
        transmute(SignalID = factor(SignalID),
                  Timeperiod = Timeperiod,
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  Mainline = grepl("Mainline", ApproachDesc)) ## New

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
                  Mainline,
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
               maxvol_flag = if_else(Mainline,
                                     sum(vol > max_volume_mainline) > hi_vol_pers,
                                     sum(vol > max_volume) > hi_vol_pers),
               mad_flag = if_else(Mainline,
                                  mean_abs_delta > max_abs_delta_mainline,
                                  mean_abs_delta > max_abs_delta)) %>%

        ungroup() %>%
        select(-Mainline, -vol_streak)

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



get_adjusted_counts <- function(df) {
    fc <- df %>%
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
        mutate(sum_vol = sum(vol, na.rm = TRUE),
               Ph_Contr = vol/sum_vol) %>%
        ungroup() %>%
        dplyr::select(-vol, -sum_vol)

    # fill in missing detectors from other detectors on that phase
    fc_phc <- left_join(fc, ph_contr, by = c("SignalID", "CallPhase", "Detector")) %>%
        # fill in missing detectors from other detectors on that phase
        group_by(SignalID, Timeperiod, CallPhase) %>%
        mutate(mvol = mean(vol/Ph_Contr, na.rm = TRUE)) %>% ungroup()

    fc_phc$mvol[fc_phc$mvol > 3000] <- NA  # Prevent ridiculously high interpolated values

    fc_phc$vol[is.na(fc_phc$vol)] <- as.integer(fc_phc$mvol[is.na(fc_phc$vol)] * fc_phc$Ph_Contr[is.na(fc_phc$vol)])

    #hourly volumes over the month to fill in missing data for all detectors in a phase
    mo_hrly_vols <- fc_phc %>%
        group_by(SignalID, CallPhase, Detector, DOW, Month_Hour) %>%
        summarize(Hourly_Volume = median(vol, na.rm = TRUE), .groups = "drop")

    # fill in missing detectors by hour and day of week volume in the month
    left_join(fc_phc,
              mo_hrly_vols,
              by = (c("SignalID", "CallPhase", "Detector", "DOW", "Month_Hour"))) %>%
        ungroup() %>%
        mutate(vol = if_else(is.na(vol), as.integer(Hourly_Volume), as.integer(vol))) %>%

        dplyr::select(SignalID, CallPhase, Detector, Timeperiod, vol) %>%

        filter(!is.na(vol))
}




# ==============================================================================
# Arrow implementation on local drive

prep_db_for_adjusted_counts_arrow <- function(table, conf, date_range) {

    fc_ds <- arrow::open_dataset(
        sources = join_path("s3:/", conf$bucket, conf$key_prefix, mark, table),
        schema = schema(
            SignalID = string(),
            Timeperiod = timestamp(unit = "ms", timezone = "GMT"),
            Detector = string(),
            CallPhase = string(),
            vol = float(),
            delta_vol = float(),
            Good = float(),
            mean_abs_delta = float(),
            Good_Day = int32(),
            Month_Hour = timestamp(unit = "ms", timezone = "GMT"),
            Hour = timestamp(unit = "ms", timezone = "GMT"),
            date = string()
        )
    ) %>%
        filter(date %in% format(date_range, "%F"))
    chunks <- get_signals_chunks_arrow(fc_ds)
    groups <- tibble(group = names(chunks), SignalID = chunks) %>%
        unnest(SignalID)

    if (dir.exists(table)) unlink(table, recursive = TRUE)
    dir.create(table)

    date_range <- fc_ds %>% distinct(date) %>% collect() %>% pull(date) %>% as_date()
    mclapply(date_range, mc.cores = usable_cores, mc.preschedule = FALSE, FUN = function(date_) {
        date_str <- format(date_, "%F")
        cat('.')

        fc <- s3_read_parquet_parallel(table, date_, date_, bucket = conf$bucket, conf = conf) %>%
            transmute(
                SignalID = as.character(SignalID),
                Date, Timeperiod, Month_Hour,
                Detector = as.integer(as.character(Detector)),
                CallPhase = as.integer(as.character(CallPhase)),
                Good_Day,
                vol)
        dc <- get_det_config_vol(date_) %>%
            transmute(
                SignalID = as.character(SignalID),
                Date,
                Detector = as.integer(as.character(Detector)),
                CallPhase = as.integer(as.character(CallPhase)),
                CountPriority) %>%
            filter(!is.na(CountPriority))

        fc <- left_join(fc, dc, by = c("SignalID", "Detector", "CallPhase", "Date")) %>%
            filter(!is.na(CountPriority)) %>%
            left_join(groups, by = c("SignalID"))
        for (grp in names(chunks)) {
            folder_location <- glue("{table}/group={grp}/date={date_str}")
            if (!dir.exists(folder_location)) dir.create(folder_location, recursive = TRUE)
            fc %>% filter(group == grp) %>% select(-group) %>%
                write_parquet(glue("{folder_location}/{table}.parquet"))
        }
    })
    cat('\n')
    return (TRUE)
}




get_adjusted_counts_arrow <- function(fc_table, ac_table, conf, callback = function(x) {x}) {
    # callback would be for get_thruput

    fc_ds <- arrow::open_dataset(fc_table)

    if (dir.exists(ac_table)) unlink(ac_table, recursive = TRUE)
    dir.create(ac_table)

    groups <- (fc_ds %>% select(group) %>% collect() %>% distinct())$group

    mclapply(groups, mc.cores = usable_cores, mc.preschedule = FALSE, FUN = function(grp) {
        cat('.')
        ac <- arrow::open_dataset(fc_table) %>%
            filter(group == grp) %>%
            collect() %>%
            get_adjusted_counts() %>%
            mutate(Date = as_date(Timeperiod)) %>%
            callback()
        for (date_ in unique(ac$Date)) {
            date_str <- format(as_date(date_), "%F")
            ac_dir <- glue("{ac_table}/date={date_str}")
            if (!dir.exists(ac_dir)) dir.create(ac_dir)
            filename <- tempfile(
                tmpdir = ac_dir,
                pattern = "ac_",
                fileext = ".parquet")
            ac %>% filter(Date == date_) %>% write_parquet(filename)
        }
    })
    cat('\n')
}

