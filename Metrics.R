
get_uptime <- function(df, start_date, end_time) {

    ts_sig <- df %>%
        mutate(timestamp = date_trunc('minute', timestamp)) %>%
        distinct(signalid, timestamp) %>%
        collect()

    signals <- unique(ts_sig$signalid)
    bookend1 <- expand.grid(SignalID = as.integer(signals), Timestamp = ymd_hms(glue("{start_date} 00:00:00")))
    bookend2 <- expand.grid(SignalID = as.integer(signals), Timestamp = ymd_hms(end_time))


    ts_sig <- ts_sig %>%
        transmute(SignalID = signalid, Timestamp = ymd_hms(timestamp)) %>%
        bind_rows(., bookend1, bookend2) %>%
        distinct() %>%
        arrange(SignalID, Timestamp)

    ts_all <- ts_sig %>%
        distinct(Timestamp) %>%
        mutate(SignalID = 0) %>%
        arrange(Timestamp)

    uptime <- lapply(list(ts_sig, ts_all), function (x) {
        x %>%
            mutate(Date = date(Timestamp)) %>%
            group_by(SignalID, Date) %>%
            mutate(lag_Timestamp = lag(Timestamp),
                   span = as.numeric(Timestamp - lag_Timestamp, units = "mins")) %>%
            select(-lag_Timestamp) %>%
            drop_na() %>%
            mutate(span = if_else(span > 15, span, 0)) %>%

            group_by(SignalID, Date) %>%
            summarize(uptime = 1 - sum(span, na.rm = TRUE)/(60 * 24),
                      .groups = "drop")
    })
    names(uptime) <- c("sig", "all")
    uptime$all <- uptime$all %>%
        dplyr::select(-SignalID) %>% rename(uptime_all = uptime)
    uptime
}



get_spm_data_atspm <- function(start_date, end_date, signals_list, conf_atspm, table, TWR_only=TRUE) {

    conn <- get_atspm_connection(conf_atspm)

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



get_spm_data_athena <- function(start_date, end_date, signals_list = NULL, conf, table, TWR_only=TRUE) {

    conn <- get_athena_connection(conf)

    if (TWR_only == TRUE) {
        query_where <- "WHERE date_format(date_parse(date, '%Y-%m-%d'), '%W') in ('Tuesday','Wednesday','Thursday')"
    } else {
        query_where <- ""
    }

    query <- glue("SELECT DISTINCT * FROM {conf$athena$database}.{tolower(table)} {query_where}")

    df <- tbl(conn, sql(query))

    if (!is.null(signals_list)) {
        if (is.factor(signals_list)) {
            signals_list <- as.integer(as.character(signals_list))
        } else if (is.character(signals_list)) {
            signals_list <- as.integer(signals_list)
        }
        df <- df %>% filter(signalid %in% signals_list)
    }

    end_date1 <- ymd(end_date) + days(1)

    df %>%
        dplyr::filter(date >= start_date & date < end_date1)
}

get_spm_data <- get_spm_data_athena

# Query Cycle Data
get_cycle_data <- function(start_date, end_date, conf, signals_list = NULL) {
    get_spm_data(
        start_date,
        end_date,
        signals_list,
        conf,
        table = "CycleData",
        TWR_only = FALSE)
}


# Query Detection Events
get_detection_events <- function(start_date, end_date, conf, signals_list = NULL) {
    get_spm_data(
        start_date,
        end_date,
        signals_list,
        conf,
        table = "DetectionEvents",
        TWR_only = FALSE)
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


get_bad_ped_detectors <- function(pau) {
    pau %>%
        filter(uptime == 0) %>%
        dplyr::select(SignalID, Detector, Date)
}


# Volume VPD
get_vpd <- function(counts, mainline_only = TRUE) {

    if (mainline_only == TRUE) {
        counts <- counts %>%
            filter(CallPhase %in% c(2,6)) # sum over Phases 2,6 # added 4/24/18
    }
    counts %>%
        mutate(DOW = wday(Timeperiod),
               Week = week(date(Timeperiod)),
               Date = date(Timeperiod)) %>%
        group_by(SignalID, CallPhase, Week, DOW, Date) %>%
        summarize(vpd = sum(vol, na.rm = TRUE), .groups = "drop")
}


# SPM Throughput
get_thruput <- function(counts) {

    counts %>%
        mutate(
            Date = date(Timeperiod),
            DOW = wday(Date),
            Week = week(Date)) %>%
        group_by(SignalID, Week, DOW, Date, Timeperiod) %>%
        summarize(vph = sum(vol, na.rm = TRUE),
                  .groups = "drop_last") %>%

        summarize(vph = tdigest::tquantile(tdigest::tdigest(vph), probs=c(0.95)) * 4,
                  .groups = "drop") %>%

        arrange(SignalID, Date) %>%
        mutate(CallPhase = 0) %>%
        dplyr::select(SignalID, CallPhase, Date, Week, DOW, vph)
}



get_daily_aog <- function(aog) {

    aog %>%
        ungroup() %>%
        mutate(DOW = wday(Date),
               Week = week(Date),
               CallPhase = factor(CallPhase)) %>%
        filter(DOW %in% c(TUE,WED,THU) & (hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))) %>%
        group_by(SignalID, CallPhase, Date, Week, DOW) %>%
        summarize(aog = weighted.mean(aog, vol, na.rm = TRUE),
                  vol = sum(vol, na.rm = TRUE),
                  .groups = "drop")
}


get_daily_pr <- function(aog) {

    aog %>%
        ungroup() %>%
        mutate(DOW = wday(Date),
               Week = week(Date),
               CallPhase = factor(CallPhase)) %>%
        #filter(DOW %in% c(TUE,WED,THU) & (hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))) %>%
        group_by(SignalID, CallPhase, Date, Week, DOW) %>%
        summarize(pr = weighted.mean(pr, vol, na.rm = TRUE),
                  vol = sum(vol, na.rm = TRUE),
                  .groups = "drop")

    # SignalID | CallPhase | Date | Week | DOW | pr | vol
}


get_occupancy <- function(de_dt, int_dt) {
    occ_df <- foverlaps(de_dt, int_dt, type = "any") %>%
        filter(!is.na(IntervalStart), !is.na(IntervalEnd)) %>%

        as_tibble() %>%

        transmute(
            SignalID = factor(SignalID),
            Phase,
            Detector = as.integer(as.character(Detector)),
            CycleStart,
            IntervalStart,
            IntervalEnd,
            int_interval = lubridate::interval(IntervalStart, IntervalEnd),
            occ_interval = lubridate::interval(DetOn, DetOff),
            occ_duration = as.numeric(intersect(occ_interval, int_interval)),
            int_duration = as.numeric(int_interval))

    int_df <- as_tibble(int_dt)

    occ_df <- full_join(
            int_df, occ_df,
            by = c("SignalID", "Phase", "CycleStart", "IntervalStart", "IntervalEnd")
        ) %>%
        tidyr::replace_na(
            list(Detector = 0, occ_duration = 0, int_duration = 1)) %>%
        mutate(SignalID = factor(SignalID),
               Detector = factor(Detector)) %>%

        group_by(SignalID, Phase, CycleStart, Detector) %>%
        summarize(occ = sum(occ_duration)/max(int_duration),
                  .groups = "drop_last") %>%

        summarize(occ = max(occ),
                  .groups = "drop") %>%

        mutate(SignalID = factor(SignalID),
               Phase = factor(Phase))

    occ_df
}



get_sf_utah <- function(date_, conf, signals_list = NULL, first_seconds_of_red = 5, intervals = c("hour")) {


    plan(multisession)
    print("Pulling data...")

    cat('.')

    dc <- get_det_config_sf(date_) %>%
        filter(SignalID %in% signals_list) %>%
        rename(Phase = CallPhase)

    cat('.')

    ds_de <- arrow::open_dataset(glue("gs://{conf$bucket}/{conf$key_prefix}/detections/date={date_}"))
    de <- ds_de %>%
        filter(SignalID %in% signals_list,
               Phase %in% c(3, 4, 7, 8)) %>%
        collect() %>%
        convert_to_utc() %>%
        arrange(
            SignalID, Phase, CycleStart, PhaseStart) %>%
        transmute(
            SignalID = factor(SignalID),
            Phase = factor(Phase),
            Detector = factor(Detector),
            CycleStart, PhaseStart,
            DetOn = DetTimeStamp,
            DetOff = DetTimeStamp + seconds(DetDuration),
            Date = as_date(date_)) %>%

        left_join(dc, by = c("SignalID", "Phase", "Detector", "Date")) %>%
        filter(!is.na(TimeFromStopBar)) %>%
        mutate(SignalID = factor(SignalID),
               Phase = factor(Phase),
               Detector = factor(Detector)) %>%
        data.table()

    cat('.')

    ds_cd <- arrow::open_dataset(glue("gs://{conf$bucket}/{conf$key_prefix}/cycles/date={date_}"))
    cd <- ds_cd %>%
        filter(SignalID %in% signals_list,
               Phase %in% c(3, 4, 7, 8),
               EventCode %in% c(1, 9)) %>%
        collect() %>%
        convert_to_utc() %>%
        arrange(SignalID, Phase, CycleStart, PhaseStart) %>%
        mutate(SignalID = factor(SignalID),
               Phase = factor(Phase),
               Date = as_date(date_))

    cat('\n')
    print("Running calcs")

    grn_interval <- cd %>%
        filter(EventCode == 1) %>%
        mutate(IntervalStart = PhaseStart,
               IntervalEnd = PhaseEnd) %>%
        select(-EventCode)

    sor_interval <- cd %>%
        filter(EventCode == 9) %>%
        mutate(IntervalStart = ymd_hms(PhaseStart),
               IntervalEnd = ymd_hms(IntervalStart) + seconds(first_seconds_of_red)) %>%
        select(-EventCode)

    cat('.')

    grn_interval <- setDT(grn_interval)
    sor_interval <- setDT(sor_interval)

    setkey(de, SignalID, Phase, DetOn, DetOff)
    setkey(grn_interval, SignalID, Phase, IntervalStart, IntervalEnd)
    setkey(sor_interval, SignalID, Phase, IntervalStart, IntervalEnd)

    ## ---

    grn_occ <- get_occupancy(de, grn_interval) %>%
        rename(gr_occ = occ)
    cat('.')
    sor_occ <- get_occupancy(de, sor_interval) %>%
        rename(sr_occ = occ)
    cat('.\n')

    sf <- full_join(grn_occ, sor_occ, by = c("SignalID", "Phase", "CycleStart")) %>%
        replace_na(list(sr_occ = 0, gr_occ = 0)) %>%
        mutate(sf = if_else((gr_occ > 0.80) & (sr_occ > 0.80), 1, 0))

    # if a split failure on any phase
    sf0 <- sf %>% group_by(SignalID, Phase = factor(0), CycleStart) %>%
        summarize(sf = max(sf), .groups = "drop")

    sf <- bind_rows(sf, sf0) %>%
        mutate(Phase = factor(Phase))

    return_list <- list()
    for (interval in intervals) {
        return_list[[interval]] <- sf %>%
            group_by(SignalID, Phase, hour = floor_date(CycleStart, unit = interval)) %>%
            summarize(cycles = n(),
                      sf_freq = sum(sf, na.rm = TRUE)/cycles,
                      sf = sum(sf, na.rm = TRUE),
                      .groups = "drop") %>%

            transmute(SignalID,
                      CallPhase = Phase,
                      Date_Hour = as_datetime(hour),
                      Date = date(Date_Hour),
                      DOW = wday(Date),
                      Week = week(Date),
                      sf = as.integer(sf),
                      cycles = cycles,
                      sf_freq = sf_freq)
    }
    return_list
}


get_peak_sf_utah <- function(msfh) {

    msfh %>%
        group_by(SignalID,
                 Date = date(Hour),
                 Peak = if_else(hour(Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS),
                                "Peak", "Off_Peak")) %>%
        summarize(sf_freq = weighted.mean(sf_freq, cycles, na.rm = TRUE),
                  cycles = sum(cycles, na.rm = TRUE),
                  .groups = "drop") %>%
        select(-cycles) %>%
        split(.$Peak)
}


# SPM Split Failures
get_sf <- function(df) {
    df %>% mutate(SignalID = factor(SignalID),
                  CallPhase = factor(Phase),
                  Date_Hour = as_datetime(Hour),
                  Date = date(Date_Hour),
                  DOW = wday(Date),
                  Week = week(Date)) %>%
        dplyr::select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, sf, cycles, sf_freq) %>%
        as_tibble()
}


# SPM Queue Spillback - updated 2/20/2020
get_qs <- function(detection_events, intervals = c("hour")) {

    cat('.')
    qs_df <- detection_events %>%

        # By Detector by cycle. Get 95th percentile duration as occupancy
        group_by(date,
                 signalid,
                 cyclestart,
                 callphase = phase,
                 detector) %>%
        summarize(occ = approx_percentile(detduration, 0.95), .groups = "drop") %>%
        collect() %>%
        transmute(Date = date(date),
                  SignalID = factor(signalid),
                  CycleStart = as_datetime(cyclestart),
                  CallPhase = factor(callphase),
                  Detector = factor(detector),
                  occ)

    dates <- unique(qs_df$Date)

    cat('.')

    # Get detector config for queue spillback.
    # get_det_config_qs2 filters for Advanced Count and Advanced Speed detector types
    # It also filters out bad detectors
    dc <- lapply(dates, get_det_config_qs) %>%
        bind_rows() %>%
        dplyr::select(Date, SignalID, CallPhase, Detector, TimeFromStopBar) %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase))

    qs_df <- qs_df %>%
        left_join(dc, by=c("Date", "SignalID", "CallPhase", "Detector")) %>%
        filter(!is.na(TimeFromStopBar)) %>%
        # -- data.tables. This is faster ---
        as.data.table

    qs_df <- qs_df[, .(occ = max(occ, na.rm = TRUE)),
       by = c("Date", "SignalID", "CallPhase", "CycleStart")]

    cat('.\n')

    return_list <- lapply(intervals, function(interval, df=qs_df) {
        print(interval)

        df[, Hour := floor_date(CycleStart, unit = interval)]
        df <- df[, .(qs = sum(occ > 3), cycles = .N),
           by = c("Date", "SignalID", "Hour", "CallPhase")]
        df %>%
            transmute(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date),
                Date_Hour = as_datetime(Hour),
                DOW = wday(Date),
                Week = week(Date),
                qs = as.integer(qs),
                cycles = as.integer(cycles),
                qs_freq = as.double(qs)/as.double(cycles)) %>% as_tibble()
    })
    names(return_list) <- intervals
    return_list
}

get_pau_high_ <- function(paph, pau_start_date) {
    paph <- paph %>% filter(Date >= pau_start_date)

    # Fail pushbutton input if mean hourly count > 600
    # or std dev hourly count > 9000
    print("too high filter (based on mean and sd for the day)...")
    too_high_distn <- paph %>%
        filter(hour(Hour) >= 6, hour(Hour) <= 22) %>%
        complete(
            nesting(SignalID, Detector, CallPhase),
            nesting(Date, Hour, DOW, Week),
            fill = list(paph = 0)) %>%
        group_by(SignalID, Detector, CallPhase, Date) %>%
        summarize(
            mn = mean(paph, na.rm = TRUE),
            sd = sd(paph, na.rm = TRUE),
            .groups = "drop") %>%
        filter(mn > 600 | sd > 9000) %>%
        mutate(toohigh_distn = TRUE)

    # Fail pushbutton input if between midnight and 6am,
    # at least one hour > 300 or at least three hours > 60
    print("too high filter (early morning counts)...")
    too_high_am <- paph %>%
        filter(hour(Hour) < 6) %>%
        group_by(SignalID, Detector, CallPhase, Date) %>%
        summarize(
            mvol = sort(paph, TRUE)[3],
            hvol = max(paph),
            .groups = "drop") %>%
        filter(hvol > 300 | mvol > 60) %>%
        transmute(
            SignalID = as.character(SignalID),
            Detector = as.character(Detector),
            CallPhase = as.character(CallPhase),
            Date,
            toohigh_am = TRUE) %>%
        arrange(SignalID, Detector, CallPhase, Date)

    # Fail pushbutton inputs if there are at least four outlier data points.
    # An outlier data point is when, for a given hour, the count is > 300
    # and exceeds 100 times the count of next highest pushbutton input
    # (if next highest pushbutton input count is 0, use 1 instead)
    print("too high filter (compared to other phases for the same signal)...")
    too_high_nn <- paph %>%
        complete(
            nesting(SignalID, Detector, CallPhase),
            nesting(Date, Hour, DOW, Week),
            fill = list(paph = 0)) %>%
        group_by(SignalID, Hour) %>%
        mutate(outlier = paph > max(1, sort(paph, TRUE)[2]) * 100 & paph > 300) %>%

        group_by(SignalID, Date, Detector, CallPhase) %>%
        summarize(toohigh_nn = sum(outlier) >= 4, .groups = "drop") %>%
        filter(toohigh_nn)

    too_high <- list(too_high_distn, too_high_am, too_high_nn) %>%
        reduce(full_join, by = c("SignalID", "Detector", "CallPhase", "Date"))

    too_high <- if (nrow(too_high) > 0) {
        too_high %>%
            transmute(
                SignalID, Detector, CallPhase, Date,
                toohigh = as.logical(max(c_across(starts_with("toohigh")), na.rm = TRUE)))
    } else {
        too_high %>%
            transmute(
                SignalID, Detector, CallPhase, Date,
                toohigh = FALSE)
    }

    too_high
}

get_pau_high <- split_wrapper(get_pau_high_)



fitdist_trycatch <- function(x, ...) {
    tryCatch({
        fitdist(x, ...)
    }, error = function(e) {
        NULL
    })
}

pgamma_trycatch <- function(x, ...) {
    tryCatch({
        pgamma(x, ...)
    }, error = function(e) {
        0
    })
}

get_gamma_p0 <- function(df) {
    tryCatch({
        if (max(df$papd)==0) {
            1
        } else {
            model <- fitdist(df$papd, "gamma", method = "mme")
            pgamma(1, shape = model$estimate["shape"], rate = model$estimate[["rate"]])
        }
    }, error = function(e) {
        print(e)
        1
    })
}


get_pau_gamma <- function(papd, paph, corridors, wk_calcs_start_date, pau_start_date) {

    # A pushbutton input (aka "detector") is failed for a given day if:
    # the streak of days with no actuations is greater that what
    # would be expected based on the distribution of daily presses
    # (the probability of that many consecutive zero days is < 0.01)
    #  - or -
    # the number of actuations is more than 100 times the 80% percentile
    # number of daily presses for that pushbutton input
    # (i.e., it's a [high] outlier for that input)
    # - or -
    # between midnight and 6am, there is at least one hour in a day with
    # at least 300 actuations or at least three hours with over 60
    # (i.e., it is an outlier based on what would be expected
    # for any input in the early morning hours)

    # too_high <- get_pau_high(paph, 200, wk_calcs_start_date)
    too_high <- get_pau_high_(paph, wk_calcs_start_date)


    begin_date <- min(papd$Date)

    corrs <- corridors %>%
        group_by(SignalID) %>%
        summarize(Asof = min(Asof),
                  .groups = "drop")

    ped_config <- lapply(unique(papd$Date), function(d) {
        get_ped_config(d) %>%
            mutate(Date = d) %>%
            filter(SignalID %in% papd$SignalID)
    }) %>%
        bind_rows() %>%
        mutate(SignalID = factor(SignalID),
               Detector = factor(Detector),
               CallPhase = factor(CallPhase))

    print("too low filter...")
    papd <- papd %>%
        full_join(ped_config, by = c("SignalID", "Detector", "CallPhase", "Date"))
    rm(ped_config)
    gc()

    papd <- papd %>%
        transmute(SignalID = factor(SignalID),
                  CallPhase = factor(CallPhase),
                  Detector = factor(Detector),
                  Date = Date,
                  Week = week(Date),
                  DOW = wday(Date),
                  weekday = DOW %in% c(2,3,4,5,6),
                  papd = papd) %>%
        filter(CallPhase != 0) %>%
        complete(
            nesting(SignalID, CallPhase, Detector),
            nesting(Date, weekday),
            fill = list(papd=0)) %>%
        arrange(SignalID, Detector, CallPhase, Date) %>%
        group_by(SignalID, Detector, CallPhase) %>%
        mutate(
            streak_id = runner::which_run(papd, which = "last"),
            streak_id = ifelse(papd > 0, NA, streak_id)) %>%
        ungroup() %>%
        left_join(corrs, by = c("SignalID")) %>%
        replace_na(list(Asof = begin_date)) %>%
        filter(Date >= pmax(ymd(pau_start_date), Asof)) %>%
        dplyr::select(-Asof) %>%
        mutate(SignalID = factor(SignalID))

    #plan(multisession, workers = detectCores()-1)
    modres <- papd %>%
        group_by(SignalID, Detector, CallPhase, weekday) %>%
        filter(n() > 2) %>%
        ungroup() %>%
        select(-c(Week, DOW, streak_id)) %>%
        nest(data = c(Date, papd)) %>%
        mutate(
            p0 = purrr::map(data, get_gamma_p0)) %>%
        unnest(p0) %>%
        select(-data)

    pz <- left_join(
        papd, modres,
        by = c("SignalID", "CallPhase", "Detector", "weekday")
    ) %>%
        group_by(SignalID, CallPhase, Detector, streak_id) %>%
        mutate(
            prob_streak = if_else(is.na(streak_id), 1, prod(p0)),
            prob_bad = 1 - prob_streak) %>%
        ungroup() %>%
        select(SignalID, CallPhase, Detector, Date, problow = prob_bad)

    print("all filters combined...")
    pau <- left_join(
        select(papd, SignalID, CallPhase, Detector, Date, Week, DOW, papd),
        pz,
        by = c("SignalID", "Detector", "CallPhase", "Date")) %>%
        mutate(
            SignalID = as.character(SignalID),
            Detector = as.character(Detector),
            CallPhase = as.character(CallPhase),
            papd
        ) %>%
        filter(Date >= wk_calcs_start_date) %>%
        left_join(too_high, by = c("SignalID", "Detector", "CallPhase", "Date")) %>%
        replace_na(list(toohigh = FALSE)) %>%

        transmute(
            SignalID = factor(SignalID),
            Detector = factor(Detector),
            CallPhase = factor(CallPhase),
            Date = Date,
            DOW = wday(Date),
            Week = week(Date),
            papd = papd,
            problow = problow,
            probhigh = as.integer(toohigh),
            uptime = if_else(problow > 0.99 | toohigh, 0, 1),
            all = 1)
    pau
}



get_ped_delay <- function(date_, conf, signals_list) {

    print("Pulling data...")

    cat('.')

    plan(sequential)
    plan(multisession)

    conn <- get_atspm_connection()
    ds <- tbl(conn, "Controller_Event_Log")

    pe <- ds %>%
        filter(EventCode %in% c(45, 21, 22, 132)) %>%
        select(SignalID, Timestamp, EventCode, EventParam) %>%
        collect() %>%
        convert_to_utc() %>%
        mutate(CycleLength = ifelse(EventCode == 132, EventParam, NA)) %>%
        arrange(SignalID, Timestamp) %>%
        rename(Phase = EventParam) %>%
	    convert_to_utc()

    cat('.')
    coord.type <- group_by(pe, SignalID) %>%
        tidyr::fill(CycleLength) %>%
        summarise(CL = max(CycleLength, na.rm = T), .groups = "drop") %>%
        mutate(Pattern = ifelse((CL == 0 | !is.finite(CL)), "Free", "Coordinated"))

    cat('.')
    pe <- inner_join(pe, coord.type, by = "SignalID") %>%
        filter(
            EventCode != 132,
            !(Pattern == "Coordinated" & (is.na(CycleLength) | CycleLength == 0 )) #filter out times of day when coordinated signals run in free
        ) %>%
        select(
            SignalID, Phase, EventCode, Timestamp, Pattern, CycleLength
        ) %>%
        arrange(SignalID, Phase, Timestamp) %>%
        group_by(SignalID, Phase) %>%
        mutate(
            Lead_EventCode = lead(EventCode),
            Lead_Timestamp = lead(Timestamp),
            Sequence = paste(as.character(EventCode),
                             as.character(Lead_EventCode),
                             Pattern,
                             sep = "_")) %>%
        filter(Sequence %in% c("45_21_Free", "22_21_Coordinated")) %>%
        mutate(Delay = as.numeric(difftime(Lead_Timestamp, Timestamp), units="secs")) %>% #what to do about really long "delay" for 2/6?
        ungroup() %>%
        filter(!(Pattern == "Coordinated" & Delay > CycleLength)) %>% #filter out events where max ped delay/cycle > CL
        filter(!(Pattern == "Free" & Delay > 300)) # filter out events where delay for uncoordinated signals is > 5 min (300 s)

    cat('.')
    pe.free.summary <- filter(pe, Pattern == "Free") %>%
        group_by(SignalID) %>%
        summarise(
            Pattern = "Free",
            Avg.Max.Ped.Delay = sum(Delay) / n(),
            Events = n(),
            .groups = "drop"
        )

    cat('.')
    pe.coordinated.summary.byphase <- filter(pe, Pattern == "Coordinated") %>%
        group_by(SignalID, Phase) %>%
        summarise(
            Pattern = "Coordinated",
            Max.Ped.Delay.per.Cycle = sum(Delay) / n(),
            Events = n(),
            .groups = "drop"
        )

    cat('.')
    pe.coordinated.summary <- pe.coordinated.summary.byphase %>%
        group_by(SignalID) %>%
        summarise(
            Pattern = "Coordinated",
            Avg.Max.Ped.Delay = weighted.mean(Max.Ped.Delay.per.Cycle,Events),
            Events = sum(Events),
            .groups = "drop"
        )

    cat('.')
    pe.summary.overall <- bind_rows(pe.free.summary, pe.coordinated.summary) %>%
        mutate(Date = date_)

    pe.summary.overall
}



get_detection_levels_by_signal <- function(date_) {

    dc <- get_det_config(date_)

    llc <- grepl('Lane-by-lane Count', dc$DetectionTypeDesc)
    adv <- grepl('Advanced Count', dc$DetectionTypeDesc)
    sbp <- grepl('Stop Bar Presence', dc$DetectionTypeDesc)

    dc[llc, "Level"] <- 3
    dc[((llc & sbp) | sbp) & dc$CallPhase %in% c(2,6), "Level"] <- 2
    dc[((llc & sbp) | sbp) & !dc$CallPhase %in% c(2,6), "Level"] <- 1
    dc[((llc & adv) | adv) & dc$CallPhase %in% c(2,6), "Level"] <- 1

    dc %>%
        group_by(SignalID) %>%
        summarize(Level = max(Level), .groups = "drop")
}



get_termination_type <- function(date_, conf, signals_list = NULL) {

    df <- arrow::open_dataset(glue("gs://{conf$bucket}/{conf$key_prefix}/cycles/date={date_}")) %>%
        select(
            SignalID, CallPhase=Phase, EventCode, TermType) %>%
        filter(
            EventCode==1, TermType != 0) %>%
        group_by(
            SignalID, CallPhase, TermType) %>%
        count() %>%
        collect() %>%
        mutate(
            SignalID = factor(SignalID),
            CallPhase = factor(CallPhase)) %>%
        group_by(
            SignalID, CallPhase) %>%
        mutate(
            pct = as.numeric(n)/sum(as.numeric(n))) %>%
        pivot_wider(
            values_from = "pct", names_from = "TermType", values_fill = 0) %>%
        rename(
            Cycles = n, GapOut = `4`, MaxOut = `5`, ForceOff = `6`) %>%
        summarize(
            across(where(is.numeric), sum), .groups = "drop") %>%
        tibble::add_column(
            Date = as_date(date_), .after = "CallPhase")

    if (!is.null(signals_list)) {
        df <- filter(df, SignalID %in% signals_list)
    }
    df
}
