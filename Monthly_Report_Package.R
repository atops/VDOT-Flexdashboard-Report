
# Monthly_Report_Package.R

source("Monthly_Report_Package_init.R")

# options(warn = 2)

# # DETECTOR UPTIME ###########################################################

print(glue("{Sys.time()} Vehicle Detector Uptime [1 of 25]"))

tryCatch(
    {
        cb <- function(x) {
            get_avg_daily_detector_uptime(x) %>%
                mutate(Date = date(Date))
        }

        avg_daily_detector_uptime <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "detector_uptime_pd",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf,
            callback = cb
        ) %>%
            mutate(
                SignalID = factor(SignalID)
            )

        cor_avg_daily_detector_uptime <-
            get_cor_avg_daily_detector_uptime(avg_daily_detector_uptime, corridors)
        sub_avg_daily_detector_uptime <-
            (get_cor_avg_daily_detector_uptime(avg_daily_detector_uptime, subcorridors) %>%
                filter(!is.na(Corridor)))

        weekly_detector_uptime <-
            get_weekly_detector_uptime(avg_daily_detector_uptime)
        cor_weekly_detector_uptime <-
            get_cor_weekly_detector_uptime(weekly_detector_uptime, corridors)
        sub_weekly_detector_uptime <-
            (get_cor_weekly_detector_uptime(weekly_detector_uptime, subcorridors) %>%
                filter(!is.na(Corridor)))

        monthly_detector_uptime <-
            get_monthly_detector_uptime(avg_daily_detector_uptime)
        cor_monthly_detector_uptime <-
            get_cor_monthly_detector_uptime(avg_daily_detector_uptime, corridors)
        sub_monthly_detector_uptime <-
            (get_cor_monthly_detector_uptime(avg_daily_detector_uptime, subcorridors) %>%
                filter(!is.na(Corridor)))

        addtoRDS(
            avg_daily_detector_uptime, "avg_daily_detector_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )
        addtoRDS(
            weekly_detector_uptime, "weekly_detector_uptime.rds", "uptime",
            report_start_date, wk_calcs_start_date
        )
        addtoRDS(
            monthly_detector_uptime, "monthly_detector_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )

        addtoRDS(
            cor_avg_daily_detector_uptime, "cor_avg_daily_detector_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )
        addtoRDS(
            cor_weekly_detector_uptime, "cor_weekly_detector_uptime.rds", "uptime",
            report_start_date, wk_calcs_start_date
        )
        addtoRDS(
            cor_monthly_detector_uptime, "cor_monthly_detector_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )

        addtoRDS(
            sub_avg_daily_detector_uptime, "sub_avg_daily_detector_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )
        addtoRDS(
            sub_weekly_detector_uptime, "sub_weekly_detector_uptime.rds", "uptime",
            report_start_date, wk_calcs_start_date
        )
        addtoRDS(
            sub_monthly_detector_uptime, "sub_monthly_detector_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )

        # rm(ddu)
        # rm(daily_detector_uptime)
        rm(avg_daily_detector_uptime)
        rm(weekly_detector_uptime)
        rm(monthly_detector_uptime)
        rm(cor_avg_daily_detector_uptime)
        rm(cor_weekly_detector_uptime)
        rm(cor_monthly_detector_uptime)
        rm(sub_avg_daily_detector_uptime)
        rm(sub_weekly_detector_uptime)
        rm(sub_monthly_detector_uptime)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# DAILY PEDESTRIAN PUSHBUTTON UPTIME ###############################################

print(glue("{Sys.time()} Ped Pushbutton Uptime [2 of 25]"))

tryCatch(
    {
        pau_start_date <- pmin(
            ymd(calcs_start_date),
            floor_date(ymd(report_end_date) - as.duration("6 months"), "month")
        ) %>%
            format("%F")

        counts_ped_hourly <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "counts_ped_1hr",
            start_date = pau_start_date, # We have to look at a longer duration for pau
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf,
            parallel = FALSE
        ) %>%
            filter(!is.na(CallPhase)) %>%
            mutate(
                SignalID = factor(SignalID),
                Detector = factor(Detector),
                CallPhase = factor(CallPhase),
                Date = date(Date),
                DOW = wday(Date),
                Week = week(Date),
                vol = as.numeric(vol)
            )

        counts_ped_daily <- counts_ped_hourly %>%
            group_by(SignalID, Date, DOW, Week, Detector, CallPhase) %>%
            summarize(papd = sum(vol, na.rm = TRUE), .groups = "drop") %>%
            complete(
                Date = seq(ymd(pau_start_date), ymd(report_end_date), by = "1 day"),
                nesting(SignalID, Detector, CallPhase),
                fill = list("papd"=0)
            ) %>%
            transmute(
                SignalID,
                Date,
                DOW = wday(Date),
                Week = week(Date),
                Detector,
                CallPhase,
                papd)

        papd <- counts_ped_daily
        paph <- counts_ped_hourly %>%
            rename(Hour = Timeperiod, paph = vol)
        rm(counts_ped_daily)
        rm(counts_ped_hourly)

        pau <- get_pau_gamma(papd, paph, corridors, wk_calcs_start_date, pau_start_date)

        # Remove and replace papd for bad days, similar to filtered_counts.
        # Replace papd with papd averaged over all days in the date range
        # for that signal and pushbutton input (detector)
        papd <- pau %>%
            mutate(papd = ifelse(uptime == 1, papd, NA)) %>%
            group_by(SignalID, Detector, CallPhase, yr = year(Date), mo = month(Date)) %>%
            mutate(
                papd = ifelse(uptime == 1, papd, floor(mean(papd, na.rm = TRUE)))
            ) %>%
            ungroup() %>%
            select(SignalID, Detector, CallPhase, Date, DOW, Week, papd, uptime, all)

        # We have do to this here rather than in Monthly_Report_Calcs
        # because we need a longer time series to calculate ped detector uptime
        # based on the exponential distribution method (as least 6 months)
        bad_detectors <- get_bad_ped_detectors(pau) %>%
            filter(Date >= calcs_start_date)

        if (nrow(bad_detectors)) {
            s3_upload_parquet_date_split(
                bad_detectors,
                bucket = conf$bucket,
                prefix = "bad_ped_detectors",
                table_name = "bad_ped_detectors",
                conf = conf,
                parallel = FALSE
            )
        }

        # Hack to make the aggregation functions work
        addtoRDS(
            pau, "pa_uptime.rds", "uptime", report_start_date, calcs_start_date
        )
        pau <- pau %>%
            mutate(CallPhase = Detector)


        daily_pa_uptime <- get_daily_avg(pau, "uptime", peak_only = FALSE)
        weekly_pa_uptime <- get_weekly_avg_by_day(pau, "uptime", peak_only = FALSE)
        monthly_pa_uptime <- get_monthly_avg_by_day(pau, "uptime", "all", peak_only = FALSE)

        cor_daily_pa_uptime <-
            get_cor_weekly_avg_by_day(daily_pa_uptime, corridors, "uptime")
        sub_daily_pa_uptime <-
            get_cor_weekly_avg_by_day(daily_pa_uptime, subcorridors, "uptime") %>%
                filter(!is.na(Corridor))

        cor_weekly_pa_uptime <-
            get_cor_weekly_avg_by_day(weekly_pa_uptime, corridors, "uptime")
        sub_weekly_pa_uptime <-
            get_cor_weekly_avg_by_day(weekly_pa_uptime, subcorridors, "uptime") %>%
                filter(!is.na(Corridor))

        cor_monthly_pa_uptime <-
            get_cor_monthly_avg_by_day(monthly_pa_uptime, corridors, "uptime")
        sub_monthly_pa_uptime <-
            get_cor_monthly_avg_by_day(monthly_pa_uptime, subcorridors, "uptime") %>%
                filter(!is.na(Corridor))

        addtoRDS(
            daily_pa_uptime, "daily_pa_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )
        addtoRDS(
            cor_daily_pa_uptime, "cor_daily_pa_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )
        addtoRDS(
            sub_daily_pa_uptime, "sub_daily_pa_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )

        addtoRDS(
            weekly_pa_uptime, "weekly_pa_uptime.rds", "uptime",
            report_start_date, wk_calcs_start_date
        )
        addtoRDS(
            cor_weekly_pa_uptime, "cor_weekly_pa_uptime.rds", "uptime",
            report_start_date, wk_calcs_start_date
        )
        addtoRDS(
            sub_weekly_pa_uptime, "sub_weekly_pa_uptime.rds", "uptime",
            report_start_date, wk_calcs_start_date
        )

        addtoRDS(
            monthly_pa_uptime, "monthly_pa_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )
        addtoRDS(
            cor_monthly_pa_uptime, "cor_monthly_pa_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )
        addtoRDS(
            sub_monthly_pa_uptime, "sub_monthly_pa_uptime.rds", "uptime",
            report_start_date, calcs_start_date
        )

        # rm(papd)
        # rm(bad_ped_detectors)
        rm(pau)
        rm(daily_pa_uptime)
        rm(weekly_pa_uptime)
        rm(monthly_pa_uptime)
        rm(cor_daily_pa_uptime)
        rm(cor_weekly_pa_uptime)
        rm(cor_monthly_pa_uptime)
        rm(sub_daily_pa_uptime)
        rm(sub_weekly_pa_uptime)
        rm(sub_monthly_pa_uptime)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)


# # WATCHDOG ###########################################################

print(glue("{Sys.time()} watchdog alerts [3 of 25]"))

tryCatch(
    {
        # -- Alerts: detector downtime --
        bad_det <- s3_read_parquet_parallel(
            "bad_detectors",
            start_date = today() - days(90),
            end_date = today() - days(1),
            bucket = conf$bucket,
            conf = conf
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                Detector = factor(Detector)
            )

        det_config <- lapply(sort(unique(bad_det$Date)), function(date_) {
            get_det_config(date_) %>%
                transmute(
                    SignalID,
                    CallPhase,
                    Detector,
                    ApproachDesc,
                    LaneNumber,
                    Date = date_
                )
        }) %>%
            bind_rows() %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Detector = factor(Detector)
            )

        bad_det <- bad_det %>%
            left_join(
                det_config,
                by = c("SignalID", "Detector", "Date")
            ) %>%
            left_join(
                dplyr::select(corridors, Zone_Group, Zone, Corridor, SignalID, Name),
                by = c("SignalID")
            ) %>%
            filter(!is.na(Corridor)) %>%
            transmute(
                Zone_Group,
                Zone,
                Corridor,
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Detector = factor(Detector),
                Date,
                Alert = factor("Bad Vehicle Detection"),
                Name = factor(if_else(Corridor == "Ramp Meter", sub("@", "-", Name), Name)),
                ApproachDesc = if_else(
                    is.na(ApproachDesc),
                    "",
                    as.character(glue("{trimws(ApproachDesc)} Lane {LaneNumber}"))
                )
            )

        # Zone_Group | Zone | Corridor | SignalID/CameraID | CallPhase | DetectorID | Date | Alert | Name
        s3_write_parquet(
            bad_det,
            object = join_path(conf$key_prefix, "mark/watchdog/bad_detectors.parquet"),
            bucket = conf$bucket
        )
        rm(bad_det)
        rm(det_config)

        # -- Alerts: pedestrian detector downtime --
        bad_ped <- s3_read_parquet_parallel(
            "bad_ped_detectors",
            start_date = today() - days(90),
            end_date = today() - days(1),
            bucket = conf$bucket,
            conf = conf
        )

        if (nrow(bad_ped)) {
            bad_ped %>%
                mutate(SignalID = factor(SignalID),
                       Detector = factor(Detector)) %>%

                left_join(
                    dplyr::select(corridors, Zone_Group, Zone, Corridor, SignalID, Name),
                    by = c("SignalID")
                ) %>%
                transmute(Zone_Group,
                          Zone,
                          Corridor = factor(Corridor),
                          SignalID = factor(SignalID),
                          Detector = factor(Detector),
                          Date,
                          Alert = factor("Bad Ped Detection"),
                          Name = factor(Name)
                )
            s3_write_parquet(
                bad_ped,
                object = join_path(conf$key_prefix, "mark/watchdog/bad_ped_pushbuttons.parquet"),
                bucket = conf$bucket)
        }
        rm(bad_ped)

        # -- Alerts: CCTV downtime --


        # -- Alerts: Comm downtime --

        bad_comm <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "comm_quality",
            start_date = today() - days(90),
            end_date = today() - days(1),
            signals_list = signals_list,
            conf = conf,
        )
        if (nrow(bad_comm)) {
            bad_comm <- bad_comm %>%
                mutate(SignalID = factor(AssetNum)) %>%

                left_join(
                    dplyr::select(corridors, Zone_Group, Zone, Corridor, SignalID, Name),
                    by = c("SignalID")
                ) %>%
                filter(
                    !is.na(Corridor),
                    AvgQuality < 90) %>%
                transmute(Zone_Group,
                          Zone,
                          Corridor = factor(Corridor),
                          SignalID = factor(SignalID),
                          CallPhase = factor(0),
                          Detector = factor(0),
                          Date,
                          Alert = factor("Bad Comm"),
                          Name = factor(Name)
                )

            s3_write_parquet(
                bad_comm,
                object = join_path(conf$key_prefix, "mark/watchdog/bad_comm.parquet"),
                bucket = conf$bucket)

        }
        rm(bad_comm)


        # -- Watchdog Alerts --

        # Nothing to do here


        # -- --------------- --

        # Zone_Group | Zone | Corridor | SignalID/CameraID | CallPhase | DetectorID | Date | Alert | Name
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)




# DAILY PEDESTRIAN ACTIVATIONS ################################################

print(glue("{Sys.time()} Daily Pedestrian Activations [4 of 25]"))

tryCatch(
    {
        daily_papd <- get_daily_sum(papd, "papd")
        weekly_papd <- get_weekly_papd(papd)
        monthly_papd <- get_monthly_papd(papd)

        # Group into corridors --------------------------------------------------------
        cor_daily_papd <- get_cor_weekly_papd(daily_papd, corridors) %>% select(-Week)
        cor_weekly_papd <- get_cor_weekly_papd(weekly_papd, corridors)
        cor_monthly_papd <- get_cor_monthly_papd(monthly_papd, corridors)

        # Group into subcorridors --------------------------------------------------------
        sub_daily_papd <- get_cor_weekly_papd(daily_papd, subcorridors) %>% select(-Week) %>%
            filter(!is.na(Corridor))
        sub_weekly_papd <- get_cor_weekly_papd(weekly_papd, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_monthly_papd <- get_cor_monthly_papd(monthly_papd, subcorridors) %>%
            filter(!is.na(Corridor))


        addtoRDS(daily_papd, "daily_papd.rds", "papd", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_papd, "weekly_papd.rds", "papd", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_papd, "monthly_papd.rds", "papd", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_papd, "cor_daily_papd.rds", "papd", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_papd, "cor_weekly_papd.rds", "papd", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_papd, "cor_monthly_papd.rds", "papd", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_papd, "sub_daily_papd.rds", "papd", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_papd, "sub_weekly_papd.rds", "papd", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_papd, "sub_monthly_papd.rds", "papd", report_start_date, calcs_start_date)

        rm(papd)
        rm(daily_papd)
        rm(weekly_papd)
        rm(monthly_papd)
        rm(cor_daily_papd)
        rm(cor_weekly_papd)
        rm(cor_monthly_papd)
        rm(sub_daily_papd)
        rm(sub_weekly_papd)
        rm(sub_monthly_papd)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# HOURLY PEDESTRIAN ACTIVATIONS ###############################################

print(glue("{Sys.time()} Hourly Pedestrian Activations [5 of 25]"))

tryCatch(
    {
        weekly_paph <- get_weekly_paph(paph)
        monthly_paph <- get_monthly_paph(paph)

        # Group into corridors --------------------------------------------------------
        cor_weekly_paph <- get_cor_weekly_paph(weekly_paph, corridors)
        sub_weekly_paph <- get_cor_weekly_paph(weekly_paph, subcorridors) %>%
            filter(!is.na(Corridor))

        # Hourly volumes by Corridor --------------------------------------------------
        cor_monthly_paph <- get_cor_monthly_paph(monthly_paph, corridors)
        sub_monthly_paph <- get_cor_monthly_paph(monthly_paph, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(weekly_paph, "weekly_paph.rds", "paph", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_paph, "monthly_paph.rds", "paph", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_paph, "cor_weekly_paph.rds", "paph", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_paph, "cor_monthly_paph.rds", "paph", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_paph, "sub_weekly_paph.rds", "paph", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_paph, "sub_monthly_paph.rds", "paph", report_start_date, calcs_start_date)

        rm(paph)
        rm(weekly_paph)
        rm(monthly_paph)
        rm(cor_weekly_paph)
        rm(cor_monthly_paph)
        rm(sub_weekly_paph)
        rm(sub_monthly_paph)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# GET PEDESTRIAN DELAY ###################################################

print(glue("{Sys.time()} Pedestrian Delay [6 of 25]"))

tryCatch(
    {
        cb <- function(x) {
            if ("Avg.Max.Ped.Delay" %in% names(x)) {
                x <- x %>%
                    rename(pd = Avg.Max.Ped.Delay) %>%
                    mutate(CallPhase = factor(0))
            }
            x %>%
                mutate(
                    DOW = wday(Date),
                    Week = week(Date)
                )
        }

        ped_delay <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "ped_delay",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf,
            callback = cb
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase)
            ) %>%
            replace_na(list(Events = 1))


        daily_pd <- get_daily_avg(ped_delay, "pd", "Events")
        weekly_pd_by_day <- get_weekly_avg_by_day(ped_delay, "pd", "Events", peak_only = FALSE)
        monthly_pd_by_day <- get_monthly_avg_by_day(ped_delay, "pd", "Events", peak_only = FALSE)

        cor_daily_pd <- get_cor_weekly_avg_by_day(daily_pd, corridors, "pd", "Events") %>% select(-Week)
        cor_weekly_pd_by_day <- get_cor_weekly_avg_by_day(weekly_pd_by_day, corridors, "pd", "Events")
        cor_monthly_pd_by_day <- get_cor_monthly_avg_by_day(monthly_pd_by_day, corridors, "pd", "Events")

        sub_daily_pd <- get_cor_weekly_avg_by_day(daily_pd, subcorridors, "pd", "Events") %>% select(-Week) %>%
            filter(!is.na(Corridor))
        sub_weekly_pd_by_day <- get_cor_weekly_avg_by_day(weekly_pd_by_day, subcorridors, "pd", "Events") %>%
            filter(!is.na(Corridor))
        sub_monthly_pd_by_day <- get_cor_monthly_avg_by_day(monthly_pd_by_day, subcorridors, "pd", "Events") %>%
            filter(!is.na(Corridor))

        addtoRDS(daily_pd, "daily_pd.rds", "pd", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_pd_by_day, "weekly_pd_by_day.rds", "pd", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_pd_by_day, "monthly_pd_by_day.rds", "pd", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_pd, "cor_daily_pd.rds", "pd", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_pd_by_day, "cor_weekly_pd_by_day.rds", "pd", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_pd_by_day, "cor_monthly_pd_by_day.rds", "pd", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_pd, "sub_daily_pd.rds", "pd", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_pd_by_day, "sub_weekly_pd_by_day.rds", "pd", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_pd_by_day, "sub_monthly_pd_by_day.rds", "pd", report_start_date, calcs_start_date)

        rm(ped_delay)
        rm(daily_pd)
        rm(weekly_pd_by_day)
        rm(monthly_pd_by_day)
        rm(cor_daily_pd)
        rm(cor_weekly_pd_by_day)
        rm(cor_monthly_pd_by_day)
        rm(sub_daily_pd)
        rm(sub_weekly_pd_by_day)
        rm(sub_monthly_pd_by_day)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)


# GET COMMUNICATIONS UPTIME ###################################################

print(glue("{Sys.time()} Communication Uptime [7 of 25]"))

tryCatch(
    {
        # New Comm Quality Metric from KITS
        cu <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "comm_quality",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf
        ) %>%
            mutate(
                SignalID = factor(AssetNum),
                CallPhase = 0,
                uptime = AvgQuality/100,
                CallPhase = factor(CallPhase),
                Date = date(Date),
                DOW = wday(Date),
                Week = week(Date)
            ) %>%
            filter(SignalID %in% corridors$SignalID, Date == as_date(CSDATE)) %>%
            left_join(select(corridors, SignalID, Asof), by = "SignalID") %>%
            filter(Date >= Asof)

        daily_comm_uptime <- get_daily_avg(cu, "uptime", peak_only = FALSE)
        cor_daily_comm_uptime <-
            get_cor_weekly_avg_by_day(daily_comm_uptime, corridors, "uptime")
        sub_daily_comm_uptime <-
            (get_cor_weekly_avg_by_day(daily_comm_uptime, subcorridors, "uptime") %>%
                filter(!is.na(Corridor)))

        weekly_comm_uptime <- get_weekly_avg_by_day(cu, "uptime", peak_only = FALSE)
        cor_weekly_comm_uptime <-
            get_cor_weekly_avg_by_day(weekly_comm_uptime, corridors, "uptime")
        sub_weekly_comm_uptime <-
            (get_cor_weekly_avg_by_day(weekly_comm_uptime, subcorridors, "uptime") %>%
                filter(!is.na(Corridor)))

        monthly_comm_uptime <- get_monthly_avg_by_day(cu, "uptime", peak_only = FALSE)
        cor_monthly_comm_uptime <-
            get_cor_monthly_avg_by_day(monthly_comm_uptime, corridors, "uptime")
        sub_monthly_comm_uptime <-
            (get_cor_monthly_avg_by_day(monthly_comm_uptime, subcorridors, "uptime") %>%
                filter(!is.na(Corridor)))


        addtoRDS(daily_comm_uptime, "daily_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_comm_uptime, "cor_daily_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_comm_uptime, "sub_daily_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)

        addtoRDS(weekly_comm_uptime, "weekly_comm_uptime.rds", "uptime", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_comm_uptime, "cor_weekly_comm_uptime.rds", "uptime", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_comm_uptime, "sub_weekly_comm_uptime.rds", "uptime", report_start_date, wk_calcs_start_date)

        addtoRDS(monthly_comm_uptime, "monthly_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)
        addtoRDS(cor_monthly_comm_uptime, "cor_monthly_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)
        addtoRDS(sub_monthly_comm_uptime, "sub_monthly_comm_uptime.rds", "uptime", report_start_date, calcs_start_date)

        rm(cu)
        rm(daily_comm_uptime)
        rm(weekly_comm_uptime)
        rm(monthly_comm_uptime)
        rm(cor_daily_comm_uptime)
        rm(cor_weekly_comm_uptime)
        rm(cor_monthly_comm_uptime)
        rm(sub_daily_comm_uptime)
        rm(sub_weekly_comm_uptime)
        rm(sub_monthly_comm_uptime)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# DAILY VOLUMES ###############################################################

print(glue("{Sys.time()} Daily Volumes [8 of 25]"))

tryCatch(
    {
        vpd <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "vehicles_pd",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date)
            )

        daily_vpd <- get_daily_sum(vpd, "vpd")
        weekly_vpd <- get_weekly_vpd(vpd)
        monthly_vpd <- get_monthly_vpd(vpd)

        # Group into corridors --------------------------------------------------------
        cor_daily_vpd <- get_cor_weekly_vpd(daily_vpd, corridors) %>% select(-ones, -Week)
        cor_weekly_vpd %<-% get_cor_weekly_vpd(weekly_vpd, corridors)
        cor_monthly_vpd %<-% get_cor_monthly_vpd(monthly_vpd, corridors)

        # Subcorridors
        sub_daily_vpd <- get_cor_weekly_vpd(daily_vpd, subcorridors) %>% select(-ones, -Week) %>%
            filter(!is.na(Corridor))
        sub_weekly_vpd <-
            get_cor_weekly_vpd(weekly_vpd, subcorridors) %>%
                filter(!is.na(Corridor))
        sub_monthly_vpd <-
            get_cor_monthly_vpd(monthly_vpd, subcorridors) %>%
                filter(!is.na(Corridor))

        # Monthly % change from previous month by corridor ----------------------------
        addtoRDS(daily_vpd, "daily_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_vpd, "weekly_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_vpd, "monthly_vpd.rds", "vpd", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_vpd, "cor_daily_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_vpd, "cor_weekly_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_vpd, "cor_monthly_vpd.rds", "vpd", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_vpd, "sub_daily_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_vpd, "sub_weekly_vpd.rds", "vpd", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_vpd, "sub_monthly_vpd.rds", "vpd", report_start_date, calcs_start_date)

        rm(vpd)
        rm(daily_vpd)
        rm(weekly_vpd)
        rm(monthly_vpd)
        rm(cor_daily_vpd)
        rm(cor_weekly_vpd)
        rm(cor_monthly_vpd)
        rm(sub_daily_vpd)
        rm(sub_weekly_vpd)
        rm(sub_monthly_vpd)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# HOURLY VOLUMES ##############################################################

print(glue("{Sys.time()} Hourly Volumes [9 of 25]"))

tryCatch(
    {
        vph <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "vehicles_ph",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(2), # Hack because next function needs a CallPhase
                Date = date(Date)
            )

        hourly_vol <- get_hourly(vph, "vph", corridors)

        cor_daily_vph <- get_cor_weekly_vph(hourly_vol, corridors)
        sub_daily_vph <- get_cor_weekly_vph(hourly_vol, subcorridors) %>%
            filter(!is.na(Corridor))

        daily_vph_peak <- get_weekly_vph_peak(hourly_vol)
        cor_daily_vph_peak <- get_cor_weekly_vph_peak(cor_daily_vph)
        sub_daily_vph_peak <- get_cor_weekly_vph_peak(sub_daily_vph) %>%
            map(~ filter(., !is.na(Corridor)))

        weekly_vph <- get_weekly_vph(vph)
        cor_weekly_vph <- get_cor_weekly_vph(weekly_vph, corridors)
        sub_weekly_vph <- get_cor_weekly_vph(weekly_vph, subcorridors) %>%
            filter(!is.na(Corridor))

        weekly_vph_peak <- get_weekly_vph_peak(weekly_vph)
        cor_weekly_vph_peak <- get_cor_weekly_vph_peak(cor_weekly_vph)
        sub_weekly_vph_peak <- get_cor_weekly_vph_peak(sub_weekly_vph) %>%
            map(~ filter(., !is.na(Corridor)))

        monthly_vph <- get_monthly_vph(vph)
        cor_monthly_vph <- get_cor_monthly_vph(monthly_vph, corridors)
        sub_monthly_vph <- get_cor_monthly_vph(monthly_vph, subcorridors) %>%
            filter(!is.na(Corridor))

        monthly_vph_peak <- get_monthly_vph_peak(monthly_vph)
        cor_monthly_vph_peak <- get_cor_monthly_vph_peak(cor_monthly_vph)
        sub_monthly_vph_peak <- get_cor_monthly_vph_peak(sub_monthly_vph) %>%
            map(~ filter(., !is.na(Corridor)))


        addtoRDS(daily_vph_peak$am, "daily_vph_am.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(daily_vph_peak$pm, "daily_vph_pm.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(weekly_vph, "weekly_vph.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_vph, "monthly_vph.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_vph, "cor_daily_vph.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_vph, "cor_weekly_vph.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_vph, "cor_monthly_vph.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_vph, "sub_daily_vph.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_vph, "sub_weekly_vph.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_vph, "sub_monthly_vph.rds", "vph", report_start_date, calcs_start_date)

        addtoRDS(weekly_vph_peak$am, "weekly_vph_am.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_vph_peak$pm, "weekly_vph_pm.rds", "vph", report_start_date, wk_calcs_start_date)

        addtoRDS(monthly_vph_peak$am, "monthly_vph_am.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(monthly_vph_peak$pm, "monthly_vph_pm.rds", "vph", report_start_date, calcs_start_date)

        addtoRDS(cor_daily_vph_peak$am, "cor_daily_vph_am.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_vph_peak$pm, "cor_daily_vph_pm.rds", "vph", report_start_date, calcs_start_date)

        addtoRDS(cor_weekly_vph_peak$am, "cor_weekly_vph_am.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_vph_peak$pm, "cor_weekly_vph_pm.rds", "vph", report_start_date, wk_calcs_start_date)

        addtoRDS(cor_monthly_vph_peak$am, "cor_monthly_vph_am.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(cor_monthly_vph_peak$pm, "cor_monthly_vph_pm.rds", "vph", report_start_date, calcs_start_date)

        addtoRDS(sub_daily_vph_peak$am, "sub_daily_vph_am.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_vph_peak$pm, "sub_daily_vph_pm.rds", "vph", report_start_date, calcs_start_date)

        addtoRDS(sub_weekly_vph_peak$am, "sub_weekly_vph_am.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_vph_peak$pm, "sub_weekly_vph_pm.rds", "vph", report_start_date, wk_calcs_start_date)

        addtoRDS(sub_monthly_vph_peak$am, "sub_monthly_vph_am.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(sub_monthly_vph_peak$pm, "sub_monthly_vph_pm.rds", "vph", report_start_date, calcs_start_date)

        rm(vph)
        rm(hourly_vol)
        rm(cor_daily_vph)
        rm(sub_daily_vph)

        rm(weekly_vph)
        rm(cor_weekly_vph)
        rm(sub_weekly_vph)

        rm(monthly_vph)
        rm(cor_monthly_vph)
        rm(sub_monthly_vph)

        rm(weekly_vph_am)
        rm(monthly_vph_am)
        rm(cor_weekly_vph_am)
        rm(cor_monthly_vph_am)
        rm(sub_weekly_vph_am)
        rm(sub_monthly_vph_am)

        rm(weekly_vph_pm)
        rm(monthly_vph_pm)
        rm(cor_weekly_vph_pm)
        rm(cor_monthly_vph_pm)
        rm(sub_weekly_vph_pm)
        rm(sub_monthly_vph_pm)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)






# DAILY THROUGHPUT ############################################################

print(glue("{Sys.time()} Daily Throughput [10 of 25]"))

tryCatch(
    {
        throughput <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "throughput",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(as.integer(CallPhase)),
                Date = date(Date)
            )

        daily_throughput <- get_daily_sum(throughput, "vph")
        weekly_throughput %<-% get_weekly_thruput(throughput)
        monthly_throughput %<-% get_monthly_thruput(throughput)

        # Daily throughput
        cor_daily_throughput <- get_cor_weekly_thruput(daily_throughput, corridors) %>% select(-Week)
        sub_daily_throughput <- get_cor_weekly_thruput(daily_throughput, subcorridors) %>% select(-Week) %>%
            filter(!is.na(Corridor))

        # Weekly throughput - Group into corridors ---------------------------------
        cor_weekly_throughput <- get_cor_weekly_thruput(weekly_throughput, corridors)
        sub_weekly_throughput <- get_cor_weekly_thruput(weekly_throughput, subcorridors) %>%
            filter(!is.na(Corridor))

        # Monthly throughput - Group into corridors
        cor_monthly_throughput <- get_cor_monthly_thruput(monthly_throughput, corridors)
        sub_monthly_throughput <- get_cor_monthly_thruput(monthly_throughput, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(daily_throughput, "daily_throughput.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_throughput, "weekly_throughput.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_throughput, "monthly_throughput.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_throughput, "cor_daily_throughput.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_throughput, "cor_weekly_throughput.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_throughput, "cor_monthly_throughput.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_throughput, "sub_daily_throughput.rds", "vph", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_throughput, "sub_weekly_throughput.rds", "vph", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_throughput, "sub_monthly_throughput.rds", "vph", report_start_date, calcs_start_date)

        rm(throughput)
        rm(daily_throughput)
        rm(weekly_throughput)
        rm(monthly_throughput)
        rm(cor_daily_throughput)
        rm(cor_weekly_throughput)
        rm(cor_monthly_throughput)
        rm(sub_daily_throughput)
        rm(sub_weekly_throughput)
        rm(sub_monthly_throughput)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)




# DAILY ARRIVALS ON GREEN #####################################################

print(glue("{Sys.time()} Daily AOG [11 of 25]"))

tryCatch(
    {
        aog <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "arrivals_on_green",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date),
                DOW = wday(Date),
                Week = week(Date)
            )

        daily_aog <- get_daily_aog(aog)
        weekly_aog_by_day <- get_weekly_aog_by_day(aog)
        monthly_aog_by_day <- get_monthly_aog_by_day(aog)

        cor_daily_aog <- get_cor_weekly_aog_by_day(daily_aog, corridors) %>% select(-Week)
        cor_weekly_aog_by_day <- get_cor_weekly_aog_by_day(weekly_aog_by_day, corridors)
        cor_monthly_aog_by_day <- get_cor_monthly_aog_by_day(monthly_aog_by_day, corridors)

        sub_daily_aog <- get_cor_weekly_aog_by_day(daily_aog, subcorridors) %>% select(-Week) %>%
            filter(!is.na(Corridor))
        sub_weekly_aog_by_day <- get_cor_weekly_aog_by_day(weekly_aog_by_day, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_monthly_aog_by_day <- get_cor_monthly_aog_by_day(monthly_aog_by_day, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(daily_aog, "daily_aog.rds", "aog", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_aog_by_day, "weekly_aog_by_day.rds", "aog", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_aog_by_day, "monthly_aog_by_day.rds", "aog", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_aog, "cor_daily_aog.rds", "aog", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_aog_by_day, "cor_weekly_aog_by_day.rds", "aog", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_aog_by_day, "cor_monthly_aog_by_day.rds", "aog", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_aog, "sub_daily_aog.rds", "aog", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_aog_by_day, "sub_weekly_aog_by_day.rds", "aog", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_aog_by_day, "sub_monthly_aog_by_day.rds", "aog", report_start_date, calcs_start_date)

        rm(daily_aog)
        rm(weekly_aog_by_day)
        rm(monthly_aog_by_day)
        rm(cor_weekly_aog_by_day)
        rm(cor_monthly_aog_by_day)
        rm(sub_weekly_aog_by_day)
        rm(sub_monthly_aog_by_day)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# HOURLY ARRIVALS ON GREEN ####################################################

print(glue("{Sys.time()} Hourly AOG [12 of 25]"))

tryCatch(
    {
        aog_by_hr <- get_aog_by_hr(aog)
        monthly_aog_by_hr <- get_monthly_aog_by_hr(aog_by_hr)

        # Hourly volumes by Corridor --------------------------------------------------
        cor_monthly_aog_by_hr <- get_cor_monthly_aog_by_hr(monthly_aog_by_hr, corridors)
        sub_monthly_aog_by_hr <- get_cor_monthly_aog_by_hr(monthly_aog_by_hr, subcorridors) %>%
            filter(!is.na(Corridor))

        # cor_monthly_aog_peak <- get_cor_monthly_aog_peak(cor_monthly_aog_by_hr)

        addtoRDS(monthly_aog_by_hr, "monthly_aog_by_hr.rds", "aog", report_start_date, calcs_start_date)
        addtoRDS(cor_monthly_aog_by_hr, "cor_monthly_aog_by_hr.rds", "aog", report_start_date, calcs_start_date)
        addtoRDS(sub_monthly_aog_by_hr, "sub_monthly_aog_by_hr.rds", "aog", report_start_date, calcs_start_date)

        rm(aog_by_hr)
        # rm(cor_monthly_aog_peak)
        rm(monthly_aog_by_hr)
        rm(cor_monthly_aog_by_hr)
        rm(sub_monthly_aog_by_hr)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# DAILY PROGRESSION RATIO #####################################################

print(glue("{Sys.time()} Daily Progression Ratio [13 of 25]"))

tryCatch(
    {
        daily_pr <- get_daily_avg(aog, "pr", "vol")
        weekly_pr_by_day <- get_weekly_pr_by_day(aog)
        monthly_pr_by_day <- get_monthly_pr_by_day(aog)

        cor_daily_pr <- get_cor_weekly_pr_by_day(daily_pr, corridors) %>% select(-Week)
        cor_weekly_pr_by_day <- get_cor_weekly_pr_by_day(weekly_pr_by_day, corridors)
        cor_monthly_pr_by_day <- get_cor_monthly_pr_by_day(monthly_pr_by_day, corridors)

        sub_daily_pr <- get_cor_weekly_pr_by_day(daily_pr, subcorridors) %>% select(-Week) %>%
            filter(!is.na(Corridor))
        sub_weekly_pr_by_day <- get_cor_weekly_pr_by_day(weekly_pr_by_day, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_monthly_pr_by_day <- get_cor_monthly_pr_by_day(monthly_pr_by_day, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(daily_pr, "daily_pr.rds", "pr", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_pr_by_day, "weekly_pr_by_day.rds", "pr", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_pr_by_day, "monthly_pr_by_day.rds", "pr", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_pr, "cor_daily_pr.rds", "pr", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_pr_by_day, "cor_weekly_pr_by_day.rds", "pr", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_pr_by_day, "cor_monthly_pr_by_day.rds", "pr", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_pr, "sub_daily_pr.rds", "pr", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_pr_by_day, "sub_weekly_pr_by_day.rds", "pr", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_pr_by_day, "sub_monthly_pr_by_day.rds", "pr", report_start_date, calcs_start_date)

        rm(daily_pr)
        rm(weekly_pr_by_day)
        rm(monthly_pr_by_day)
        rm(cor_daily_pr)
        rm(cor_weekly_pr_by_day)
        rm(cor_monthly_pr_by_day)
        rm(sub_daily_pr)
        rm(sub_weekly_pr_by_day)
        rm(sub_monthly_pr_by_day)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# HOURLY PROGESSION RATIO ####################################################

print(glue("{Sys.time()} Hourly Progression Ratio [14 of 25]"))

tryCatch(
    {
        pr_by_hr <- get_pr_by_hr(aog)
        monthly_pr_by_hr <- get_monthly_pr_by_hr(pr_by_hr)

        # Hourly volumes by Corridor --------------------------------------------------
        cor_monthly_pr_by_hr <- get_cor_monthly_pr_by_hr(monthly_pr_by_hr, corridors)
        sub_monthly_pr_by_hr <- get_cor_monthly_pr_by_hr(monthly_pr_by_hr, subcorridors) %>%
            filter(!is.na(Corridor))

        # cor_monthly_pr_peak <- get_cor_monthly_pr_peak(cor_monthly_pr_by_hr)

        addtoRDS(monthly_pr_by_hr, "monthly_pr_by_hr.rds", "pr", report_start_date, calcs_start_date)
        addtoRDS(cor_monthly_pr_by_hr, "cor_monthly_pr_by_hr.rds", "pr", report_start_date, calcs_start_date)
        addtoRDS(sub_monthly_pr_by_hr, "sub_monthly_pr_by_hr.rds", "pr", report_start_date, calcs_start_date)

        rm(aog)
        rm(pr_by_hr)
        rm(monthly_pr_by_hr)
        rm(cor_monthly_pr_by_hr)
        rm(sub_monthly_pr_by_hr)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)





# DAILY SPLIT FAILURES #####################################################

tryCatch(
    {
        print(glue("{Sys.time()} Daily Split Failures [15 of 25]"))

        sf <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "split_failures",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf,
            callback = function(x) filter(x, CallPhase == 0)
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date)
            )

        # Divide into peak/off-peak split failures
        # -------------------------------------------------------------------------
        sfo <- sf %>% filter(!hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))
        sfp <- sf %>% filter(hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))
        # -------------------------------------------------------------------------

        daily_sfp <- get_daily_avg(sfp, "sf_freq", "cycles")
        daily_sfo <- get_daily_avg(sfo, "sf_freq", "cycles")

        weekly_sf_by_day <- get_weekly_avg_by_day(
            sfp, "sf_freq", "cycles",
            peak_only = FALSE
        )
        weekly_sfo_by_day <- get_weekly_avg_by_day(
            sfo, "sf_freq", "cycles",
            peak_only = FALSE
        )
        monthly_sf_by_day <- get_monthly_avg_by_day(
            sfp, "sf_freq", "cycles",
            peak_only = FALSE
        )
        monthly_sfo_by_day <- get_monthly_avg_by_day(
            sfo, "sf_freq", "cycles",
            peak_only = FALSE
        )

        cor_daily_sfp <- get_cor_weekly_sf_by_day(daily_sfp, corridors) %>% select(-Week)
        cor_daily_sfo <- get_cor_weekly_sf_by_day(daily_sfo, corridors) %>% select(-Week)
        cor_weekly_sf_by_day <- get_cor_weekly_sf_by_day(weekly_sf_by_day, corridors)
        cor_weekly_sfo_by_day <- get_cor_weekly_sf_by_day(weekly_sfo_by_day, corridors)
        cor_monthly_sf_by_day <- get_cor_monthly_sf_by_day(monthly_sf_by_day, corridors)
        cor_monthly_sfo_by_day <- get_cor_monthly_sf_by_day(monthly_sfo_by_day, corridors)

        sub_daily_sfp <- get_cor_weekly_sf_by_day(daily_sfp, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_daily_sfo <- get_cor_weekly_sf_by_day(daily_sfo, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_weekly_sf_by_day <- get_cor_weekly_sf_by_day(weekly_sf_by_day, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_weekly_sfo_by_day <- get_cor_weekly_sf_by_day(weekly_sfo_by_day, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_monthly_sf_by_day <- get_cor_monthly_sf_by_day(monthly_sf_by_day, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_monthly_sfo_by_day <- get_cor_monthly_sf_by_day(monthly_sfo_by_day, subcorridors) %>%
            filter(!is.na(Corridor))


        ############################### Ended here 9/13

        addtoRDS(daily_sfp, "daily_sfp.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_sf_by_day, "wsf.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_sf_by_day, "monthly_sfd.rds", "sf_freq", report_start_date, calcs_start_date)

        addtoRDS(cor_daily_sfp, "cor_daily_sfp.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_sf_by_day, "cor_wsf.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_sf_by_day, "cor_monthly_sfd.rds", "sf_freq", report_start_date, calcs_start_date)

        addtoRDS(sub_daily_sfp, "sub_daily_sfp.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_sf_by_day, "sub_wsf.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_sf_by_day, "sub_monthly_sfd.rds", "sf_freq", report_start_date, calcs_start_date)

        addtoRDS(daily_sfo, "daily_sfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(weekly_sfo_by_day, "wsfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_sfo_by_day, "monthly_sfo.rds", "sf_freq", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_sfo, "cor_daily_sfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_weekly_sfo_by_day, "cor_wsfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_sfo_by_day, "cor_monthly_sfo.rds", "sf_freq", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_sfo, "sub_daily_sfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_weekly_sfo_by_day, "sub_wsfo.rds", "sf_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_sfo_by_day, "sub_monthly_sfo.rds", "sf_freq", report_start_date, calcs_start_date)

        rm(sfp)
        rm(sfo)
        rm(daily_sfp)
        rm(weekly_sf_by_day)
        rm(monthly_sf_by_day)
        rm(cor_daily_sfp)
        rm(cor_weekly_sf_by_day)
        rm(cor_monthly_sf_by_day)
        rm(sub_daily_sfp)
        rm(sub_weekly_sf_by_day)
        rm(sub_monthly_sf_by_day)

        rm(daily_sfo)
        rm(weekly_sfo_by_day)
        rm(monthly_sfo_by_day)
        rm(cor_daily_sfo)
        rm(cor_weekly_sfo_by_day)
        rm(cor_monthly_sfo_by_day)
        rm(sub_daily_sfo)
        rm(sub_weekly_sfo_by_day)
        rm(sub_monthly_sfo_by_day)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# HOURLY SPLIT FAILURES #######################################################

print(glue("{Sys.time()} Hourly Split Failures [16 of 25]"))

tryCatch(
    {
        sfh <- get_sf_by_hr(sf)
        msfh <- get_monthly_sf_by_hr(sfh)

        # Hourly volumes by Corridor --------------------------------------------------
        cor_msfh <- get_cor_monthly_sf_by_hr(msfh, corridors)
        sub_msfh <- get_cor_monthly_sf_by_hr(msfh, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(msfh, "msfh.rds", "sf_freq", report_start_date, calcs_start_date)
        addtoRDS(cor_msfh, "cor_msfh.rds", "sf_freq", report_start_date, calcs_start_date)
        addtoRDS(sub_msfh, "sub_msfh.rds", "sf_freq", report_start_date, calcs_start_date)

        rm(sf)
        rm(sfh)
        rm(msfh)
        rm(cor_msfh)
        rm(sub_msfh)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# DAILY QUEUE SPILLBACK #######################################################

print(glue("{Sys.time()} Daily Queue Spillback [17 of 25]"))

tryCatch(
    {
        qs <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "queue_spillback",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date)
            )

        daily_qs <- get_daily_avg(qs, "qs_freq", "cycles")
        wqs <- get_weekly_qs_by_day(qs)
        monthly_qsd <- get_monthly_qs_by_day(qs)

        cor_daily_qs <- get_cor_weekly_qs_by_day(daily_qs, corridors) %>% select(-Week)
        cor_wqs <- get_cor_weekly_qs_by_day(wqs, corridors)
        cor_monthly_qsd <- get_cor_monthly_qs_by_day(monthly_qsd, corridors)

        sub_daily_qs <- get_cor_weekly_qs_by_day(daily_qs, subcorridors) %>% select(-Week) %>%
            filter(!is.na(Corridor))
        sub_wqs <- get_cor_weekly_qs_by_day(wqs, subcorridors) %>%
            filter(!is.na(Corridor))
        sub_monthly_qsd <- get_cor_monthly_qs_by_day(monthly_qsd, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(daily_qs, "daily_qsd.rds", "qs_freq", report_start_date, calcs_start_date)
        addtoRDS(wqs, "wqs.rds", "qs_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_qsd, "monthly_qsd.rds", "qs_freq", report_start_date, calcs_start_date)
        addtoRDS(cor_daily_qs, "cor_daily_qsd.rds", "qs_freq", report_start_date, calcs_start_date)
        addtoRDS(cor_wqs, "cor_wqs.rds", "qs_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_qsd, "cor_monthly_qsd.rds", "qs_freq", report_start_date, calcs_start_date)
        addtoRDS(sub_daily_qs, "sub_daily_qsd.rds", "qs_freq", report_start_date, calcs_start_date)
        addtoRDS(sub_wqs, "sub_wqs.rds", "qs_freq", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_qsd, "sub_monthly_qsd.rds", "qs_freq", report_start_date, calcs_start_date)

        rm(daily_qs)
        rm(wqs)
        rm(monthly_qsd)
        rm(cor_daily_qs)
        rm(cor_wqs)
        rm(cor_monthly_qsd)
        rm(sub_daily_qs)
        rm(sub_wqs)
        rm(sub_monthly_qsd)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)




# HOURLY QUEUE SPILLBACK ######################################################

print(glue("{Sys.time()} Hourly Queue Spillback [18 of 25]"))

tryCatch(
    {
        qsh <- get_qs_by_hr(qs)
        mqsh <- get_monthly_qs_by_hr(qsh)

        # Hourly volumes by Corridor --------------------------------------------------
        cor_mqsh <- get_cor_monthly_qs_by_hr(mqsh, corridors)
        sub_mqsh <- get_cor_monthly_qs_by_hr(mqsh, subcorridors) %>%
            filter(!is.na(Corridor))

        addtoRDS(mqsh, "mqsh.rds", "qs_freq", report_start_date, calcs_start_date)
        addtoRDS(cor_mqsh, "cor_mqsh.rds", "qs_freq", report_start_date, calcs_start_date)
        addtoRDS(sub_mqsh, "sub_mqsh.rds", "qs_freq", report_start_date, calcs_start_date)

        rm(qs)
        rm(qsh)
        rm(mqsh)
        rm(cor_mqsh)
        rm(sub_mqsh)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# TRAVEL TIME AND BUFFER TIME INDEXES #########################################

print(glue("{Sys.time()} Travel Time Indexes [19 of 25]"))
if (FALSE) {
tryCatch(
    {
        # ------- Corridor/Subcorridor Travel Time Metrics ------- #

        tt <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "cor_travel_times_1hr",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            conf = conf
        ) %>%
            mutate(Corridor = factor(Corridor)) %>%
            left_join(distinct(all_corridors, Zone_Group, Zone, Corridor)) %>%
            filter(!is.na(Zone_Group))


        vars <- c("tti", "pti", "tt", "spd")

        grouping_cols <- list(
            cor = c("Zone_Group", "Zone", "Corridor"),
            sub = c("Zone_Group", "Zone", "Corridor", "Subcorridor"))

        select_function <- list(
            cor = function(df) {
                select(df, Zone_Group, Zone, Corridor, Hour, all_of(vars))},
            sub = function(df) {
                select(df, Zone_Group = Zone, Zone = Corridor, Corridor = Subcorridor, Hour, all_of(vars))})

        corridors_df <- list(
            cor = all_corridors,
            sub = subcorridors)


        round_date_function <- list(
            monthly = function(df) {
                mutate(df, Hour = Hour - days(day(Hour) - 1))},
            weekly = function(df) {
                tuesdays_df <- transmute(df, Date = Hour) %>% get_Tuesdays()
                df %>%
                    left_join(tuesdays_df, by = "Week") %>%
                    mutate(Hour = Date + hours(hour(Hour)))})

        convert_date_function <- list(
            monthly = function(df) {df}, # Do nothing
            weekly = function(df) {rename(df, Date = Month)})

        calcs_start_date_ <- list(
            monthly = calcs_start_date,
            weekly = wk_calcs_start_date
        )

        period_abbr = list(
            monthly = "mo",
            weekly = "wk"
        )

        aurora <- keep_trying(get_aurora_connection, n_tries = 5)

        # For every combination of: cor|sub, monthly|weekly, tti|pti|bi|spd

        for (corr_level in c("cor", "sub")) {
            for (period in c("monthly", "weekly")) {
                # TODO: Add daily if we start collecting the data every day instead of just TWR

                ti <- tt %>%
                    group_by(across(all_of(c(grouping_cols[[corr_level]], "Hour")))) %>%
                    summarize(
                        travel_time_minutes = sum(travel_time_minutes),
                        reference_minutes = sum(reference_minutes),
                        miles = sum(miles),
                        .groups = "drop") %>%
                    mutate(Week = week(as_date(Hour))) %>%

                    round_date_function[[period]]() %>%
                    group_by(across(all_of(c(grouping_cols[[corr_level]], "Hour")))) %>%
                    summarize(
                        tti = mean(travel_time_minutes)/ mean(reference_minutes),
                        pti = quantile(travel_time_minutes, 0.90)/ mean(reference_minutes),
                        tt = mean(travel_time_minutes),
                        spd = max(miles)/mean(travel_time_minutes) * 60,
                        .groups = "drop"
                    ) %>%
                    select_function[[corr_level]]()


                vph <- readRDS(glue("{corr_level}_{period}_vph.rds")) %>%
                    rename(Zone = Zone_Group) %>%
                    left_join(distinct(corridors_df[[corr_level]], Zone_Group, Zone), by = "Zone")

                per <- period_abbr[[period]]

                for (var in vars) {
                    df <- get_cor_monthly_ti_by_hr(ti, var, vph, corridors_df[[corr_level]])
                    td <- tibble(
                        data = list(df),
                        fn = glue("{corr_level}/{per}/{var}h.parquet"),
                        var = var,
                        rsd = report_start_date,
                        csd = calcs_start_date_[[period]])
                    # print(td$data)
                    write_aggregations(aurora, td)
                }

                for (var in vars) {
                    df <- get_cor_monthly_ti_by_day(ti, var, vph, corridors_df[[corr_level]]) %>%
                        convert_date_function[[period]]()
                    td <- tibble(
                        data = list(df),
                        fn = glue("{corr_level}/{per}/{var}.parquet"),
                        var = var,
                        rsd = report_start_date,
                        csd = calcs_start_date_[[period]])
                    # print(td$data)
                    write_aggregations(aurora, td)
                }

            }
        }
        dbDisconnect(aurora)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)
}


# DAILY DETECTION LEVELS #####################################################

tryCatch(
    {
        print(glue("{Sys.time()} Daily Detection Levels [20 of 25]"))

        dl <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "detection_levels",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf,
            callback = function(x)
                mutate(x, CallPhase = 0, Week = week(Date), DOW = wday(Date))
        ) %>%
            transmute(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Level,
                Week,
                DOW,
                Date = date(Date)
            )

        weekly_dl_by_day <- get_weekly_avg_by_day(dl, "Level", peak_only = FALSE)
        cor_weekly_dl_by_day <- get_cor_weekly_avg_by_day(weekly_dl_by_day, corridors, "Level")
        sub_weekly_dl_by_day <- get_cor_weekly_avg_by_day(weekly_dl_by_day, subcorridors, "Level") %>%
            filter(!is.na(Corridor))

        monthly_dl_by_day <- get_monthly_avg_by_day(dl, "Level", peak_only = FALSE)
        cor_monthly_dl_by_day <- get_cor_monthly_avg_by_day(monthly_dl_by_day, corridors, "Level")
        sub_monthly_dl_by_day <- get_cor_monthly_avg_by_day(monthly_dl_by_day, subcorridors, "Level") %>%
            filter(!is.na(Corridor))


        addtoRDS(weekly_dl_by_day, "weekly_dl.rds", "Level", report_start_date, wk_calcs_start_date)
        addtoRDS(monthly_dl_by_day, "monthly_dl.rds", "Level", report_start_date, calcs_start_date)
        addtoRDS(cor_weekly_dl_by_day, "cor_weekly_dl.rds", "Level", report_start_date, wk_calcs_start_date)
        addtoRDS(cor_monthly_dl_by_day, "cor_monthly_dl.rds", "Level", report_start_date, calcs_start_date)
        addtoRDS(sub_weekly_dl_by_day, "sub_weekly_dl.rds", "Level", report_start_date, wk_calcs_start_date)
        addtoRDS(sub_monthly_dl_by_day, "sub_monthly_dl.rds", "Level", report_start_date, calcs_start_date)


        rm(dl)
        rm(weekly_dl_by_day)
        rm(monthly_dl_by_day)
        rm(cor_weekly_dl_by_day)
        rm(cor_monthly_dl_by_day)
        rm(sub_weekly_dl_by_day)
        rm(sub_monthly_dl_by_day)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# GREEN PHASE TERMINATIONS ############################################################

print(glue("{Sys.time()} Termination Types [21 of 25]"))

tryCatch(
    {

        daily <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "phase_termination",
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Week = week(Date)
            )

        aurora <- keep_trying(get_aurora_connection, n_tries = 5)

        for (metric in list(gap_outs, max_outs, force_offs)) {
            aggregate(metric, daily, aurora)
        }

        dbDisconnect(aurora)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# TIME IN TRANSITION ##################################################################

print(glue("{Sys.time()} Time in Transition [22 of 25]"))

tryCatch(
    {
        metric <- time_in_transition

        daily <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = metric$s3table,
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = 0,
                Week = week(Date),
                tint = TimeInTransition_sum, # total minutes in transition
                ones = 1
            )

        aurora <- keep_trying(get_aurora_connection, n_tries = 5)

        aggregate(metric, daily, aurora)

        dbDisconnect(aurora)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)


# APPROACH DELAY ######################################################################

print(glue("{Sys.time()} Approach Delay [23 of 25]"))

tryCatch(
    {
        metric <- approach_delay

        daily <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = metric$s3table,
            start_date = wk_calcs_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf
        ) %>%
            get_daily_avg(metric$variable, metric$weight) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(0),
                Week = week(Date)
            )

        aurora <- keep_trying(get_aurora_connection, n_tries = 5)

        aggregate(metric, daily, aurora)

        dbDisconnect(aurora)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



# Package up for Flexdashboard

print(glue("{Sys.time()} Package for Monthly Report [24 of 25]"))


tryCatch(
    {
        cor <- list()
        cor$dy <- list(
            "vpd" = readRDS("cor_daily_vpd.rds"),
            "vphpa" = readRDS("cor_daily_vph_am.rds"),
            "vphpp" = readRDS("cor_daily_vph_pm.rds"),
            "vph" = readRDS("cor_daily_vph.rds"),
            "papd" = readRDS("cor_daily_papd.rds"),
            "tp" = readRDS("cor_daily_throughput.rds"),
            "aogd" = readRDS("cor_daily_aog.rds"),
            "prd" = readRDS("cor_daily_pr.rds"),
            "qsd" = readRDS("cor_daily_qsd.rds"),
            "sfd" = readRDS("cor_daily_sfp.rds"),
            "sfo" = readRDS("cor_daily_sfo.rds"),
            "du" = readRDS("cor_avg_daily_detector_uptime.rds"),
            "cu" = readRDS("cor_daily_comm_uptime.rds"),
            "pau" = readRDS("cor_daily_pa_uptime.rds")
        )
        cor$wk <- list(
            "vpd" = readRDS("cor_weekly_vpd.rds"),
            # "vph" = readRDS("cor_weekly_vph.rds"),
            "vphpa" = readRDS("cor_weekly_vph_am.rds"),
            "vphpp" = readRDS("cor_weekly_vph_pm.rds"),
            "papd" = readRDS("cor_weekly_papd.rds"),
            # "paph" = readRDS("cor_weekly_paph.rds"),
            "pd" = readRDS("cor_weekly_pd_by_day.rds"),
            "tp" = readRDS("cor_weekly_throughput.rds"),
            "aogd" = readRDS("cor_weekly_aog_by_day.rds"),
            "prd" = readRDS("cor_weekly_pr_by_day.rds"),
            "qsd" = readRDS("cor_wqs.rds"),
            "sfd" = readRDS("cor_wsf.rds"),
            "sfo" = readRDS("cor_wsfo.rds"),
            "du" = readRDS("cor_weekly_detector_uptime.rds"),
            "cu" = readRDS("cor_weekly_comm_uptime.rds"),
            "pau" = readRDS("cor_weekly_pa_uptime.rds"),
            "dl" = readRDS("cor_weekly_dl.rds")
        )
        cor$mo <- list(
            "vpd" = readRDS("cor_monthly_vpd.rds"),
            # "vph" = readRDS("cor_monthly_vph.rds"),
            "vphpa" = readRDS("cor_monthly_vph_am.rds"),
            "vphpp" = readRDS("cor_monthly_vph_pm.rds"),
            "papd" = readRDS("cor_monthly_papd.rds"),
            # "paph" = readRDS("cor_monthly_paph.rds"),
            "pd" = readRDS("cor_monthly_pd_by_day.rds"),
            "tp" = readRDS("cor_monthly_throughput.rds"),
            "aogd" = readRDS("cor_monthly_aog_by_day.rds"),
            "aogh" = readRDS("cor_monthly_aog_by_hr.rds"),
            "prd" = readRDS("cor_monthly_pr_by_day.rds"),
            "prh" = readRDS("cor_monthly_pr_by_hr.rds"),
            "qsd" = readRDS("cor_monthly_qsd.rds"),
            "qsh" = readRDS("cor_mqsh.rds"),
            "sfd" = readRDS("cor_monthly_sfd.rds"),
            "sfh" = readRDS("cor_msfh.rds"),
            "sfo" = readRDS("cor_monthly_sfo.rds"),
            "tti" = readRDS("cor_monthly_tti.rds"),
            "ttih" = readRDS("cor_monthly_tti_by_hr.rds"),
            "pti" = readRDS("cor_monthly_pti.rds"),
            "ptih" = readRDS("cor_monthly_pti_by_hr.rds"),
            "bi" = readRDS("cor_monthly_bi.rds"),
            "bih" = readRDS("cor_monthly_bi_by_hr.rds"),
            "spd" = readRDS("cor_monthly_spd.rds"),
            "spdh" = readRDS("cor_monthly_spd_by_hr.rds"),
            "du" = readRDS("cor_monthly_detector_uptime.rds"),
            "cu" = readRDS("cor_monthly_comm_uptime.rds"),
            "pau" = readRDS("cor_monthly_pa_uptime.rds"),
            "dl" = readRDS("cor_monthly_dl.rds")
        )
        cor$qu <- list(
            "vpd" = get_quarterly(cor$mo$vpd, "vpd"),
            # "vph" = data.frame(), # get_quarterly(cor$mo$vph, "vph"),
            "vphpa" = get_quarterly(cor$mo$vphpa, "vph"),
            "vphpp" = get_quarterly(cor$mo$vphpp, "vph"),
            "papd" = get_quarterly(cor$mo$papd, "papd"),
            "pd" = get_quarterly(cor$mo$pd, "pd"),
            "tp" = get_quarterly(cor$mo$tp, "vph"),
            "aogd" = get_quarterly(cor$mo$aogd, "aog", "vol"),
            "prd" = get_quarterly(cor$mo$prd, "pr", "vol"),
            "qsd" = get_quarterly(cor$mo$qsd, "qs_freq"),
            "sfd" = get_quarterly(cor$mo$sfd, "sf_freq"),
            "sfo" = get_quarterly(cor$mo$sfo, "sf_freq"),
            "tti" = get_quarterly(cor$mo$tti, "tti"),
            "pti" = get_quarterly(cor$mo$pti, "pti"),
            "bi" = get_quarterly(cor$mo$bi, "bi"),
            "du" = get_quarterly(cor$mo$du, "uptime"),
            "cu" = get_quarterly(cor$mo$cu, "uptime"),
            "pau" = get_quarterly(cor$mo$pau, "uptime"),
            "dl" = get_quarterly(cor$mo$dl, "Level")
        )

        # cor$summary_data <- get_corridor_summary_data(cor)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)


tryCatch(
    {
        sub <- list()
        sub$dy <- list(
            "vpd" = readRDS("sub_daily_vpd.rds"),
            "vphpa" = readRDS("sub_daily_vph_am.rds"),
            "vphpp" = readRDS("sub_daily_vph_pm.rds"),
            "vph" = readRDS("sub_daily_vph.rds"),
            "papd" = readRDS("sub_daily_papd.rds"),
            "tp" = readRDS("sub_daily_throughput.rds"),
            "aogd" = readRDS("sub_daily_aog.rds"),
            "prd" = readRDS("sub_daily_pr.rds"),
            "qsd" = readRDS("sub_daily_qsd.rds"),
            "sfd" = readRDS("sub_daily_sfp.rds"),
            "sfo" = readRDS("sub_daily_sfo.rds"),
            "du" = readRDS("sub_avg_daily_detector_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime.sb, uptime.pr, uptime),
            "cu" = readRDS("sub_daily_comm_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime),
            "pau" = readRDS("sub_daily_pa_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime)
        )
        sub$wk <- list(
            "vpd" = readRDS("sub_weekly_vpd.rds") %>%
                select(Zone_Group, Corridor, Date, vpd),
            "vphpa" = readRDS("sub_weekly_vph_am.rds") %>%
                select(Zone_Group, Corridor, Date, vph),
            "vphpp" = readRDS("sub_weekly_vph_pm.rds") %>%
                select(Zone_Group, Corridor, Date, vph),
            "papd" = readRDS("sub_weekly_papd.rds") %>%
                select(Zone_Group, Corridor, Date, papd),
            # "paph" = readRDS("sub_weekly_paph.rds"),
            "pd" = readRDS("sub_weekly_pd_by_day.rds") %>%
                select(Zone_Group, Corridor, Date, pd),
            "tp" = readRDS("sub_weekly_throughput.rds") %>%
                select(Zone_Group, Corridor, Date, vph),
            "aogd" = readRDS("sub_weekly_aog_by_day.rds") %>%
                select(Zone_Group, Corridor, Date, aog),
            "prd" = readRDS("sub_weekly_pr_by_day.rds") %>%
                select(Zone_Group, Corridor, Date, pr),
            "qsd" = readRDS("sub_wqs.rds") %>%
                select(Zone_Group, Corridor, Date, qs_freq),
            "sfd" = readRDS("sub_wsf.rds") %>%
                select(Zone_Group, Corridor, Date, sf_freq),
            "sfo" = readRDS("sub_wsfo.rds") %>%
                select(Zone_Group, Corridor, Date, sf_freq),
            "du" = readRDS("sub_weekly_detector_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime),
            "cu" = readRDS("sub_weekly_comm_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime),
            "pau" = readRDS("sub_weekly_pa_uptime.rds") %>%
                select(Zone_Group, Corridor, Date, uptime),
            "dl" = readRDS("sub_weekly_dl.rds") %>%
                select(Zone_Group, Corridor, Date, Level)
        )
        sub$mo <- list(
            "vpd" = readRDS("sub_monthly_vpd.rds"),
            # "vph" = readRDS("sub_monthly_vph.rds"),
            "vphpa" = readRDS("sub_monthly_vph_am.rds"),
            "vphpp" = readRDS("sub_monthly_vph_pm.rds"),
            "papd" = readRDS("sub_monthly_papd.rds"),
            # "paph" = readRDS("sub_monthly_paph.rds"),
            "pd" = readRDS("sub_monthly_pd_by_day.rds"),
            "tp" = readRDS("sub_monthly_throughput.rds"),
            "aogd" = readRDS("sub_monthly_aog_by_day.rds"),
            "aogh" = readRDS("sub_monthly_aog_by_hr.rds"),
            "prd" = readRDS("sub_monthly_pr_by_day.rds"),
            "prh" = readRDS("sub_monthly_pr_by_hr.rds"),
            "qsd" = readRDS("sub_monthly_qsd.rds"),
            "qsh" = readRDS("sub_mqsh.rds"),
            "sfd" = readRDS("sub_monthly_sfd.rds"),
            "sfo" = readRDS("sub_monthly_sfo.rds"),
            "sfh" = readRDS("sub_msfh.rds"),
            "tti" = readRDS("sub_monthly_tti.rds"),
            "ttih" = readRDS("sub_monthly_tti_by_hr.rds"),
            "pti" = readRDS("sub_monthly_pti.rds"),
            "ptih" = readRDS("sub_monthly_pti_by_hr.rds"),
            "bi" = readRDS("sub_monthly_bi.rds"),
            "bih" = readRDS("sub_monthly_bi_by_hr.rds"),
            "du" = readRDS("sub_monthly_detector_uptime.rds"),
            "cu" = readRDS("sub_monthly_comm_uptime.rds"),
            "pau" = readRDS("sub_monthly_pa_uptime.rds"),
            "dl" = readRDS("sub_monthly_dl.rds")
        )
        sub$qu <- list(
            "vpd" = get_quarterly(sub$mo$vpd, "vpd"),
            # "vph" = get_quarterly(sub$mo$vph, "vph"),
            "vphpa" = get_quarterly(sub$mo$vphpa, "vph"),
            "vphpp" = get_quarterly(sub$mo$vphpp, "vph"),
            "tp" = get_quarterly(sub$mo$tp, "vph"),
            "aogd" = get_quarterly(sub$mo$aogd, "aog", "vol"),
            "prd" = get_quarterly(sub$mo$prd, "pr", "vol"),
            "qsd" = get_quarterly(sub$mo$qsd, "qs_freq"),
            "sfd" = get_quarterly(sub$mo$sfd, "sf_freq"),
            "sfo" = get_quarterly(sub$mo$sfo, "sf_freq"),
            "du" = get_quarterly(sub$mo$du, "uptime"),
            #"cu" = get_quarterly(sub$mo$cu, "uptime"),
            "pau" = get_quarterly(sub$mo$pau, "uptime"),
            "dl" = get_quarterly(sub$mo$dl, "Level")
        )
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



tryCatch(
    {
        sig <- list()
        sig$dy <- list(
            "vpd" = sigify(readRDS("daily_vpd.rds"), cor$dy$vpd, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, vpd, delta),
            "vphpa" = sigify(readRDS("daily_vph_am.rds"), cor$dy$vphpa, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, vph, delta),
            "vphpp" = sigify(readRDS("daily_vph_pm.rds"), cor$dy$vphpp, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, vph, delta),
            "papd" = sigify(readRDS("daily_papd.rds"), cor$dy$papd, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, papd, delta),
            "tp" = sigify(readRDS("daily_throughput.rds"), cor$dy$tp, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, vph, delta),
            "aogd" = sigify(readRDS("daily_aog.rds"), cor$dy$aogd, corridors) %>%
              select(Zone_Group, Corridor, Description, Date, aog, delta),
            "prd" = sigify(readRDS("daily_pr.rds"), cor$dy$prd, corridors) %>%
              select(Zone_Group, Corridor, Description, Date, pr, delta),
            "qsd" = sigify(readRDS("daily_qsd.rds"), cor$dy$qsd, corridors) %>%
              select(Zone_Group, Corridor, Description, Date, qs_freq, delta),
            "sfd" = sigify(readRDS("daily_sfp.rds"), cor$dy$sfd, corridors) %>%
              select(Zone_Group, Corridor, Description, Date, sf_freq, delta),
            "sfo" = sigify(readRDS("daily_sfo.rds"), cor$dy$sfo, corridors) %>%
              select(Zone_Group, Corridor, Description, Date, sf_freq, delta),
            "du" = sigify(readRDS("avg_daily_detector_uptime.rds"), cor$dy$du, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, uptime, uptime.sb, uptime.pr),
            "cu" = sigify(readRDS("daily_comm_uptime.rds"), cor$dy$cu, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, uptime),
            "pau" = sigify(readRDS("daily_pa_uptime.rds"), cor$dy$pau, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, uptime)
        )
        sig$wk <- list(
            "vpd" = sigify(readRDS("weekly_vpd.rds"), cor$wk$vpd, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, vpd),
            "vphpa" = sigify(readRDS("weekly_vph_am.rds"), cor$wk$vphpa, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, vph),
            "vphpp" = sigify(readRDS("weekly_vph_pm.rds"), cor$wk$vphpp, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, vph),
            "papd" = sigify(readRDS("weekly_papd.rds"), cor$wk$papd, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, papd),
            # "paph" = sigify(readRDS("weekly_paph.rds"), cor$wk$paph, corridors),
            "pd" = sigify(readRDS("weekly_pd_by_day.rds"), cor$wk$pd, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, pd),
            "tp" = sigify(readRDS("weekly_throughput.rds"), cor$wk$tp, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, vph),
            "aogd" = sigify(readRDS("weekly_aog_by_day.rds"), cor$wk$aogd, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, aog),
            "prd" = sigify(readRDS("weekly_pr_by_day.rds"), cor$wk$prd, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, pr),
            "qsd" = sigify(readRDS("wqs.rds"), cor$wk$qsd, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, qs_freq),
            "sfd" = sigify(readRDS("wsf.rds"), cor$wk$sfd, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, sf_freq),
            "sfo" = sigify(readRDS("wsfo.rds"), cor$wk$sfo, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, sf_freq),
            "du" = sigify(readRDS("weekly_detector_uptime.rds"), cor$wk$du, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, uptime),
            "cu" = sigify(readRDS("weekly_comm_uptime.rds"), cor$wk$cu, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, uptime),
            "pau" = sigify(readRDS("weekly_pa_uptime.rds"), cor$wk$pau, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, uptime),
            "dl" = sigify(readRDS("weekly_dl.rds"), cor$wk$dl, corridors) %>%
                select(Zone_Group, Corridor, Description, Date, Level)
        )
        sig$mo <- list(
            "vpd" = sigify(readRDS("monthly_vpd.rds"), cor$mo$vpd, corridors) %>%
                select(-c(Name, ones)),
            "vphpa" = sigify(readRDS("monthly_vph_am.rds"), cor$mo$vphpa, corridors) %>%
                select(-c(Name, ones)),
            "vphpp" = sigify(readRDS("monthly_vph_pm.rds"), cor$mo$vphpp, corridors) %>%
                select(-c(Name, ones)),
            "papd" = sigify(readRDS("monthly_papd.rds"), cor$mo$papd, corridors) %>%
                select(-c(Name, ones)),
            # "paph" = sigify(readRDS("monthly_paph.rds"), cor$mo$paph, corridors) %>%
            #    select(-c(Name, ones)),
            "pd" = sigify(readRDS("monthly_pd_by_day.rds"), cor$mo$pd, corridors) %>%
                select(-c(Name, Events)),
            "tp" = sigify(readRDS("monthly_throughput.rds"), cor$mo$tp, corridors) %>%
                select(-c(Name, ones)),
            "aogd" = sigify(readRDS("monthly_aog_by_day.rds"), cor$mo$aogd, corridors) %>%
                select(-c(Name, vol)),
            "aogh" = sigify(readRDS("monthly_aog_by_hr.rds"), cor$mo$aogh, corridors) %>%
                select(-c(Name, vol)),
            "prd" = sigify(readRDS("monthly_pr_by_day.rds"), cor$mo$prd, corridors) %>%
                select(-c(Name, vol)),
            "prh" = sigify(readRDS("monthly_pr_by_hr.rds"), cor$mo$prh, corridors) %>%
                select(-c(Name, vol)),
            "qsd" = sigify(readRDS("monthly_qsd.rds"), cor$mo$qsd, corridors) %>%
                select(-c(Name, cycles)),
            "qsh" = sigify(readRDS("mqsh.rds"), cor$mo$qsh, corridors) %>%
                select(-c(Name, cycles)),
            "sfd" = sigify(readRDS("monthly_sfd.rds"), cor$mo$sfd, corridors) %>%
                select(-c(Name, cycles)),
            "sfh" = sigify(readRDS("msfh.rds"), cor$mo$sfh, corridors) %>%
                select(-c(Name, cycles)),
            "sfo" = sigify(readRDS("monthly_sfo.rds"), cor$mo$sfo, corridors) %>%
                select(-c(Name, cycles)),
            "tti" = data.frame(),
            "pti" = data.frame(),
            "bi" = data.frame(),
            "spd" = data.frame(),
            "du" = sigify(readRDS("monthly_detector_uptime.rds"), cor$mo$du, corridors) %>%
                select(Zone_Group, Corridor, Month, uptime, uptime.sb, uptime.pr, delta),
            "cu" = sigify(readRDS("monthly_comm_uptime.rds"), cor$mo$cu, corridors) %>%
                select(Zone_Group, Corridor, Month, uptime, delta),
            "pau" = sigify(readRDS("monthly_pa_uptime.rds"), cor$mo$pau, corridors) %>%
                select(Zone_Group, Corridor, Month, uptime, delta),
            "dl" = sigify(readRDS("monthly_dl.rds"), cor$mo$dl, corridors) %>%
                select(Zone_Group, Corridor, Month, Level, delta)
        )
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)






qsave(cor, "cor.qs")
qsave(sig, "sig.qs")
qsave(sub, "sub.qs")


print(glue("{Sys.time()} Write to Database [25 of 25]"))

aurora <- keep_trying(get_aurora_connection, n_tries = 5)

# Update Aurora Nightly
# recreate_database(conn, cor, "cor")
# recreate_database(conn, sub, "sub")
# recreate_database(conn, sub, "sub")

# append_to_database(
#    conn, cor, sub, sig,
#    calcs_start_date = report_start_date,
#    report_start_date = report_start_date)

append_to_database(
    aurora, cor, "cor",
    calcs_start_date, report_start_date, report_end_date = NULL)
append_to_database(
    aurora, sub, "sub",
    calcs_start_date, report_start_date, report_end_date = NULL)
append_to_database(
    aurora, sig, "sig",
    calcs_start_date, report_start_date, report_end_date = NULL)

dbDisconnect(aurora)
