
# Monthly_Report_Package.R -- For 15-min Data

source("Monthly_Report_Package_init.R")

# For 15min counts (no monthly or weekly), go one extra day base MR_calcs, maximum of 7 days
# to limit the amount of data to process and upload.
calcs_start_date <- today(tzone = conf$timezone) - days(7)
calcs_start_date <- max(calcs_start_date, as_date(get_date_from_string(conf$start_date)) - days(1))

# Need to keep some data in rds prior to the calcs_start_date to calculate accurate deltas
rds_start_date <- calcs_start_date - days(1)


# 15 MINUTE PEDESTRIAN ACTIVATIONS ###############################################

print(glue("{Sys.time()} 15-Minute Pedestrian Activations [1 of 9(4)]"))

tryCatch(
    {
        paph <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "counts_ped_15min",
            start_date = rds_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf,
            parallel = FALSE
        ) %>%
            filter(!is.na(CallPhase)) %>%   # Added 1/14/20 to perhaps exclude non-programmed ped pushbuttons
            mutate(
                SignalID = factor(SignalID),
                Detector = factor(Detector),
                CallPhase = factor(CallPhase),
                Date = date(Date),
                DOW = wday(Date),
                Week = week(Date),
                vol = as.numeric(vol)
            )

        bad_ped_detectors <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "bad_ped_detectors",
            start_date = rds_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf,
            parallel = FALSE
        )

        if (nrow(bad_ped_detectors) > 0) {
            bad_ped_detectors <- bad_ped_detectors %>%
                mutate(
                    SignalID = factor(SignalID),
                    Detector = factor(Detector))
            # Filter out bad days
            paph <- paph %>%
                select(SignalID, Timeperiod, CallPhase, Detector, vol) %>%
                anti_join(bad_ped_detectors)
        } else {
            paph <- paph %>%
                select(SignalID, Timeperiod, CallPhase, Detector, vol)
        }

        pa_15min <- get_period_sum(paph, "vol", "Timeperiod")
        cor_15min_pa <- get_cor_monthly_avg_by_period(pa_15min, corridors, "vol", "Timeperiod")
        sub_15min_pa <- get_cor_monthly_avg_by_period(pa_15min, subcorridors, "vol", "Timeperiod")

        pa_15min <- sigify(pa_15min, cor_15min_pa, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, vol, delta)


        addtoRDS(
            pa_15min, "pa_15min.rds", "vol", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_15min_pa, "cor_15min_pa.rds", "vol", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_15min_pa, "sub_15min_pa.rds", "vol", rds_start_date, calcs_start_date
        )

        rm(paph)
        rm(bad_ped_detectors)
        rm(pa_15min)
        rm(cor_15min_pa)
        rm(sub_15min_pa)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# 15 MINUTE VOLUMES ##############################################################

print(glue("{Sys.time()} 15-Minute Volumes [2 of 9(4)]"))

tryCatch(
    {
        vph <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "vehicles_15min",
            start_date = rds_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(2), # Hack because next function needs a CallPhase
                Date = date(Date)
            ) %>%
            rename(vol = vph)

        vol_15min <- get_period_sum(vph, "vol", "Timeperiod")
        cor_15min_vol <- get_cor_monthly_avg_by_period(vol_15min, corridors, "vol", "Timeperiod")
        sub_15min_vol <- get_cor_monthly_avg_by_period(vol_15min, subcorridors, "vol", "Timeperiod")

        vol_15min <- sigify(vol_15min, cor_15min_vol, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, vol, delta)

        addtoRDS(
            vol_15min, "vol_15min.rds", "vol", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_15min_vol, "cor_15min_vol.rds", "vol", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_15min_vol, "sub_15min_vol.rds", "vol", rds_start_date, calcs_start_date
        )

        rm(vph)
        rm(vol_15min)
        rm(cor_15min_vol)
        rm(sub_15min_vol)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# 15 MINUTE ARRIVALS ON GREEN ####################################################

print(glue("{Sys.time()} 15-Minute AOG [3 of 9(4)]"))

tryCatch(
    {
        aog <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "arrivals_on_green_15min",
            start_date = rds_start_date,
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

        # Don't fill in gaps (leave as NA)
        # since no volume means no value for aog or pr (it's not 0)
        aog <- aog %>%
            rename(Timeperiod = Date_Period) %>%
            select(SignalID, CallPhase, Timeperiod, aog, pr, vol, Date)

        aog_15min <- aog %>%
            # Don't fill in gaps (leave as NA)
            # since no volume means no value for aog or pr (it's not 0)
            complete(
                nesting(SignalID, Date),
                Timeperiod = seq(as_datetime(rds_start_date), as_datetime(report_end_date) - minutes(15), by = "15 min")
            ) %>%
            get_period_avg("aog", "Timeperiod", "vol")

        cor_15min_aog <- get_cor_monthly_avg_by_period(aog_15min, corridors, "aog", "Timeperiod")
        sub_15min_aog <- get_cor_monthly_avg_by_period(aog_15min, subcorridors, "aog", "Timeperiod")

        aog_15min <- sigify(aog_15min, cor_15min_aog, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, aog, delta)


        addtoRDS(
            aog_15min, "aog_15min.rds", "aog", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_15min_aog, "cor_15min_aog.rds", "aog", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_15min_aog, "sub_15min_aog.rds", "aog", rds_start_date, calcs_start_date
        )

        rm(aog_15min)
        rm(cor_15min_aog)
        rm(sub_15min_aog)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# 15 MINUTE PROGESSION RATIO #####################################################

print(glue("{Sys.time()} 15-Minute Progression Ratio [4 of 9(4)]"))

tryCatch(
    {
        pr_15min <- aog %>%
            # Don't fill in gaps (leave as NA)
            # since no volume means no value for aog or pr (it's not 0)
            complete(
                nesting(SignalID, Date),
                Timeperiod = seq(as_datetime(rds_start_date), as_datetime(report_end_date) - minutes(15), by = "15 min")
            ) %>%
            get_period_avg("pr", "Timeperiod", "vol")

        cor_15min_pr <- get_cor_monthly_avg_by_period(pr_15min, corridors, "pr", "Timeperiod")
        sub_15min_pr <- get_cor_monthly_avg_by_period(pr_15min, subcorridors, "pr", "Timeperiod")

        pr_15min <- sigify(pr_15min, cor_15min_pr, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, pr, delta)


        addtoRDS(
            pr_15min, "pr_15min.rds", "pr", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_15min_pr, "cor_15min_pr.rds", "pr", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_15min_pr, "sub_15min_pr.rds", "pr", rds_start_date, calcs_start_date
        )

        rm(aog)
        rm(pr_15min)
        rm(cor_15min_pr)
        rm(sub_15min_pr)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# 15 MINUTE SPLIT FAILURES #######################################################

print(glue("{Sys.time()} 15-Minute Split Failures [5 of 9(4)]"))

tryCatch(
    {
        sf <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "split_failures_15min",
            start_date = rds_start_date,
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

        sf <- sf %>%
            rename(Timeperiod = Date_Hour) %>%
            select(SignalID, CallPhase, Timeperiod, sf_freq, Date)

        sf_15min <- sf %>%
            complete(
                nesting(SignalID, Date, CallPhase),
                Timeperiod = seq(as_datetime(rds_start_date), as_datetime(report_end_date) - minutes(15), by = "15 min"),
                fill = list(sf_freq = 0)
            ) %>%
            get_period_avg("sf_freq", "Timeperiod")

        cor_15min_sf <- get_cor_monthly_avg_by_period(sf_15min, corridors, "sf_freq", "Timeperiod")
        sub_15min_sf <- get_cor_monthly_avg_by_period(sf_15min, subcorridors, "sf_freq", "Timeperiod")

        sf_15min <- sigify(sf_15min, cor_15min_sf, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, sf_freq, delta)


        addtoRDS(
            sf_15min, "sf_15min.rds", "sf_freq", rds_start_date, rds_start_date
        )
        addtoRDS(
            cor_15min_sf, "cor_15min_sf.rds", "sf_freq", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_15min_sf, "sub_15min_sf.rds", "sf_freq", rds_start_date, calcs_start_date
        )

        rm(sf)
        rm(sf_15min)
        rm(cor_15min_sf)
        rm(sub_15min_sf)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# 15 MINUTE QUEUE SPILLBACK ######################################################

print(glue("{Sys.time()} 15-Minute Queue Spillback [6 of 9(4)]"))

tryCatch(
    {
        qs <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "queue_spillback_15min",
            start_date = rds_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date))

        qs <- qs %>%
            rename(Timeperiod = Date_Hour) %>%
            select(SignalID, CallPhase, Timeperiod, qs_freq, Date)

        # TODO: Not sure this "complete" logic is correct. Revisit.
        # It may fill in Hours that don't correspond to the date field.
        qs_15min <- qs %>%
            complete(
                nesting(SignalID, Date, CallPhase),
                Timeperiod = seq(as_datetime(rds_start_date), as_datetime(report_end_date) - minutes(15), by = "15 min"),
                fill = list(qs_freq = 0)
            ) %>%
            filter(Date == as_date(Hour)) %>%  # This is new. Need to test.
            get_period_avg("qs_freq", "Timeperiod")

        cor_15min_qs <- get_cor_monthly_avg_by_period(qs_15min, corridors, "qs_freq", "Timeperiod")
        sub_15min_qs <- get_cor_monthly_avg_by_period(qs_15min, subcorridors, "qs_freq", "Timeperiod")

        qs_15min <- sigify(qs_15min, cor_15min_qs, corridors) %>%
                select(Zone_Group, Corridor, Timeperiod, qs_freq, delta)


        addtoRDS(
            qs_15min, "qs_15min.rds", "qs_freq", rds_start_date, rds_start_date
        )
        addtoRDS(
            cor_15min_qs, "cor_15min_qs.rds", "qs_freq", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_15min_qs, "sub_15min_qs.rds", "qs_freq", rds_start_date, calcs_start_date
        )

        rm(qs)
        rm(qs_15min)
        rm(cor_15min_qs)
        rm(sub_15min_qs)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# 15 MINUTE APPROACH DELAY ######################################################################

print(glue("{Sys.time()} Approach Delay [7 of 9(4)]"))

tryCatch(
    {
        rm(daily)
        rm(df_15min)
        rm(cor_15min)
        rm(sub_15min)

        metric <- approach_delay

        daily <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "approach_delay_15min",
            start_date = rds_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(0),
                Date = date(Date)
            )

        df_15min <- daily %>%
            rename(Timeperiod = Hour) %>%
            get_period_avg(metric$variable, "Timeperiod", metric$weight)

        cor_15min <- get_cor_monthly_avg_by_period(
            df_15min, corridors, metric$variable, "Timeperiod", metric$weight)
        sub_15min <- get_cor_monthly_avg_by_period(
            df_15min, subcorridors, metric$variable, "Timeperiod", metric$weight)

        df_15min <- sigify(df_15min, cor_15min, corridors) %>%
                select(all_of(c("Zone_Group", "Corridor", "Timeperiod", metric$variable, "delta")))

        addtoRDS(
            df_15min, glue("hourly_{metric$table}.rds"), metric$variable, rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_15min, glue("cor_15min_{metric$table}.rds"), metric$variable, rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_15min, glue("sub_15min_{metric$table}.rds"), metric$variable, rds_start_date, calcs_start_date
        )

        rm(daily)
        rm(df_15min)
        rm(cor_15min)
        rm(sub_15min)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)


# Package up for Flexdashboard

print(glue("{Sys.time()} Package for Monthly Report [8 of 9(4)]"))

tryCatch(
    {
        cor2 <- list()
        cor2$qhr <- list(
            "vph" = readRDS("cor_15min_vol.rds"),
            "paph" = readRDS("cor_15min_pa.rds"),
            "aogh" = readRDS("cor_15min_aog.rds"),
            "prh" = readRDS("cor_15min_pr.rds"),
            "sfh" = readRDS("cor_15min_sf.rds"),
            "qsh" = readRDS("cor_15min_qs.rds"),
            "adh" = readRDS("cor_15min_ad.rds")
        )
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)


tryCatch(
    {
        sub2 <- list()
        sub2$qhr <- list(
            "vph" = readRDS("sub_15min_vol.rds"),
            "paph" = readRDS("sub_15min_pa.rds"),
            "aogh" = readRDS("sub_15min_aog.rds"),
            "prh" = readRDS("sub_15min_pr.rds"),
            "sfh" = readRDS("sub_15min_sf.rds"),
            "qsh" = readRDS("sub_15min_qs.rds"),
            "adh" = readRDS("sub_15min_ad.rds")
        )
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)



tryCatch(
    {
        sig2 <- list()
        sig2$qhr <- list(
            "vph" = readRDS("vol_15min.rds"),
            "paph" = readRDS("pa_15min.rds"),
            "aogh" = readRDS("aog_15min.rds"),
            "prh" = readRDS("pr_15min.rds"),
            "sfh" = readRDS("sf_15min.rds"),
            "qsh" = readRDS("qs_15min.rds"),
            "adh" = readRDS("ad_15min.rds")
        )
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)





print(glue("{Sys.time()} Write to Database [9 of 9(4)]"))

source("write_sigops_to_db.R")

# Update Aurora Nightly
aurora <- keep_trying(func = get_aurora_connection, n_tries = 5)
# recreate_database(conn)

append_to_database(
    aurora, cor2, "cor", calcs_start_date, report_start_date = report_start_date, report_end_date = NULL)
append_to_database(
    aurora, sub2, "sub", calcs_start_date, report_start_date = report_start_date, report_end_date = NULL)
append_to_database(
    aurora, sig2, "sig", calcs_start_date, report_start_date = report_start_date, report_end_date = NULL)

dbDisconnect(aurora)
