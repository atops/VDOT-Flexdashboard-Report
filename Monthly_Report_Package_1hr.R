
# Monthly_Report_Package.R -- For Hourly Data

source("Monthly_Report_Package_init.R")

# For hourly counts (no monthly or weekly), go one extra day base MR_calcs, maximum of 7 days
# to limit the amount of data to process and upload.
calcs_start_date <- today(tzone = conf$timezone) - days(7)
calcs_start_date <- max(calcs_start_date, as_date(get_date_from_string(conf$start_date)) - days(1))

# Need to keep some data in rds prior to the calcs_start_date to calculate accurate deltas
rds_start_date <- calcs_start_date - days(1)


# HOURLY PEDESTRIAN ACTIVATIONS ###############################################

print(glue("{Sys.time()} Hourly Pedestrian Activations [1 of 9(3)]"))

tryCatch(
    {
        paph <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "counts_ped_1hr",
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
                rename(Hour = Timeperiod, paph = vol) %>%
                select(SignalID, Hour, CallPhase, Detector, paph) %>%
                anti_join(bad_ped_detectors)
        } else {
            paph <- paph %>%
                rename(Hour = Timeperiod, paph = vol) %>%
                select(SignalID, Hour, CallPhase, Detector, paph)
        }

        hourly_pa <- get_period_sum(paph, "paph", "Hour")
        cor_hourly_pa <- get_cor_monthly_avg_by_period(hourly_pa, corridors, "paph", "Hour")
        sub_hourly_pa <- get_cor_monthly_avg_by_period(hourly_pa, subcorridors, "paph", "Hour")

        hourly_pa <- sigify(hourly_pa, cor_hourly_pa, corridors) %>%
                select(Zone_Group, Corridor, Hour, paph, delta)


        addtoRDS(
            hourly_pa, "hourly_pa.rds", "paph", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_hourly_pa, "cor_hourly_pa.rds", "paph", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_hourly_pa, "sub_hourly_pa.rds", "paph", rds_start_date, calcs_start_date
        )

        rm(paph)
        rm(bad_ped_detectors)
        rm(hourly_pa)
        rm(cor_hourly_pa)
        rm(sub_hourly_pa)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# HOURLY VOLUMES ##############################################################

print(glue("{Sys.time()} Hourly Volumes [2 of 9(3)]"))

tryCatch(
    {
        vph <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "vehicles_ph",
            start_date = rds_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(2), # Hack because next function needs a CallPhase
                Date = date(Date)
            )

        hourly_vol <- get_period_sum(vph, "vph", "Hour")
        cor_hourly_vol <- get_cor_monthly_avg_by_period(hourly_vol, corridors, "vph", "Hour")
        sub_hourly_vol <- get_cor_monthly_avg_by_period(hourly_vol, subcorridors, "vph", "Hour")

        hourly_vol <- sigify(hourly_vol, cor_hourly_vol, corridors) %>%
                select(Zone_Group, Corridor, Hour, vph, delta)


        addtoRDS(
            hourly_vol, "hourly_vol.rds", "vph", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_hourly_vol, "cor_hourly_vol.rds", "vph", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_hourly_vol, "sub_hourly_vol.rds", "vph", rds_start_date, calcs_start_date
        )

        rm(vph)
        rm(hourly_vol)
        rm(cor_hourly_vol)
        rm(sub_hourly_vol)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# HOURLY ARRIVALS ON GREEN ####################################################

print(glue("{Sys.time()} Hourly AOG [3 of 9(3)]"))

tryCatch(
    {
        aog <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "arrivals_on_green",
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
            rename(Hour = Date_Hour) %>%
            select(SignalID, CallPhase, Hour, aog, pr, vol, Date)

        hourly_aog <- aog %>%
            # Don't fill in gaps (leave as NA)
            # since no volume means no value for aog or pr (it's not 0)
            complete(
                nesting(SignalID, Date),
                Hour = seq(as_datetime(rds_start_date), as_datetime(report_end_date) - hours(1), by = "1 hour")
            ) %>%
            get_period_avg("aog", "Hour", "vol")
        cor_hourly_aog <- get_cor_monthly_avg_by_period(hourly_aog, corridors, "aog", "Hour")
        sub_hourly_aog <- get_cor_monthly_avg_by_period(hourly_aog, subcorridors, "aog", "Hour")

        hourly_aog <- sigify(hourly_aog, cor_hourly_aog, corridors) %>%
                select(Zone_Group, Corridor, Hour, aog, delta)


        addtoRDS(
            hourly_aog, "hourly_aog.rds", "aog", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_hourly_aog, "cor_hourly_aog.rds", "aog", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_hourly_aog, "sub_hourly_aog.rds", "aog", rds_start_date, calcs_start_date
        )

        rm(hourly_aog)
        rm(cor_hourly_aog)
        rm(sub_hourly_aog)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# HOURLY PROGESSION RATIO #####################################################

print(glue("{Sys.time()} Hourly Progression Ratio [4 of 9(3)]"))

tryCatch(
    {
        hourly_pr <- aog %>%
            # Don't fill in gaps (leave as NA)
            # since no volume means no value for aog or pr (it's not 0)
            complete(
                nesting(SignalID, Date),
                Hour = seq(as_datetime(rds_start_date), as_datetime(report_end_date) - hours(1), by = "1 hour")
            ) %>%
            get_period_avg("pr", "Hour", "vol")

        cor_hourly_pr <- get_cor_monthly_avg_by_period(hourly_pr, corridors, "pr", "Hour")
        sub_hourly_pr <- get_cor_monthly_avg_by_period(hourly_pr, subcorridors, "pr", "Hour")

        hourly_pr <- sigify(hourly_pr, cor_hourly_pr, corridors) %>%
                select(Zone_Group, Corridor, Hour, pr, delta)


        addtoRDS(
            hourly_pr, "hourly_pr.rds", "pr", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_hourly_pr, "cor_hourly_pr.rds", "pr", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_hourly_pr, "sub_hourly_pr.rds", "pr", rds_start_date, calcs_start_date
        )

        rm(aog)
        rm(hourly_pr)
        rm(cor_hourly_pr)
        rm(sub_hourly_pr)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# HOURLY SPLIT FAILURES #######################################################

print(glue("{Sys.time()} Hourly Split Failures [5 of 9(3)]"))

tryCatch(
    {
        sf <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "split_failures",
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
            rename(Hour = Date_Hour) %>%
            select(SignalID, CallPhase, Hour, sf_freq, Date)

        # TODO: Not sure this "complete" logic is correct. Revisit.
        # It may fill in Hours that don't correspond to the date field.
        hourly_sf <- sf %>%
            complete(
                nesting(SignalID, Date, CallPhase),
                Hour = seq(as_datetime(rds_start_date), as_datetime(report_end_date) - hours(1), by = "1 hour"),
                fill = list(sf_freq = 0)
            ) %>%
            filter(Date == as_date(Hour)) %>%  # This is new. Need to test.
            get_period_avg("sf_freq", "Hour")

        cor_hourly_sf <- get_cor_monthly_avg_by_period(hourly_sf, corridors, "sf_freq", "Hour")
        sub_hourly_sf <- get_cor_monthly_avg_by_period(hourly_sf, subcorridors, "sf_freq", "Hour")

        hourly_sf <- sigify(hourly_sf, cor_hourly_sf, corridors) %>%
                select(Zone_Group, Corridor, Hour, sf_freq, delta)


        addtoRDS(
            hourly_sf, "hourly_sf.rds", "sf_freq", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_hourly_sf, "cor_hourly_sf.rds", "sf_freq", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_hourly_sf, "sub_hourly_sf.rds", "sf_freq", rds_start_date, calcs_start_date
        )

        rm(sf)
        rm(hourly_sf)
        rm(cor_hourly_sf)
        rm(sub_hourly_sf)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# HOURLY QUEUE SPILLBACK ######################################################

print(glue("{Sys.time()} Hourly Queue Spillback [6 of 9(3)]"))

tryCatch(
    {
        qs <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = "queue_spillback",
            start_date = rds_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Date = date(Date)
            )

        qs <- qs %>%
            rename(Hour = Date_Hour) %>%
            select(SignalID, CallPhase, Hour, qs_freq, Date)

        hourly_qs <- qs %>%
            complete(
                nesting(SignalID, Date, CallPhase),
                Hour = seq(as_datetime(rds_start_date), as_datetime(report_end_date) - hours(1), by = "1 hour"),
                fill = list(qs_freq = 0)
            ) %>%
            get_period_avg("qs_freq", "Hour")

        cor_hourly_qs <- get_cor_monthly_avg_by_period(hourly_qs, corridors, "qs_freq", "Hour")
        sub_hourly_qs <- get_cor_monthly_avg_by_period(hourly_qs, subcorridors, "qs_freq", "Hour")

        hourly_qs <- sigify(hourly_qs, cor_hourly_qs, corridors) %>%
                select(Zone_Group, Corridor, Hour, qs_freq, delta)


        addtoRDS(
            hourly_qs, "hourly_qs.rds", "qs_freq", rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_hourly_qs, "cor_hourly_qs.rds", "qs_freq", rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_hourly_qs, "sub_hourly_qs.rds", "qs_freq", rds_start_date, calcs_start_date
        )

        rm(qs)
        rm(hourly_qs)
        rm(cor_hourly_qs)
        rm(sub_hourly_qs)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)

# HOURLY APPROACH DELAY ###############################################################

print(glue("{Sys.time()} Approach Delay [7 of 9(3)]"))

tryCatch(
    {
        ifrm(z)
        ifrm(hourly)
        ifrm(cor_hourly)
        ifrm(sub_hourly)

        metric <- approach_delay

        z <- s3_read_parquet_parallel(
            bucket = conf$bucket,
            table_name = metric$s3table,
            start_date = rds_start_date,
            end_date = report_end_date,
            signals_list = signals_list,
            conf = conf
        ) %>%
            mutate(
                SignalID = factor(SignalID),
                CallPhase = factor(0),
                Week = week(Date)
            )

        hourly <- z %>%
            get_period_avg(metric$variable, "Hour", metric$weight)

        cor_hourly <- get_cor_monthly_avg_by_period(
            hourly, corridors, metric$variable, "Hour", metric$weight)
        sub_hourly <- get_cor_monthly_avg_by_period(
            hourly, subcorridors, metric$variable, "Hour", metric$weight)

        hourly <- sigify(hourly, cor_hourly, corridors) %>%
                select(all_of(c("Zone_Group", "Corridor", "Hour", metric$variable, "delta")))

        addtoRDS(
            hourly, glue("hourly_{metric$table}.rds"), metric$variable, rds_start_date, calcs_start_date
        )
        addtoRDS(
            cor_hourly, glue("cor_hourly_{metric$table}.rds"), metric$variable, rds_start_date, calcs_start_date
        )
        addtoRDS(
            sub_hourly, glue("sub_hourly_{metric$table}.rds"), metric$variable, rds_start_date, calcs_start_date
        )

        ifrm(z)
        ifrm(hourly)
        ifrm(cor_hourly)
        ifrm(sub_hourly)
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)





# Package up for Flexdashboard

print(glue("{Sys.time()} Package for Monthly Report [8 of 9(3)]"))

# All of the hourly bins.

tryCatch(
    {
        cor2 <- list()
        cor2$hr <- list(
            "vph" = readRDS("cor_hourly_vol.rds"),
            "paph" = readRDS("cor_hourly_pa.rds"),
            "aogh" = readRDS("cor_hourly_aog.rds"),
            "prh" = readRDS("cor_hourly_pr.rds"),
            "sfh" = readRDS("cor_hourly_sf.rds"),
            "qsh" = readRDS("cor_hourly_qs.rds"),
            "adh" = readRDS("cor_hourly_ad.rds")
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
        sub2$hr <- list(
            "vph" = readRDS("sub_hourly_vol.rds"),
            "paph" = readRDS("sub_hourly_pa.rds"),
            "aogh" = readRDS("sub_hourly_aog.rds"),
            "prh" = readRDS("sub_hourly_pr.rds"),
            "sfh" = readRDS("sub_hourly_sf.rds"),
            "qsh" = readRDS("sub_hourly_qs.rds"),
            "adh" = readRDS("sub_hourly_ad.rds")
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
        sig2$hr <- list(
            "vph" = readRDS("hourly_vol.rds"),
            "paph" = readRDS("hourly_pa.rds"),
            "aogh" = readRDS("hourly_aog.rds"),
            "prh" = readRDS("hourly_pr.rds"),
            "sfh" = readRDS("hourly_sf.rds"),
            "qsh" = readRDS("hourly_qs.rds"),
            "adh" = readRDS("hourly_ad.rds")
        )
    },
    error = function(e) {
        print("ENCOUNTERED AN ERROR:")
        print(e)
    }
)





print(glue("{Sys.time()} Write to Database [9 of 9(3)]"))

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



