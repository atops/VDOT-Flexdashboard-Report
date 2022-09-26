
# Monthly_Report_Calcs.R

library(yaml)
library(glue)

source("Monthly_Report_Functions.R")


print(glue("{Sys.time()} Starting Calcs Script"))

if (interactive()) {
    plan(multisession)
} else {
    plan(multicore)
}
usable_cores <- get_usable_cores(4)
# usable_cores <- 1
doParallel::registerDoParallel(cores = usable_cores)


# Set credentials from ~/.aws/credentials file
aws.signature::use_credentials(profile = conf$profile)

# Need to set the default region as well. use_credentials doesn't do this.
credentials <- aws.signature::read_credentials()[[conf$profile]]
Sys.setenv(AWS_DEFAULT_REGION = conf$aws_region)


# aurora_pool <- get_aurora_connection_pool()
aurora <- get_aurora_connection()

#----- DEFINE DATE RANGE FOR CALCULATIONS ------------------------------------#

start_date <- get_date_from_string(conf$start_date)
end_date <- get_date_from_string(conf$end_date)

# Manual overrides
# start_date <- "2020-01-04"
# end_date <- "2020-01-04"

month_abbrs <- get_month_abbrs(start_date, end_date)
#-----------------------------------------------------------------------------#

# # GET CORRIDORS #############################################################

# -- Code to update corridors file/table from Excel file


xlsx_last_modified <- get_bucket(bucket = conf$bucket, prefix = conf$corridors_filename_s3)$Contents$LastModified
qs_filename <- sub("\\..*", ".qs", conf$corridors_filename_s3)
qs_last_modified <- get_bucket(bucket = conf$bucket, prefix = qs_filename)$Contents$LastModified

if (as_datetime(xlsx_last_modified) > as_datetime(qs_last_modified)) {
    corridors <- s3read_using(
        function(x) get_corridors(x, filter_signals = TRUE),
        object = conf$corridors_filename_s3,
        bucket = conf$bucket
    )
    qsave(corridors, qs_filename)
    aws.s3::put_object(
        file = qs_filename,
        object = qs_filename,
        bucket = conf$bucket,
        multipart = TRUE
    )
    dbExecute(aurora, "TRUNCATE TABLE Corridors")
    dbWriteTable(aurora, "Corridors", corridors, overwrite = FALSE, append = TRUE, row.names = FALSE)

    all_corridors <- s3read_using(
        function(x) get_corridors(x, filter_signals = FALSE),
        object = conf$corridors_filename_s3,
        bucket = conf$bucket
    )
    qs_all_filename <- sub("\\..*", ".qs", paste0("all_", conf$corridors_filename_s3))
    qsave(all_corridors, qs_filename)
    aws.s3::put_object(
        file = qs_all_filename,
        object = qs_all_filename,
        bucket = conf$bucket,
        multipart = TRUE
    )
    dbExecute(aurora, "TRUNCATE TABLE AllCorridors")
    dbWriteTable(aurora, "AllCorridors", all_corridors, overwrite = FALSE, append = TRUE, row.names = FALSE)
}

corridors <- dbReadTable(aurora, "Corridors")
dbDisconnect(aurora)


signals_list <- unique(corridors$SignalID)




print(Sys.time())

# # GET CAMERA UPTIMES ########################################################

print(glue("{Sys.time()} parse cctv logs [1 of 11]"))

if (conf$run$cctv == TRUE) {
    # Run python scripts asynchronously
    system("c:/users/ATSPM/miniconda3/python.exe parse_cctvlog.py", wait = FALSE)
    system("c:/users/ATSPM/miniconda3/python.exe parse_cctvlog_encoders.py", wait = FALSE)
    system("~/miniconda3/bin/conda run -n vdot-flexdashboard-report python parse_cctvlog.py", wait = FALSE)
    system("~/miniconda3/bin/conda run -n vdot-flexdashboard-report python parse_cctvlog_encoders.py", wait = FALSE)
}

# # GET RSU UPTIMES ###########################################################

print(glue("{Sys.time()} parse rsu logs [2 of 11]"))

# # TRAVEL TIMES FROM RITIS API ###############################################

print(glue("{Sys.time()} travel times [3 of 11]"))

if (conf$run$travel_times == TRUE) {
    # Run python script asynchronously
    # system("c:/users/ATSPM/miniconda3/python.exe get_travel_times_1hr.py", wait = FALSE)
    system("~/miniconda3/bin/conda run -n vdot-flexdashboard-report python get_travel_times.py travel_times_1hr.yaml", wait = FALSE)
    # system("~/miniconda3/bin/conda run -n vdot-flexdashboard-report python get_travel_times.py travel_times_15min.yaml", wait = FALSE)
}

# # COUNTS ####################################################################

print(glue("{Sys.time()} counts [4 of 11]"))

if (conf$run$counts == TRUE) {
    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    if (length(date_range) == 1) {
        date_ <- date_range
        get_counts2(
            date_,
            bucket = conf$bucket,
            conf = conf,
            uptime = TRUE,
            counts = TRUE
        )
    } else {
        foreach(date_ = date_range, .errorhandling = "pass") %dopar% {
            get_counts2(
                date_,
                bucket = conf$bucket,
                conf = conf,
                uptime = TRUE,
                counts = TRUE
            )
        }
    }


    print(glue("{Sys.time()} Detection Levels by Signal [5.1 of 11]"))

    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    lapply(date_range, function(date_) {

        signals_df <- select(corridors, SignalID) %>% mutate(Date = date_)
        detection_levels_df <- get_detection_levels_by_signal(date_)

        det_levels <- left_join(signals_df, detection_levels_df, by = "SignalID") %>%
            replace_na(list(Level = 0))
        s3_upload_parquet_date_split(
            det_levels,
            bucket = conf$bucket,
            prefix = "detection_levels",
            table_name = "detection_levels",
            conf = conf, parallel = FALSE
        )
    })
}


print("\n---------------------- Finished counts ---------------------------\n")



print(glue("{Sys.time()} monthly cu [5 of 11]"))


# --- Everything up to here needs the ATSPM Database ---

signals_list <- as.integer(as.character(corridors$SignalID))
signals_list <- unique(as.character(signals_list[signals_list > 0]))

# Group into months to calculate filtered and adjusted counts
# adjusted counts needs a full month to fill in gaps based on monthly averages


# Read Raw Counts for a month from files and output:
#   filtered_counts_1hr
#   adjusted_counts_1hr
#   BadDetectors

# COMMUNICATIONS QUALITY (FROM KITS) as proxy for communications uptime

# df <- s3read_using(read_parquet, bucket = conf$bucket, object = "mark/comm_quality/date=2022-04-28/2022-04-28_cq.parquet") %>% convert_to_utc()
# df %>% transmute(SignalID = INTID, CallPhase = 0, uptime = AvgQuality/100, ones = 1, Date = as_date(CSDATE))



print(glue("{Sys.time()} counts-based measures [6 of 11]"))

get_counts_based_measures <- function(month_abbrs) {
    lapply(month_abbrs, function(yyyy_mm) {
        # yyyy_mm <- month_abbrs[1] # for debugging
        gc()

        #-----------------------------------------------
        # 1-hour counts, filtered, adjusted, bad detectors

        # start and end days of the month
        sd <- ymd(paste0(yyyy_mm, "-01"))
        ed <- sd + months(1) - days(1)
        ed <- min(ed, ymd(end_date))
        date_range <- seq(sd, ed, by = "1 day")


        print("1-hour adjusted counts")
        prep_db_for_adjusted_counts_arrow("filtered_counts_1hr", conf, date_range)
        get_adjusted_counts_arrow("filtered_counts_1hr", "adjusted_counts_1hr", conf)

        fc_ds <- keep_trying(
            function() arrow::open_dataset(sources = "filtered_counts_1hr/"),
            n_tries = 3, timeout = 60)
        ac_ds <- keep_trying(
            function() arrow::open_dataset(sources = "adjusted_counts_1hr/"),
            n_tries = 3, timeout = 60)

        lapply(date_range, function(date_) {
            # print(date_)
            adjusted_counts_1hr <- ac_ds %>%
                filter(Date == date_) %>%
                select(-c(Date, date)) %>%
                collect()
            s3_upload_parquet_date_split(
                adjusted_counts_1hr,
                bucket = conf$bucket,
                prefix = "adjusted_counts_1hr",
                table_name = "adjusted_counts_1hr",
                conf = conf, parallel = FALSE
            )
        })

        mclapply(date_range, mc.cores = usable_cores, mc.preschedule = FALSE, FUN = function(x) {
            write_signal_details(x, conf, signals_list)
        })


        mclapply(date_range, mc.cores = usable_cores, mc.preschedule = FALSE, FUN = function(date_) {
            date_str <- format(date_, "%F")
            if (between(date_, start_date, end_date)) {
                print(glue("filtered_counts_1hr: {date_str}"))
                filtered_counts_1hr <- fc_ds %>%
                    filter(date == date_str) %>%
                    select(-date) %>%
                    collect()

                if (!is.null(filtered_counts_1hr) && nrow(filtered_counts_1hr)) {
                    filtered_counts_1hr <- filtered_counts_1hr %>%
                        mutate(
                            Date = date(Date),
                            SignalID = factor(SignalID),
                            CallPhase = factor(CallPhase),
                            Detector = factor(Detector)
                        )

                    # BAD DETECTORS
                    print(glue("detectors: {date_}"))
                    bad_detectors <- get_bad_detectors(filtered_counts_1hr)
                    s3_upload_parquet_date_split(
                        bad_detectors,
                        bucket = conf$bucket,
                        prefix = "bad_detectors",
                        table_name = "bad_detectors",
                        conf = conf
                    )

                    # # DAILY DETECTOR UPTIME
                    print(glue("ddu: {date_}"))
                    daily_detector_uptime <- get_daily_detector_uptime(filtered_counts_1hr) %>%
                        bind_rows()
                    s3_upload_parquet_date_split(
                        daily_detector_uptime,
                        bucket = conf$bucket,
                        prefix = "ddu",
                        table_name = "detector_uptime_pd",
                        conf = conf
                    )
                }
            }

            print(glue("reading adjusted_counts_1hr: {date_str}"))
            adjusted_counts_1hr <- ac_ds %>%
                filter(date == date_str) %>%
                select(-date) %>%
                collect()

            if (!is.null(adjusted_counts_1hr) && nrow(adjusted_counts_1hr)) {
                adjusted_counts_1hr <- adjusted_counts_1hr %>%
                    mutate(
                        Date = date(Date),
                        SignalID = factor(SignalID),
                        CallPhase = factor(CallPhase),
                        Detector = factor(Detector)
                    )

                # VPD
                print(glue("vpd: {date_}"))
                vpd <- get_vpd(adjusted_counts_1hr) # calculate over current period
                s3_upload_parquet_date_split(
                    vpd,
                    bucket = conf$bucket,
                    prefix = "vpd",
                    table_name = "vehicles_pd",
                    conf = conf
                )

                # VPH
                print(glue("vph: {date_}"))
                vph <- get_vph(adjusted_counts_1hr, interval = "1 hour")
                s3_upload_parquet_date_split(
                    vph,
                    bucket = conf$bucket,
                    prefix = "vph",
                    table_name = "vehicles_ph",
                    conf = conf
                )
            }
        })
        if (dir.exists("filtered_counts_1hr")) {
            unlink("filtered_counts_1hr", recursive = TRUE)
        }
        if (dir.exists("adjusted_counts_1hr")) {
            unlink("adjusted_counts_1hr", recursive = TRUE)
        }



        #-----------------------------------------------
        # 15-minute counts and throughput
        # FOR EVERY TUE, WED, THU OVER THE WHOLE MONTH
        print("15-minute counts and throughput")

        print("15-minute adjusted counts")
        prep_db_for_adjusted_counts_arrow("filtered_counts_15min", conf, date_range)
        get_adjusted_counts_arrow("filtered_counts_15min", "adjusted_counts_15min", conf)

        fc_ds <- keep_trying(
            function() arrow::open_dataset(sources = "filtered_counts_15min/"),
            n_tries = 3, timeout = 60)
        ac_ds <- keep_trying(
            function() arrow::open_dataset(sources = "adjusted_counts_15min/"),
            n_tries = 3, timeout = 60)

        lapply(date_range, function(date_) {
            print(date_)
            adjusted_counts_15min <- ac_ds %>%
                filter(Date == date_) %>%
                select(-c(Date, date)) %>%
                collect()
            s3_upload_parquet_date_split(
                adjusted_counts_15min,
                bucket = conf$bucket,
                prefix = "adjusted_counts_15min",
                table_name = "adjusted_counts_15min",
                conf = conf, parallel = FALSE
            )

            throughput <- get_thruput(adjusted_counts_15min)
            s3_upload_parquet_date_split(
                throughput,
                bucket = conf$bucket,
                prefix = "tp",
                table_name = "throughput",
                conf = conf, parallel = FALSE
            )

            # Vehicles per 15-minute timeperiod
            print(glue("vp15: {date_}"))
            vp15 <- get_vph(adjusted_counts_15min, interval = "15 min")
            s3_upload_parquet_date_split(
                vp15,
                bucket = conf$bucket,
                prefix = "vp15",
                table_name = "vehicles_15min",
                conf = conf
            )
        })

        if (dir.exists("filtered_counts_15min")) {
            unlink("filtered_counts_15min", recursive = TRUE)
        }
        if (dir.exists("adjusted_counts_15min")) {
            unlink("adjusted_counts_15min", recursive = TRUE)
        }



        #-----------------------------------------------
        # 1-hour pedestrian activation counts
        print("1-hour pedestrian activation counts")

        counts_ped_1hr <- s3_read_parquet_parallel(
            "counts_ped_1hr",
            as.character(sd),
            as.character(ed),
            bucket = conf$bucket
        )

        if (!is.null(counts_ped_1hr) && nrow(counts_ped_1hr)) {

            # PAPD - pedestrian activations per day
            print("papd")
            papd <- get_vpd(counts_ped_1hr, mainline_only = FALSE) %>%
                ungroup() %>%
                rename(papd = vpd)
            s3_upload_parquet_date_split(
                papd,
                bucket = conf$bucket,
                prefix = "papd",
                table_name = "ped_actuations_pd",
                conf = conf
            )

            # PAPH - pedestrian activations per hour
            print("paph")
            paph <- get_vph(counts_ped_1hr, interval = "1 hour", mainline_only = FALSE) %>%
                rename(paph = vph)
            s3_upload_parquet_date_split(
                paph,
                bucket = conf$bucket,
                prefix = "paph",
                table_name = "ped_actuations_ph",
                conf = conf
            )
	}

        #-----------------------------------------------
        # 15-min pedestrian activation counts
        print("15-minute pedestrian activation counts")

        counts_ped_15min <- s3_read_parquet_parallel(
            "counts_ped_15min",
            as.character(sd),
            as.character(ed),
            bucket = conf$bucket
        )

        if (!is.null(counts_ped_15min) && nrow(counts_ped_15min)) {
            # PA15 - pedestrian activations per 15min
            print("pa15")
            pa15 <- get_vph(counts_ped_15min, interval = "15 min", mainline_only = FALSE) %>%
                rename(pa15 = vph)
            s3_upload_parquet_date_split(
                pa15,
                bucket = conf$bucket,
                prefix = "pa15",
                table_name = "ped_actuations_15min",
                conf = conf
            )
        }
    })
}
if (conf$run$counts_based_measures == TRUE) {
    get_counts_based_measures(month_abbrs)
}


print("--- Finished counts-based measures ---")



# -- Run etl_dashboard (Python): cycledata, detectionevents to S3/Athena --
print(glue("{Sys.time()} etl [7 of 11]"))

if (conf$run$etl == TRUE) {

    # run python script and wait for completion
    system(glue("~/miniconda3/bin/conda run -n vdot-flexdashboard-report python etl_dashboard.py {start_date} {end_date}"), wait = TRUE)
}

# --- ----------------------------- -----------

# # GET ARRIVALS ON GREEN #####################################################
print(glue("{Sys.time()} aog [8 of 11]"))

if (conf$run$arrivals_on_green == TRUE) {

    # run python script and wait for completion
    system(glue("~/miniconda3/bin/conda run -n vdot-flexdashboard-report python get_aog.py {start_date} {end_date}"), wait = TRUE)
}
gc()

# # GET QUEUE SPILLBACK #######################################################
get_queue_spillback_date_range <- function(start_date, end_date) {
    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    lapply(date_range, function(date_) {
        print(date_)

        detection_events <- get_detection_events(date_, date_, conf, signals_list)
        if (nrow(collect(head(detection_events))) > 0) {

            qs <- get_qs(detection_events, intervals = c("hour", "15min"))

            s3_upload_parquet_date_split(
                qs$hour,
                bucket = conf$bucket,
                prefix = "qs",
                table_name = "queue_spillback",
                conf = conf
            )
            s3_upload_parquet_date_split(
                qs$`15min`,
                bucket = conf$bucket,
                prefix = "qs",
                table_name = "queue_spillback_15min",
                conf = conf
            )
        }
    })
}
print(glue("{Sys.time()} queue spillback [9 of 11]"))

if (conf$run$queue_spillback == TRUE) {
    get_queue_spillback_date_range(start_date, end_date)
}



# # GET PED DELAY ########################################################

# Ped delay using ATSPM method, based on push button-start of walk durations
print(glue("{Sys.time()} ped delay [10 of 11]"))

get_pd_date_range <- function(start_date, end_date) {
    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    lapply(date_range, function(date_) {
        print(date_)

        pd <- get_ped_delay(date_, conf, signals_list)
        if (nrow(pd) > 0) {
            s3_upload_parquet_date_split(
                pd,
                bucket = conf$bucket,
                prefix = "pd",
                table_name = "ped_delay",
                conf = conf
            )
        }
    })
}

if (conf$run$ped_delay == TRUE) {
    get_pd_date_range(start_date, end_date)
}



# # GET SPLIT FAILURES ########################################################

print(glue("{Sys.time()} split failures [11 of 11]"))

get_sf_date_range <- function(start_date, end_date) {
    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    lapply(date_range, function(date_) {
        print(date_)

        sf <- get_sf_utah(date_, conf, signals_list, intervals = c("hour", "15min"))

        s3_upload_parquet_date_split(
            sf$hour,
            bucket = conf$bucket,
            prefix = "sf",
            table_name = "split_failures",
            conf = conf
        )
        s3_upload_parquet_date_split(
            sf$`15min`,
            bucket = conf$bucket,
            prefix = "sf",
            table_name = "split_failures_15min",
            conf = conf
        )
    })
}

if (conf$run$split_failures == TRUE) {
    # Utah method, based on green, start-of-red occupancies
    get_sf_date_range(start_date, end_date)
}



# # GET TERMINATION TYPES #####################################################

print(glue("{Sys.time()} phase terminations [11b of 11]"))

if (conf$run$phase_termination == TRUE) {
    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    lapply(date_range, function(date_) {
        print(date_)

        terms <- get_termination_type(date_, conf, signals_list)

        s3_upload_parquet_date_split(
            terms,
            bucket = conf$bucket,
            prefix = "term",
            table_name = "phase_termination",
            conf = conf
        )
    })
}


print("\n--------------------- End Monthly Report calcs -----------------------\n")
