
# Monthly_Report_Calcs.R

library(yaml)
library(glue)

source("Monthly_Report_Functions.R")


print(glue("{Sys.time()} Starting Calcs Script"))


usable_cores <- get_usable_cores(4)
doParallel::registerDoParallel(cores = usable_cores)


# aurora_pool <- get_aurora_connection_pool()
# aurora <- get_aurora_connection()

#----- DEFINE DATE RANGE FOR CALCULATIONS ------------------------------------#
start_date <- ifelse(
    conf$start_date == "yesterday",
    format(today() - days(1), "%Y-%m-%d"),
    conf$start_date
)
end_date <- ifelse(
    conf$end_date == "yesterday",
    format(today() - days(1), "%Y-%m-%d"),
    conf$end_date
)

# Manual overrides
# start_date <- "2020-01-04"
# end_date <- "2020-01-04"

month_abbrs <- get_month_abbrs(start_date, end_date)
#-----------------------------------------------------------------------------#

# # GET CORRIDORS #############################################################

# -- Code to update corridors file/table from Excel file

corridors <- s3read_using(
    function(x) get_corridors(x, filter_signals = TRUE),
    object = conf$corridors_filename_s3,
    bucket = conf$bucket
)
feather_filename <- sub("\\..*", ".feather", conf$corridors_filename_s3)
write_feather(corridors, feather_filename)
aws.s3::put_object(
    file = feather_filename,
    object = feather_filename,
    bucket = conf$bucket,
    multipart = TRUE
)
qs_filename <- sub("\\..*", ".qs", conf$corridors_filename_s3)
qsave(corridors, qs_filename)
aws.s3::put_object(
    file = qs_filename,
    object = qs_filename,
    bucket = conf$bucket,
    multipart = TRUE
)

all_corridors <- s3read_using(
    function(x) get_corridors(x, filter_signals = FALSE),
    object = conf$corridors_filename_s3,
    bucket = conf$bucket
)
feather_filename <- sub("\\..*", ".feather", paste0("all_", conf$corridors_filename_s3))
write_feather(all_corridors, feather_filename)
aws.s3::put_object(
    file = feather_filename,
    object = feather_filename,
    bucket = conf$bucket,
    multipart = TRUE
)
qs_filename <- sub("\\..*", ".qs", paste0("all_", conf$corridors_filename_s3))
qsave(all_corridors, qs_filename)
aws.s3::put_object(
    file = qs_filename,
    object = qs_filename,
    bucket = conf$bucket,
    multipart = TRUE
)

signals_list <- unique(corridors$SignalID)




print(Sys.time())

# # GET CAMERA UPTIMES ########################################################

print(glue("{Sys.time()} parse cctv logs [1 of 11]"))

if (conf$run$cctv == TRUE) {
    # Run python scripts asynchronously
    system("c:/users/ATSPM/miniconda3/python.exe parse_cctvlog.py", wait = FALSE)
    system("c:/users/ATSPM/miniconda3/python.exe parse_cctvlog_encoders.py", wait = FALSE)
    system("~/miniconda3/bin/python parse_cctvlog.py", wait = FALSE)
    system("~/miniconda3/bin/python parse_cctvlog_encoders.py", wait = FALSE)
}

# # GET RSU UPTIMES ###########################################################

print(glue("{Sys.time()} parse rsu logs [2 of 11]"))

# # TRAVEL TIMES FROM RITIS API ###############################################

print(glue("{Sys.time()} travel times [3 of 11]"))

if (conf$run$travel_times == TRUE) {
    # Run python script asynchronously
    # system("c:/users/ATSPM/miniconda3/python.exe get_travel_times_1hr.py", wait = FALSE)
    system("~/miniconda3/bin/python get_travel_times.py travel_times_1hr.yaml", wait = FALSE)
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
            conf_athena = conf$athena,
            uptime = TRUE,
            counts = TRUE
        )
    } else {
        foreach(date_ = date_range, .errorhandling = "pass") %dopar% {
            get_counts2(
                date_,
                bucket = conf$bucket,
                conf_athena = conf$athena,
                uptime = TRUE,
                counts = TRUE
            )
        }
    }
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

print(glue("{Sys.time()} counts-based measures [6 of 11]"))

get_counts_based_measures <- function(month_abbrs) {
    lapply(month_abbrs, function(yyyy_mm) {
        # yyyy_mm <- month_abbrs # for debugging
        gc()

        #-----------------------------------------------
        # 1-hour counts, filtered, adjusted, bad detectors

        # start and end days of the month
        sd <- ymd(paste0(yyyy_mm, "-01"))
        ed <- sd + months(1) - days(1)
        ed <- min(ed, ymd(end_date))
        date_range <- seq(sd, ed, by = "1 day")


        print("adjusted counts")
        s3_read_parquet_parallel(
            "filtered_counts_1hr",
            as.character(sd),
            as.character(ed),
            bucket = conf$bucket
        ) %>%
            mutate(
                Date = date(Date),
                SignalID = factor(SignalID),
                CallPhase = factor(CallPhase),
                Detector = factor(Detector)
            ) %>%
            get_adjusted_counts() %>%
            s3_upload_parquet_date_split(
                bucket = conf$bucket,
                prefix = "adjusted_counts_1hr",
                table_name = "adjusted_counts_1hr",
                conf_athena = conf$athena
            )

        lapply(date_range, function(date_) {
            # date_ <- date_range[1] # for debugging
            date_str <- format(date_, "%F")
            if (between(date_, start_date, end_date)) {
                print(glue("filtered_counts_1hr: {date_}"))
                filtered_counts_1hr <- s3_read_parquet_parallel(
                    "filtered_counts_1hr",
                    date_str,
                    date_str,
                    bucket = conf$bucket
                )
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
                        conf_athena = conf$athena
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
                        conf_athena = conf$athena
                    )
                }
            }

            print(glue("reading adjusted_counts_1hr: {date_}"))
            adjusted_counts_1hr <- s3_read_parquet_parallel(
                "adjusted_counts_1hr",
                as.character(date_),
                as.character(date_),
                bucket = conf$bucket
            )

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
                    conf_athena = conf$athena
                )

                # VPH
                print(glue("vph: {date_}"))
                vph <- get_vph(adjusted_counts_1hr)
                s3_upload_parquet_date_split(
                    vph,
                    bucket = conf$bucket,
                    prefix = "vph",
                    table_name = "vehicles_ph",
                    conf_athena = conf$athena
                )
            }
        })


        #-----------------------------------------------
        # 15-minute counts and throughput
        # FOR EVERY TUE, WED, THU OVER THE WHOLE MONTH
        print("15-minute counts and throughput")

        doParallel::registerDoParallel(cores = usable_cores)

        date_range_twr <- date_range[lubridate::wday(date_range, label = TRUE) %in% c("Tue", "Wed", "Thu")]

        filtered_counts_15min <- lapply(date_range_twr, function(date_) {
            date_ <- as.character(date_)
            print(date_)
            s3_read_parquet_parallel(
                "filtered_counts_15min", date_, date_, bucket = conf$bucket)
        }) %>% bind_rows()

        if (!is.null(filtered_counts_15min) && nrow(filtered_counts_15min)) {
            filtered_counts_15min <- filtered_counts_15min %>%
                transmute(
                    SignalID = factor(SignalID),
                    CallPhase = factor(CallPhase),
                    Detector = factor(Detector),
                    Date = date(Date),
                    Timeperiod = Timeperiod,
                    Month_Hour = Month_Hour,
                    Hour = Hour,
                    vol = vol,
                    Good_Day = Good_Day,
                    delta_vol = delta_vol,
                    mean_abs_delta = mean_abs_delta
                )
        }

        print("adjusted counts and throughput")

        if (length(filtered_counts_15min) > 0) {
            print("15-min adjusted counts")
            adjusted_counts_15min <- get_adjusted_counts(filtered_counts_15min) %>%
                mutate(Date = date(Timeperiod))
            rm(filtered_counts_15min)

            # Calculate and write Throughput
            throughput <- get_thruput(adjusted_counts_15min)

            s3_upload_parquet_date_split(
                throughput,
                bucket = conf$bucket,
                prefix = "tp",
                table_name = "throughput",
                conf_athena = conf$athena
            )
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
                conf_athena = conf$athena
            )

            # PAPH - pedestrian activations per hour
            print("paph")
            paph <- get_vph(counts_ped_1hr, mainline_only = FALSE) %>%
                rename(paph = vph)
            s3_upload_parquet_date_split(
                paph,
                bucket = conf$bucket,
                prefix = "paph",
                table_name = "ped_actuations_ph",
                conf_athena = conf$athena
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
    # system2("etl_dashboard.bat", args = c(start_date, end_date))
    system2("./etl_dashboard.sh", args = c(start_date, end_date))
}

# --- ----------------------------- -----------

# # GET ARRIVALS ON GREEN #####################################################
print(glue("{Sys.time()} aog [8 of 11]"))

if (conf$run$arrivals_on_green == TRUE) {

    # run python script and wait for completion
    # system2("get_aog.bat", args = c(start_date, end_date))
    system2("./get_aog.sh", args = c(start_date, end_date))
}
gc()

# # GET QUEUE SPILLBACK #######################################################
get_queue_spillback_date_range <- function(start_date, end_date) {
    date_range <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    lapply(date_range, function(date_) {
        print(date_)

        detection_events <- get_detection_events(date_, date_, conf$athena, signals_list)
        if (nrow(collect(head(detection_events))) > 0) {
            qs <- get_qs(detection_events)
            s3_upload_parquet_date_split(
                qs,
                bucket = conf$bucket,
                prefix = "qs",
                table_name = "queue_spillback",
                conf_athena = conf$athena)
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
                conf_athena = conf$athena
            )
        }
    })
    gc()
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

            sf <- get_sf_utah(date_, conf, signals_list)
            s3_upload_parquet_date_split(
                sf,
                bucket = conf$bucket,
                prefix = "sf",
                table_name = "split_failures",
                conf_athena = conf$athena
            )
    })
}

if (conf$run$split_failures == TRUE) {
    # Utah method, based on green, start-of-red occupancies
    get_sf_date_range(start_date, end_date)
}



print("\n--------------------- End Monthly Report calcs -----------------------\n")
