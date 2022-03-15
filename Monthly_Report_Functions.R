
# Monthly_Report_Functions.R

suppressMessages({
    library(DBI)
    library(readxl)
    library(readr)
    library(dplyr)
    library(tidyr)
    library(stringr)
    library(purrr)
    library(lubridate)
    library(glue)
    library(data.table)
    library(formattable)
    library(forcats)
    library(fst)
    library(parallel)
    library(doParallel)
    library(future)
    library(pool)
    library(httr)
    library(aws.s3)
    library(sp)
    library(sf)
    library(yaml)
    library(utils)
    library(readxl)
    library(runner)
    library(fitdistrplus)
    library(foreach)
    library(arrow)
    # https://arrow.apache.org/install/
    library(qs)
})


#options(dplyr.summarise.inform = FALSE)

select <- dplyr::select
filter <- dplyr::filter

options(future.rng.onMisue = "ignore")

conf <- read_yaml("Monthly_Report.yaml")
aws_conf <- read_yaml("Monthly_Report_AWS.yaml")
conf$athena$uid <- aws_conf$AWS_ACCESS_KEY_ID
conf$athena$pwd <- aws_conf$AWS_SECRET_ACCESS_KEY
conf$athena$region <- aws_conf$AWS_DEFAULT_REGION


if (Sys.info()["sysname"] == "Windows") {
    home_path <- dirname(path.expand("~"))

} else if (Sys.info()["sysname"] == "Linux") {
    home_path <- "~"

} else {
    stop("Unknown operating system.")
}


# Colorbrewer Paired Palette Colors
LIGHT_BLUE = "#A6CEE3";   BLUE = "#1F78B4"
LIGHT_GREEN = "#B2DF8A";  GREEN = "#33A02C"
LIGHT_RED = "#FB9A99";    RED = "#E31A1C"
LIGHT_ORANGE = "#FDBF6F"; ORANGE = "#FF7F00"
LIGHT_PURPLE = "#CAB2D6"; PURPLE = "#6A3D9A"
LIGHT_BROWN = "#FFFF99";  BROWN = "#B15928"

RED2 = "#e41a1c"
VDOT_BLUE = "#005da9"

BLACK <- "#000000"
WHITE <- "#FFFFFF"
GRAY <- "#D0D0D0"
DARK_GRAY <- "#7A7A7A"
DARK_DARK_GRAY <- "#494949"


SUN = 1; MON = 2; TUE = 3; WED = 4; THU = 5; FRI = 6; SAT = 7

AM_PEAK_HOURS = conf$AM_PEAK_HOURS
PM_PEAK_HOURS = conf$PM_PEAK_HOURS

Sys.setenv(TZ="America/New_York")


get_cor <- function() {
    s3read_using(qs::qread, bucket = conf$bucket, object = "cor_ec2.qs")
}
get_sub <- function() {
    s3read_using(qs::qread, bucket = conf$bucket, object = "sub_ec2.qs")
}
get_sig <- function() {
    s3read_using(qs::qread, bucket = conf$bucket, object = "sig_ec2.qs")
}

sizeof <- function(x) {
    format(object.size(x), units = "Mb")
}

apply_style <- function(filename) {
    styler::style_file(filename, transformers = styler::tidyverse_style(indent_by = 4))
}

get_most_recent_monday <- function(date_) {
    date_ + days(1 - lubridate::wday(date_, week_start = 1))
}

get_usable_cores <- function(GB=8) {
    # Usable cores is one per 8 GB of RAM.
    # Get RAM from system file and divide

    if (Sys.info()["sysname"] == "Windows") {
        1
    } else if (Sys.info()["sysname"] == "Linux") {
        x <- readLines('/proc/meminfo')

        memline <- x[grepl("MemTotal", x)]
        mem <- stringr::str_extract(string =  memline, pattern = "\\d+")
        mem <- as.integer(mem)
        mem <- round(mem, -6)
        min(max(floor(mem/(GB*1e6)), 1), parallel::detectCores() - 1)

    } else {
        stop("Unknown operating system.")
    }
}



split_wrapper <- function(FUN) {

    # Creates a function that runs a function, splits by signalid and recombines

    f <- function(df, split_size, ...) {
        # Define temporary directory and file names
        temp_dir <- tempdir()
        if (!dir.exists(temp_dir)) {
            dir.create(temp_dir)
        }
        temp_file_root <- stringi::stri_rand_strings(1,8)
        temp_path_root <- file.path(temp_dir, temp_file_root)
        print(temp_path_root)


        print("Writing to temporary files by SignalID...")
        signalids <- as.character(unique(df$SignalID))
        splits <- split(signalids, ceiling(seq_along(signalids)/split_size))
        lapply(
            names(splits),
            function(i) {
                cat('.')
                df %>%
                    filter(SignalID %in% splits[[i]]) %>%
                    write_fst(paste0(temp_path_root, "_", i, ".fst"))
            })
        cat('.', sep='\n')

        file_names <- paste0(temp_path_root, "_", names(splits), ".fst")

        # Read in each temporary file and run adjusted counts in parallel. Afterward, clean up.
        print("Running for each SignalID...")
        df <- mclapply(file_names, mc.cores = usable_cores, FUN = function(fn) {
            cat('.')
            FUN(read_fst(fn), ...)
        }) %>% bind_rows()
        cat('.', sep='\n')

        lapply(file_names, FUN = file.remove)

        df
    }
}


read_zipped_feather <- function(x) {
    read_feather(unzip(x))
}



keep_trying <- function(func, n_tries, ...){

    possibly_func = purrr::possibly(func, otherwise = NULL)

    result = NULL
    try_number = 1
    sleep = 1

    while(is.null(result) && try_number <= n_tries){
        if (try_number > 1) {
            print(paste("Attempt:", try_number))
        }
        try_number = try_number + 1
        result = possibly_func(...)
        Sys.sleep(sleep)
        sleep = sleep + 1
    }

    return(result)
}


get_atspm_connection <- function(conf_atspm) {

    dbConnect(odbc::odbc(),
              dsn = conf_atspm$ATSPM_DSN,
              uid = conf_atspm$ATSPM_UID,
              pwd = conf_atspm$ATSPM_PWD)
}


get_maxview_connection <- function(dsn = "MaxView") {

    if (Sys.info()["sysname"] == "Windows") {

        dbConnect(odbc::odbc(),
                  dsn = dsn,
                  uid = Sys.getenv("ATSPM_USERNAME"),
                  pwd = Sys.getenv("ATSPM_PASSWORD"))

    } else if (Sys.info()["sysname"] == "Linux") {

        dbConnect(odbc::odbc(),
                  driver = "FreeTDS",
                  server = Sys.getenv("MAXV_SERVER_INSTANCE"),
                  database = Sys.getenv("MAXV_EVENTLOG_DB"),
                  uid = Sys.getenv("ATSPM_USERNAME"),
                  pwd = Sys.getenv("ATSPM_PASSWORD"))
    }
}


get_maxview_eventlog_connection <- function() {
    get_maxview_connection(dsn = "MaxView_EventLog")
}


get_cel_connection <- get_maxview_eventlog_connection



get_aurora_connection <- function(f = RMySQL::dbConnect) {

    f(drv = RMySQL::MySQL(),
      host = cred$RDS_HOST,
      port = 3306,
      dbname = cred$RDS_DATABASE,
      username = cred$RDS_USERNAME,
      password = cred$RDS_PASSWORD)
}


get_aurora_connection_pool <- function() {
    get_aurora_connection(pool::dbPool)
}


get_athena_connection <- function(conf_athena, f = dbConnect) {
    f(odbc::odbc(), dsn = "athena")
}


get_athena_connection_pool <- function(conf_athena) {
    get_athena_connection(conf_athena, pool::dbPool)
}


add_partition <- function(conf_athena, table_name, date_) {
    tryCatch({
        conn_ <- get_athena_connection(conf_athena)
        dbExecute(conn_,
                  sql(glue(paste("ALTER TABLE {conf_athena$database}.{table_name}",
                                 "ADD PARTITION (date='{date_}')"))))
        print(glue("Successfully created partition (date='{date_}') for {conf_athena$database}.{table_name}"))
    }, error = function(e) {
        print(stringr::str_extract(as.character(e), "message:.*?\\."))
    }, finally = {
        dbDisconnect(conn_)
    })
}



# Because of issue with Apache Arrow (feather, parquet)
# where R wants to convert UTC to local time zone on read
# Switch date or datetime fields back to UTC. Run on read.
convert_to_utc <- function(df) {

    # -- This may be a more elegant alternative. Needs testing. --
    #df %>% mutate_if(is.POSIXct, ~with_tz(., "UTC"))

    is_datetime <- sapply(names(df), function(x) sum(class(df[[x]]) == "POSIXct"))
    datetimes <- names(is_datetime[is_datetime==1])

    for (col in datetimes) {
        df[[col]] <- with_tz(df[[col]], "UTC")
    }
    df
}


s3_upload_parquet <- function(df, date_, fn, bucket, table_name, conf_athena) {

  df <- ungroup(df)

  if ("Date" %in% names(df)) {
    df <- df %>% select(-Date)
  }


  if ("Detector" %in% names(df)) {
    df <- mutate(df, Detector = as.character(Detector))
  }
  if ("CallPhase" %in% names(df)) {
    df <- mutate(df, CallPhase = as.character(CallPhase))
  }
  if ("SignalID" %in% names(df)) {
    df <- mutate(df, SignalID = as.character(SignalID))
  }

      keep_trying(
        s3write_using,
        n_tries = 5,
        df,
        write_parquet,
        use_deprecated_int96_timestamps = TRUE,
        bucket = bucket,
        object = glue("mark/{table_name}/date={date_}/{fn}.parquet"),
        opts = list(multipart = TRUE, body_as_string = TRUE)
    )

    add_partition(conf_athena, table_name, date_)
}



s3_upload_parquet_date_split <- function(df, prefix, bucket, table_name, conf_athena, parallel = TRUE) {

  if (!("Date" %in% names(df))) {
    if ("Timeperiod" %in% names(df)) {
      df <- mutate(df, Date = date(Timeperiod))
    } else if ("Hour" %in% names(df)) {
      df <- mutate(df, Date = date(Hour))
    }
  }

    d <- unique(df$Date)
    if (length(d) == 1) { # just one date. upload.
        date_ <- d
        s3_upload_parquet(df, date_,
                          fn = glue("{prefix}_{date_}"),
                          bucket = bucket,
                          table_name = table_name,
                          conf_athena = conf_athena)
    } else { # loop through dates
        if (parallel & Sys.info()["sysname"] != "Windows") {
            df %>%
                split(.$Date) %>%
                mclapply(mc.cores = max(usable_cores, detectCores()-1), FUN = function(x) {
                    date_ <- as.character(x$Date[1])
                    s3_upload_parquet(x, date_,
                                      fn = glue("{prefix}_{date_}"),
                                      bucket = bucket,
                                      table_name = table_name,
                                      conf_athena = conf_athena)
                    Sys.sleep(1)
                })
        } else {
            df %>%
                split(.$Date) %>%
                lapply(function(x) {
                    date_ <- as.character(x$Date[1])
                    s3_upload_parquet(x, date_,
                                      fn = glue("{prefix}_{date_}"),
                                      bucket = bucket,
                                      table_name = table_name,
                                      conf_athena = conf_athena)
                })
        }
    }

}


s3_read_parquet <- function(bucket, object, date_ = NULL) {

  if (is.null(date_)) {
    date_ <- str_extract(object, "\\d{4}-\\d{2}-\\d{2}")
  }
    tryCatch({
  s3read_using(read_parquet, bucket = bucket, object = object) %>%
    select(-starts_with("__")) %>%
    mutate(Date = ymd(date_))
        }, error = function(e) {
        data.frame()
    })
}


s3_read_parquet_parallel <- function(table_name,
                                     start_date,
                                     end_date,
                                     signals_list = NULL,
                                     bucket = NULL,
                                     callback = function(x) {x},
                                     parallel = FALSE) {

    dates <- seq(ymd(start_date), ymd(end_date), by = "1 day")

    func <- function(date_) {
        prefix <- glue("mark/{table_name}/date={date_}")
        objects = aws.s3::get_bucket(bucket = bucket, prefix = prefix)
        lapply(objects, function(obj) {
            s3_read_parquet(bucket = bucket, object = get_objectkey(obj), date_) %>%
                convert_to_utc() %>%
                callback()
        }) %>% bind_rows()
    }
    # When using mclapply, it fails. When using lapply, it works. 6/23/2020
    # Give to option to run in parallel, like when in interactive mode
    if (parallel & Sys.info()["sysname"] != "Windows") {
        dfs <- mclapply(dates, mc.cores = max(usable_cores, detectCores()-1), FUN = func)
    } else {
        dfs <- lapply(dates, func)
    }
    dfs[lapply(dfs, nrow)>0] %>% bind_rows()
}




aurora_write_parquet <- function(conn, df, date_, table_name) {
    fieldnames <- dbListFields(conn, table_name)

    # clear existing data for the given table and date
    response <- dbSendQuery(
        conn,
        glue("delete from {table_name} where Date = {date_}"))

    # Write data to Aurora database for the given day.
    # Append to table.
    dbWriteTable(
        conn,
        table_name,
        select(df, !!!fieldnames),
        overwrite = FALSE, append = TRUE, row.names = FALSE)
}


week <- function(d) {
    d0 <- ymd("2016-12-25")
    as.integer(trunc((ymd(d) - d0)/dweeks(1)))
}


get_month_abbrs <- function(start_date, end_date) {
    start_date <- ymd(start_date)
    day(start_date) <- 1
    end_date <- ymd(end_date)

    sapply(seq(start_date, end_date, by = "1 month"), function(d) { format(d, "%Y-%m")} )
}


bind_rows_keep_factors <- function(dfs) {
    ## Identify all factors
    factors <- unique(unlist(
        map(list(dfs[[1]]), ~ select_if(dfs[[1]], is.factor) %>% names())
    ))
    ## Bind dataframes, convert characters back to factors
    suppressWarnings(bind_rows(dfs)) %>%
        mutate_at(vars(one_of(factors)), factor)
}


match_type <- function(val, val_type_to_match) {
    eval(parse(text=paste0('as.',class(val_type_to_match), "(", val, ")")))
}


addtoRDS <- function(df, fn, delta_var, rsd, csd) {

    #' combines data frame in local rds file with newly calculated data
    #' trimming the current data and appending the new data to prevent overlaps
    #' and/or duplicates. Used throughout Monthly_Report_Package code
    #' to avoid having to recalculate entire report period (13 months) every time
    #' which takes too long and runs into memory issues frequently.
    #'
    #' @param df newly calculated data frame on most recent data
    #' @param fn filename of same data over entire reporting period (13 months)
    #' @param rsd report_start_date: start of current report period (13 months prior)
    #' @param csd calculation_start_date: start date of most recent data
    #' @return a combined data frame
    #' @examples
    #' addtoRDS(avg_daily_detector_uptime, "avg_daily_detector_uptime.rds", report_start_date, calc_start_date)

    combine_dfs <- function(df0, df, delta_var, rsd, csd) {

        # Extract aggregation period from the data fields
        periods <- intersect(c("Month", "Date", "Hour"), names(df0))
        per_ <- as.name(periods)

        # Remove everything after calcs_start_date (csd) in original df
        df0 <- df0 %>% filter(!!per_ >= rsd, !!per_ < csd)

        # Make sure new data starts on csd
        # This is especially important for when csd is the start of the month
        # and we've run calcs going back to the start of the week, which is in
        # the previous month, e.g., 3/31/2020 is a Tuesday.
        df <- df %>% filter(!!per_ >= csd)

        # Extract aggregation groupings from the data fields
        # to calculate period-to-period deltas

        groupings <- intersect(c("Zone_Group", "Corridor", "SignalID"), names(df0))
        groups_ <- sapply(groupings, as.name)

        group_arrange <- c(periods, groupings) %>%
            sapply(as.name)

        var_ <- as.name(delta_var)

        # Combine old and new
        x <- bind_rows_keep_factors(list(df0, df)) %>%

            # Recalculate deltas from prior periods over combined df
            group_by(!!!groups_) %>%
            arrange(!!!group_arrange) %>%
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(-lag_)

        x
    }

    if (!file.exists(fn)) {
        saveRDS(df, fn)
    } else {
        df0 <- readRDS(fn)
        if (is.list(df) && is.list(df0) &&
            !is.data.frame(df) && !is.data.frame(df0) &&
            (names(df) == names(df0))) {
            x <- purrr::map2(df0, df, combine_dfs, delta_var, rsd, csd)
        } else {
            x <- combine_dfs(df0, df, delta_var, rsd, csd)
        }
        saveRDS(x, fn)
        x
    }

}


write_fst_ <- function(df, fn, append = FALSE) {
    if (append == TRUE & file.exists(fn)) {

        factors <- unique(unlist(
            map(list(df), ~ select_if(df, is.factor) %>% names())
        ))

        df_ <- read_fst(fn)
        df_ <- bind_rows(df, df_) %>%
            mutate_at(vars(one_of(factors)), factor)
    } else {
        df_ <- df
    }
    write_fst(distinct(df_), fn)
}


get_corridors <- function(corr_fn, filter_signals = TRUE) {

    # Keep this up to date to reflect the Corridors_Latest.xlsx file
    cols <- list(SignalID = "numeric", #"text",
                 Zone_Group = "text",
                 Zone = "text",
                 Corridor = "text",
                 Subcorridor = "text",
                 Agency = "text",
                 `Main Street Name` = "text",
                 `Side Street Name` = "text",
                 Milepost = "numeric",
                 Asof = "date",
                 Duplicate = "numeric",
                 Include = "logical",
                 Modified = "date",
                 Note = "text",
                 Latitude = "numeric",
                 Longitude = "numeric")

    df <- readxl::read_xlsx(corr_fn, col_types = unlist(cols)) %>%

        # Get the last modified record for the Signal|Zone|Corridor combination
        replace_na(replace = list(Modified = ymd("1900-01-01"))) %>%
        group_by(SignalID, Zone, Corridor) %>%
        filter(Modified == max(Modified)) %>%
        ungroup() %>%

        filter(!is.na(Corridor))

    # if filter_signals == FALSE, this creates all_corridors, which
    #   includes corridors without signals
    #   which is used for manual ped/det uptimes and camera uptimes
    if (filter_signals) {
        df <- df %>%
            filter(
                SignalID > 0,
                Include == TRUE)
    }

    df %>%
        tidyr::unite(Name, c(`Main Street Name`, `Side Street Name`), sep = ' @ ') %>%
        transmute(SignalID = factor(SignalID),
                  Zone = as.factor(Zone),
                  Zone_Group = factor(Zone_Group),
                  Corridor = as.factor(Corridor),
                  Subcorridor = as.factor(Subcorridor),
                  Milepost = as.numeric(Milepost),
                  Agency = Agency,
                  Name = Name,
                  Asof = date(Asof)) %>%
        mutate(Description = paste(SignalID, Name, sep = ": "))

}


get_unique_timestamps <- function(df) {
    df %>%
        dplyr::select(Timestamp) %>%

        distinct() %>%
        mutate(SignalID = 0) %>%
        dplyr::select(SignalID, Timestamp)
}


get_uptime <- function(df, start_date, end_time) {

    ts_sig <- df %>%
        mutate(timestamp = date_trunc('minute', timestamp)) %>% # Presto/Athena
        # mutate(ts = DATEADD(Minute, DATEDIFF(Minute, 0, timestamp), 0)) %>% # SQL Server
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


multicore_decorator <- function(FUN) {

    usable_cores <- get_usable_cores()

    function(x) {
        x %>%
            split(.$SignalID) %>%
            mclapply(FUN, mc.cores = usable_cores) %>% 
            bind_rows()
    }
}


# This is a "function factory"
# It is meant to be used to create a get_det_config function that takes only the date:
# like: get_det_config <- get_det_config_(conf$bucket, "atspm_det_config_good")
get_det_config_  <- function(bucket, folder) {

    function(date_) {

        tryCatch({
            arrow::open_dataset(
                sources = glue("s3://{bucket}/{folder}/date={date_}"), 
                format="feather", 
                unify_schemas=TRUE
            ) %>% 
            collect() %>%
            mutate(SignalID = as.character(SignalID),
                   Detector = as.integer(Detector),
                   CallPhase = as.integer(CallPhase))
        }, error = function(e) {
            stop(glue("Problem getting detector config file for {date_}"))
            print(e)
        })
    }
}

get_det_config <- get_det_config_(conf$bucket, "atspm_det_config_good")
get_ped_config <- get_det_config_(conf$bucket, "atspm_ped_config")



get_det_config_aog <- function(date_) {

    get_det_config(date_) %>%
        filter(!is.na(Detector)) %>%
        mutate(AOGPriority =
                   dplyr::case_when(
                       grepl("Exit", DetectionTypeDesc)  ~ 0,
                       grepl("Advanced Count", DetectionTypeDesc) ~ 1,
                       TRUE ~ 2)) %>%
        filter(AOGPriority < 2) %>%
        group_by(SignalID, CallPhase) %>%
        filter(AOGPriority == min(AOGPriority)) %>%
        ungroup() %>%

        transmute(SignalID = factor(SignalID),
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_))
}


get_det_config_qs <- function(date_) {

    # Detector config
    dc <- get_det_config(date_) %>%
        filter(grepl("Advanced Count", DetectionTypeDesc) |
                   grepl("Advanced Speed", DetectionTypeDesc)) %>%
        filter(!is.na(DistanceFromStopBar)) %>%
        filter(!is.na(Detector)) %>%

        transmute(SignalID = factor(SignalID),
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_))

    # Bad detectors
    bd <- s3read_using(
        read_parquet,
        bucket = conf$bucket,
        object = glue("mark/bad_detectors/date={date_}/bad_detectors_{date_}.parquet")) %>%
        transmute(SignalID = factor(SignalID),
                    Detector = factor(Detector),
                    Good_Day)

    # Join to take detector config for only good detectors for this day
    left_join(dc, bd, by=c("SignalID", "Detector")) %>%
        filter(is.na(Good_Day)) %>% select(-Good_Day)

}


get_det_config_sf <- function(date_) {

    get_det_config(date_) %>%
        filter(grepl("Stop Bar Presence", DetectionTypeDesc)) %>%
        filter(!is.na(Detector)) %>%


        transmute(SignalID = factor(SignalID),
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_))
}


get_det_config_vol <- function(date_) {

    get_det_config(date_) %>%
        transmute(SignalID = factor(SignalID),
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  CountPriority = as.integer(CountPriority),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_)) %>%
        group_by(SignalID, CallPhase) %>%
        mutate(minCountPriority = min(CountPriority, na.rm = TRUE)) %>%
        ungroup() %>%
        filter(CountPriority == minCountPriority)
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
## -- --- End of Adds CountPriority from detector config file -------------- -- ##


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



get_spm_data_athena <- function(start_date, end_date, signals_list = NULL, conf_athena, table, TWR_only=TRUE) {

    conn <- get_athena_connection(conf_athena)

    if (TWR_only == TRUE) {
        query_where <- "WHERE date_format(date_parse(date, '%Y-%m-%d'), '%W') in ('Tuesday','Wednesday','Thursday')"
    } else {
        query_where <- ""
    }

    query <- glue("SELECT DISTINCT * FROM {tolower(table)} {query_where}")

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
get_cycle_data <- function(start_date, end_date, conf_athena, signals_list = NULL) {
    get_spm_data_athena(
        start_date,
        end_date,
        signals_list,
        conf_athena,
        table = "CycleData",
        TWR_only = FALSE)
}


# Query Detection Events
get_detection_events <- function(start_date, end_date, conf_athena, signals_list = NULL) {
    get_spm_data_athena(
        start_date,
        end_date,
        signals_list,
        conf_athena,
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

    # SignalID | CallPhase | Week | DOW | Date | vpd
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

    # SignalID | CallPhase | Date | Week | DOW | vph
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

    # SignalID | CallPhase | Date | Week | DOW | aog | vol
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


# SPM Arrivals on Green using Utah method -- modified for use with dbplyr on AWS Athena
get_sf_utah <- function(date_, conf, signals_list = NULL, first_seconds_of_red = 5, parallel = FALSE) {

    print("Pulling data...")

    ncores <- if (parallel & Sys.info()["sysname"] != "Windows") {
         usable_cores * 6
    } else {
        1
    }

    de <- mclapply(signals_list, mc.cores = ncores, FUN = function(signalid) {

        s3bucket = conf$bucket
        s3object = glue("detections/date={date_}/de_{signalid}_{date_}.parquet")

        object_exists <- length(aws.s3::get_bucket(s3bucket, s3object))

        if (object_exists) {
            cat('.')
            tryCatch({
                s3read_using(read_parquet, bucket = s3bucket, object = s3object) %>%
                filter(Phase %in% c(3, 4, 7, 8)) %>%
                arrange(SignalID, Phase, CycleStart, PhaseStart) %>%
                transmute(SignalID = factor(SignalID),
                          Phase = factor(Phase),
                          Detector = factor(Detector),
                          CycleStart, PhaseStart,
                          DetOn = DetTimeStamp,
                          DetOff = DetTimeStamp + seconds(DetDuration),
                          Date = as_date(date_))
            }, error = function(e) {
                print(e)
            })
        }
    }) %>% bind_rows() %>% convert_to_utc()

    cd <- mclapply(signals_list, mc.cores = ncores, FUN = function(signalid) {

        s3bucket = conf$bucket
        s3object = glue("cycles/date={date_}/cd_{signalid}_{date_}.parquet")

        object_exists <- length(aws.s3::get_bucket(s3bucket, s3object))

        if (object_exists) {
            cat('.')
            tryCatch({
                s3read_using(read_parquet, bucket = s3bucket, object = s3object) %>%
                mutate(SignalID = factor(SignalID),
                       Phase = factor(Phase),
                       Date = as_date(date_)) %>%
                filter(Phase %in% c(3, 4, 7, 8),
                       EventCode %in% c(1, 9)) %>%
                arrange(SignalID, Phase, CycleStart, PhaseStart)
            }, error = function(e) {
                print(e)
            })
        }
    }) %>% bind_rows() %>% convert_to_utc()


    dc <- get_det_config_sf(date_) %>%
        filter(SignalID %in% signals_list) %>%
        rename(Phase = CallPhase)

    de <- de %>%
        left_join(dc, by = c("SignalID", "Phase", "Detector", "Date")) %>%
        filter(!is.na(TimeFromStopBar)) %>%
        mutate(SignalID = factor(SignalID),
               Phase = factor(Phase),
               Detector = factor(Detector))

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

    rm(cd)

    de_dt <- data.table(de)
    rm(de)

    cat('.')

    gr_dt <- data.table(grn_interval)
    sr_dt <- data.table(sor_interval)

    setkey(de_dt, SignalID, Phase, DetOn, DetOff)
    setkey(gr_dt, SignalID, Phase, IntervalStart, IntervalEnd)
    setkey(sr_dt, SignalID, Phase, IntervalStart, IntervalEnd)

    ## ---

    get_occupancy <- function(de_dt, int_dt, interval_) {
        occdf <- foverlaps(de_dt, int_dt, type = "any") %>%
            filter(!is.na(IntervalStart)) %>%

            transmute(
                SignalID = factor(SignalID),
                Phase,
                Detector = as.integer(as.character(Detector)),
                CycleStart,
                IntervalStart,
                IntervalEnd,
                int_int = lubridate::interval(IntervalStart, IntervalEnd),
                occ_int = lubridate::interval(DetOn, DetOff),
                occ_duration = as.duration(intersect(occ_int, int_int)),
                int_duration = as.duration(int_int))

        occdf <- full_join(interval_,
                           occdf,
                           by = c("SignalID", "Phase",
                                  "CycleStart", "IntervalStart", "IntervalEnd")) %>%
            tidyr::replace_na(
                list(Detector = 0, occ_duration = 0, int_duration = 1)) %>%
            mutate(SignalID = factor(SignalID),
                   Detector = factor(Detector),
                   occ_duration = as.numeric(occ_duration),
                   int_duration = as.numeric(int_duration)) %>%

            group_by(SignalID, Phase, CycleStart, Detector) %>%
            summarize(occ = sum(occ_duration)/max(int_duration),
                      .groups = "drop_last") %>%

            summarize(occ = max(occ),
                      .groups = "drop") %>%

            mutate(SignalID = factor(SignalID),
                   Phase = factor(Phase))

        occdf
    }

    grn_occ <- get_occupancy(de_dt, gr_dt, grn_interval) %>%
        rename(gr_occ = occ)
    cat('.')
    sor_occ <- get_occupancy(de_dt, sr_dt, sor_interval) %>%
        rename(sr_occ = occ)
    cat('.\n')



    df <- full_join(grn_occ, sor_occ, by = c("SignalID", "Phase", "CycleStart")) %>%
        mutate(sf = if_else((gr_occ > 0.80) & (sr_occ > 0.80), 1, 0))

    # if a split failure on any phase
    df0 <- df %>% group_by(SignalID, Phase = factor(0), CycleStart) %>%
        summarize(sf = max(sf), .groups = "drop")

    sf <- bind_rows(df, df0) %>%
        mutate(Phase = factor(Phase)) %>%

        group_by(SignalID, Phase, hour = floor_date(CycleStart, unit = "hour")) %>%
        summarize(cycles = n(),
                  sf_freq = sum(sf, na.rm = TRUE)/cycles,
                  sf = sum(sf, na.rm = TRUE),
                  .groups = "drop") %>%

        transmute(SignalID,
                  CallPhase = Phase,
                  Date_Hour = ymd_hms(hour),
                  Date = date(Date_Hour),
                  DOW = wday(Date),
                  Week = week(Date),
                  sf = as.integer(sf),
                  cycles = cycles,
                  sf_freq = sf_freq)

    sf

    # SignalID | CallPhase | Date_Hour | Date | Hour | sf_freq | cycles
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

    # SignalID | CallPhase | Date | Week | DOW | Peak | sf_freq | cycles
}


# SPM Split Failures
get_sf <- function(df) {
    df %>% mutate(SignalID = factor(SignalID),
                  CallPhase = factor(Phase),
                  Date_Hour = lubridate::ymd_hms(Hour),
                  Date = date(Date_Hour),
                  DOW = wday(Date),
                  Week = week(Date)) %>%
        dplyr::select(SignalID, CallPhase, Date, Date_Hour, DOW, Week, sf, cycles, sf_freq) %>%
        as_tibble()

    # SignalID | CallPhase | Date | Date_Hour | DOW | Week | sf | cycles | sf_freq
}


# SPM Queue Spillback - updated 2/20/2020
get_qs <- function(detection_events) {

    qs <- detection_events %>%

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
                  CycleStart = cyclestart,
                  CallPhase = factor(callphase),
                  Detector = factor(detector),
                  occ)

    dates <- unique(qs$Date)

    # Get detector config for queue spillback.
    # get_det_config_qs2 filters for Advanced Count and Advanced Speed detector types
    # It also filters out bad detectors
    dc <- lapply(dates, get_det_config_qs) %>%
        bind_rows() %>%
        dplyr::select(Date, SignalID, CallPhase, Detector, TimeFromStopBar) %>%
        mutate(SignalID = factor(SignalID),
               CallPhase = factor(CallPhase))

    qs %>% left_join(dc, by=c("Date", "SignalID", "CallPhase", "Detector")) %>%
        filter(!is.na(TimeFromStopBar)) %>%

        # Date | SignalID | CycleStart | CallPhase | Detector | occ

        # -- data.tables. This is faster ---
        as.data.table %>%
            .[,.(occ = max(occ, na.rm = TRUE)),
              by = .(Date, SignalID, CallPhase, CycleStart)] %>%
            .[,.(qs = sum(occ > 3),
                      cycles = .N),
              by = .(Date, SignalID, Hour = floor_date(CycleStart, unit = 'hours'), CallPhase)] %>%
        # -- --------------------------- ---

        transmute(SignalID = factor(SignalID),
                  CallPhase = factor(CallPhase),
                  Date = date(Date),
                  Date_Hour = ymd_hms(Hour),
                  DOW = wday(Date),
                  Week = week(Date),
                  qs = as.integer(qs),
                  cycles = as.integer(cycles),
                  qs_freq = as.double(qs)/as.double(cycles)) %>% as_tibble()

    # SignalID | CallPhase | Date | Date_Hour | DOW | Week | qs | cycles | qs_freq
}





# -- Generic Aggregation Functions
get_Tuesdays <- function(df) {
    dates_ <- seq(min(df$Date) - days(6), max(df$Date) + days(6), by = "days")
    tuesdays <- dates_[wday(dates_) == 3]

    tuesdays <- pmax(min(df$Date), tuesdays) # unsure of this. need to test
    #tuesdays <- tuesdays[between(tuesdays, min(df$Date), max(df$Date))]

    data.frame(Week = week(tuesdays), Date = tuesdays)
}


weighted_mean_by_corridor_ <- function(df, per_, corridors, var_, wt_ = NULL) {

    per_ <- as.name(per_)

    gdf <- left_join(df, corridors) %>%
        mutate(Corridor = factor(Corridor)) %>%
        group_by(Zone_Group, Zone, Corridor, !!per_)

    if (is.null(wt_)) {
        gdf %>%
            summarize(!!var_ := mean(!!var_, na.rm = TRUE),
                      .groups = "drop_last") %>%
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(Zone_Group, Zone, Corridor, !!per_, !!var_, delta)
    } else {
        gdf %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE),
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last") %>%
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(Zone_Group, Zone, Corridor, !!per_, !!var_, !!wt_, delta)
    }
}


group_corridor_by_ <- function(df, per_, var_, wt_, corr_grp) {
    df %>%
        group_by(!!per_) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE),
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(Corridor = factor(corr_grp)) %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        mutate(Zone_Group = corr_grp) %>%
        dplyr::select(Corridor, Zone_Group, !!per_, !!var_, !!wt_, delta)
}


group_corridor_by_sum_ <- function(df, per_, var_, wt_, corr_grp) {
    df %>%
        group_by(!!per_) %>%
        summarize(!!var_ := sum(!!var_, na.rm = TRUE)) %>%
        mutate(Corridor = factor(corr_grp)) %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        mutate(Zone_Group = corr_grp) %>%
        dplyr::select(Corridor, Zone_Group, !!per_, !!var_, delta)
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

    # Get average for each Zone or District, according to corridors$Zone
    df_ <- df %>%
        split(df$Zone)
    all_zones <- lapply(names(df_), function(x) { gr_(df_[[x]], per_, var_, wt_, x) })

    # Get average for All RTOP (RTOP1 and RTOP2)
    all_rtop <- df %>%
        filter(Zone_Group %in% c("RTOP1", "RTOP2")) %>%
        gr_(per_, var_, wt_, "All RTOP")

    # Get average for RTOP1
    all_rtop1 <- df %>%
        filter(Zone_Group == "RTOP1") %>%
        gr_(per_, var_, wt_, "RTOP1")

    # Get average for RTOP2
    all_rtop2 <- df %>%
        filter(Zone_Group == "RTOP2") %>%
        gr_(per_, var_, wt_, "RTOP2")


    # Get average for All Zone 7 (Zone 7m and Zone 7d)
    all_zone7 <- df %>%
        filter(Zone %in% c("Zone 7m", "Zone 7d")) %>%
        gr_(per_, var_, wt_, "Zone 7")

    # concatenate all summaries with corridor averages
    dplyr::bind_rows(select(df, "Corridor", Zone_Group = "Zone", !!per_, !!var_, !!wt_, "delta"),
                     all_zones,
                     all_rtop,
                     all_rtop1,
                     all_rtop2,
                     all_zone7) %>%
        distinct() %>%
        mutate(Zone_Group = factor(Zone_Group),
        Corridor = factor(Corridor))
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
            filter(hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS))
    }

    df %>%
        group_by(SignalID, Date) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>% # Sum of phases 2,6
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(SignalID, Date, !!var_, !!wt_, delta)

    # SignalID | Date | var_ | wt_ | delta
}


get_daily_sum <- function(df, var_, per_) {

    var_ <- as.name(var_)
    per_ <- as.name(per_)


    df %>%
        complete(nesting(SignalID, CallPhase), !!var_ := full_seq(!!var_, 1)) %>%
        group_by(SignalID, !!per_) %>%
        summarize(!!var_ := sum(!!var_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(SignalID, !!per_, !!var_, delta)
}


get_weekly_sum_by_day <- function(df, var_) {

    var_ <- as.name(var_)

    Tuesdays <- get_Tuesdays(df)

    df %>%
        group_by(SignalID, Week, CallPhase) %>%
        summarize(!!var_ := mean(!!var_, na.rm = TRUE),
                  .groups = "drop_last") %>% # Mean over 3 days in the week
        summarize(!!var_ := sum(!!var_, na.rm = TRUE),
                  .groups = "drop_last") %>% # Sum of phases 2,6
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        left_join(Tuesdays, by = c("Week")) %>%
        dplyr::select(SignalID, Date, Week, !!var_, delta)
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
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop") %>% # Mean over 3 days in the week

        group_by(SignalID, Week) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>% # Sum of phases 2,6

        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        left_join(Tuesdays, by = c("Week")) %>%
        dplyr::select(SignalID, Date, Week, !!var_, !!wt_, delta) %>%
        ungroup()

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
    # if (wt_ == "ones") { ##--- this needs to be revisited. this should be added and this function should look like the weekly avg function above
    #     wt_ <- as.name(wt_)
    #     df <- mutate(df, !!wt_ := 1)
    # } else {
    #     wt_ <- as.name(wt_)
    # }

    if (peak_only == TRUE) {
        df <- df %>%
            filter((hour(Date_Hour) %in% c(AM_PEAK_HOURS, PM_PEAK_HOURS)))
    }

    df$Month <- df$Date
    day(df$Month) <- 1

    current_month <- max(df$Month)

    gdf <- df %>%
        group_by(SignalID, CallPhase) %>%
        complete(nesting(SignalID, CallPhase),
                 Month = seq(min(Month), current_month, by = "1 month")) %>%
        group_by(SignalID, Month, CallPhase)

    if (is.null(wt_)) {
        gdf %>%
            summarize(!!var_ := mean(!!var_, na.rm = TRUE),
                      .groups = "drop_last") %>%
            summarize(!!var_ := sum(!!var_, na.rm = TRUE),
                      .groups = "drop_last") %>% # Sum over Phases (2,6)
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(-lag_) %>%
            filter(!is.na(Month))
    } else {
        wt_ <- as.name(wt_)
        gdf %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE),
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last") %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean over Phases(2,6)
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last") %>%
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(-lag_) %>%
            filter(!is.na(Month))
    }
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

    df_ <- select(df, -Date) %>%
        left_join(Tuesdays, by = c("Week")) %>%
        filter(!is.na(Date))
    year(df_$Hour) <- year(df_$Date)
    month(df_$Hour) <- month(df_$Date)
    day(df_$Hour) <- day(df_$Date)

    if (is.null(wt_)) {
        df_ %>%
            group_by(SignalID, Week, Hour, CallPhase) %>%
            summarize(!!var_ := mean(!!var_, na.rm = TRUE),
                      .groups = "drop_last") %>% # Mean over 3 days in the week
            summarize(!!var_ := mean(!!var_, na.rm = TRUE),
                      .groups = "drop_last") %>% # Mean of phases 2,6
            mutate(lag_ = lag(!!var_),
                   delta = ((!!var_) - lag_)/lag_) %>%
            ungroup() %>%
            dplyr::select(SignalID, Hour, Week, !!var_, delta)
    } else {
        wt_ <- as.name(wt_)
        df_ %>%
            group_by(SignalID, Week, Hour, CallPhase) %>%
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE),
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last") %>% # Mean over 3 days in the week
            summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE), # Mean of phases 2,6
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last") %>% # Sum of phases 2,6
            mutate(lag_ = lag(!!var_),
                       delta = ((!!var_) - lag_)/lag_) %>%
                ungroup() %>%
            dplyr::select(SignalID, Hour, Week, !!var_, !!wt_, delta)
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
        summarize(!!var_ := sum(!!var_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(-lag_)

    # SignalID | CallPhase | Week | DOW | Hour | var_ | delta

} ## unused. untested


get_avg_by_hr <- function(df, var_, wt_ = NULL) {

    df_ <- as.data.table(df)

    df_[, c("DOW", "Week", "Hour") := list(wday(Date_Hour),
                                           week(date(Date_Hour)),
                                           floor_date(Date_Hour, unit = '1 hour'))]
    if (is.null(wt_)) {
        ret <- df_[, .(mean(get(var_)), 1),
                   by = .(SignalID, CallPhase, Week, DOW, Hour)]
        ret <- ret[, c((var_), (wt_)) := .(V1, V2)][, -(V1:V2)]
        as_tibble(ret)
    } else {
        ret <- df_[, .(weighted.mean(get(var_), get(wt_), na.rm = TRUE),
                       sum(get(wt_), na.rm = TRUE)),
                   by = .(SignalID, CallPhase, Week, DOW, Hour)]
        ret <- ret[, c((var_), (wt_)) := .(V1, V2)][, -(V1:V2)]
        ret[, delta := (get(var_) - shift(get(var_), 1, type = "lag"))/shift(get(var_), 1, type = "lag"),
            by = .(SignalID, CallPhase)]
        as_tibble(ret)
    }
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
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(!!var_ := weighted.mean(!!var_, !!wt_, na.rm = TRUE),
                  !!wt_ := sum(!!wt_, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(-lag_)

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


# Used to get peak period metrics from hourly results, i.e., peak/off-peak split failures
summarize_by_peak <- function(df, date_col) {

    date_col_ <- as.name(date_col)

    df %>%
        mutate(Peak = if_else(hour(!!date_col_) %in% AM_PEAK_HOURS | hour(!!date_col_) %in% PM_PEAK_HOURS,
                              "Peak",
                              "Off_Peak"),
               Peak = factor(Peak)) %>%
        split(.$Peak) %>%
        lapply(function(x) {
            x %>%
                group_by(SignalID, Period = date(!!date_col_), Peak) %>%
                 summarize(sf_freq = weighted.mean(sf_freq, cycles),
                          cycles = sum(cycles),
                          .groups = "drop") %>%
                select(-Peak)
        })
}



# -- end Generic Aggregation Functions


get_daily_detector_uptime <- function(filtered_counts) {

    usable_cores <- get_usable_cores()

    # this seems ripe for optimization
    bad_comms <- filtered_counts %>%
        group_by(SignalID, Timeperiod) %>%
        summarize(vol = sum(vol, na.rm = TRUE),
                  .groups = "drop") %>%
        dplyr::filter(vol == 0) %>%
        dplyr::select(-vol)
    fc <- anti_join(filtered_counts, bad_comms, by = c("SignalID", "Timeperiod")) %>%
        ungroup()

    ddu <- fc %>%
        mutate(Date_Hour = Timeperiod,
               Date = date(Date_Hour)) %>%
        dplyr::select(SignalID, CallPhase, Detector, Date, Date_Hour, Good_Day) %>%
        ungroup() %>%
        mutate(setback = ifelse(CallPhase %in% c(2,6), "Setback", "Presence"),
               setback = factor(setback),
               SignalID = factor(SignalID)) %>%
        split(.$setback) %>% lapply(function(x) {
            x %>%
                group_by(SignalID, CallPhase, Date, Date_Hour, setback) %>%
                summarize(uptime = sum(Good_Day, na.rm = TRUE),
                          all = n(),
                          .groups = "drop") %>%
                mutate(uptime = uptime/all,
                       all = as.double(all))
        })
    ddu
}


get_avg_daily_detector_uptime <- function(ddu) {

    sb_daily_uptime <- get_daily_avg(filter(ddu, setback == "Setback"),
                                     "uptime", "all",
                                     peak_only = FALSE)
    pr_daily_uptime <- get_daily_avg(filter(ddu, setback == "Presence"),
                                     "uptime", "all",
                                     peak_only = FALSE)
    all_daily_uptime <- get_daily_avg(ddu,
                                      "uptime", "all",
                                      peak_only = FALSE)

    sb_pr <- full_join(sb_daily_uptime, pr_daily_uptime,
                       by = c("SignalID", "Date"),
                       suffix = c(".sb", ".pr"))

    full_join(all_daily_uptime, sb_pr,
              by = c("SignalID", "Date")) %>%
        dplyr::select(-starts_with("delta.")) %>%
        rename(uptime = uptime)
}


get_cor_avg_daily_detector_uptime <- function(avg_daily_detector_uptime, corridors) {

    cor_daily_sb_uptime %<-% (avg_daily_detector_uptime %>%
        filter(!is.na(uptime.sb)) %>%
        get_cor_weekly_avg_by_day(corridors, "uptime.sb", "all.sb"))

    cor_daily_pr_uptime %<-% (avg_daily_detector_uptime %>%
        filter(!is.na(uptime.pr)) %>%
        get_cor_weekly_avg_by_day(corridors, "uptime.pr", "all.pr"))

    cor_daily_all_uptime %<-% (avg_daily_detector_uptime %>%
        filter(!is.na(uptime)) %>%
        # average corridor by ones instead of all:
        #  to treat all signals equally instead of weighted by # of detectors
        get_cor_weekly_avg_by_day(corridors, "uptime", "ones"))

    full_join(dplyr::select(cor_daily_sb_uptime, -c(all.sb, delta)),
              dplyr::select(cor_daily_pr_uptime, -c(all.pr, delta)),
              by = c("Zone_Group", "Corridor", "Date", "Week")) %>%
        left_join(dplyr::select(cor_daily_all_uptime, -c(ones, delta)),
        by = c("Zone_Group", "Corridor", "Date", "Week")) %>%
        mutate(Zone_Group = factor(Zone_Group))
}


get_weekly_vpd <- function(vpd) {
    vpd <- filter(vpd, DOW %in% c(TUE,WED,THU))
    get_weekly_sum_by_day(vpd, "vpd")
}


get_weekly_papd <- function(papd) {
    papd <- filter(papd, DOW %in% c(TUE,WED,THU))
    get_weekly_sum_by_day(papd, "papd")
}


get_weekly_thruput <- function(throughput) {
    get_weekly_sum_by_day(throughput, "vph")
}


get_weekly_aog_by_day <- function(daily_aog) {
    get_weekly_avg_by_day(daily_aog, "aog", "vol", peak_only = TRUE)
}


get_weekly_pr_by_day <- function(daily_pr) {
    get_weekly_avg_by_day(daily_pr, "pr", "vol", peak_only = TRUE)
}


get_weekly_sf_by_day <- function(sf) {
    get_weekly_avg_by_day(sf, "sf_freq", "cycles", peak_only = TRUE)
}


get_weekly_qs_by_day <- function(qs) {
    get_weekly_avg_by_day(qs, "qs_freq", "cycles", peak_only = TRUE)
}


get_weekly_detector_uptime <- function(avg_daily_detector_uptime) {
    avg_daily_detector_uptime %>%
        mutate(CallPhase = 0, Week = week(Date)) %>%
        get_weekly_avg_by_day("uptime", "all", peak_only = FALSE) %>%
        replace_na(list(uptime = 0)) %>%
        arrange(SignalID, Date)
}


get_cor_weekly_vpd <- function(weekly_vpd, corridors) {
    get_cor_weekly_avg_by_day(weekly_vpd, corridors, "vpd")
}


get_cor_weekly_papd <- function(weekly_papd, corridors) {
    get_cor_weekly_avg_by_day(weekly_papd, corridors, "papd")
}


get_cor_weekly_thruput <- function(weekly_throughput, corridors) {
    get_cor_weekly_avg_by_day(weekly_throughput, corridors, "vph")
}


get_cor_weekly_aog_by_day <- function(weekly_aog, corridors) {
    get_cor_weekly_avg_by_day(weekly_aog, corridors, "aog", "vol")
}


get_cor_weekly_pr_by_day <- function(weekly_pr, corridors) {
    get_cor_weekly_avg_by_day(weekly_pr, corridors, "pr", "vol")
}


get_cor_weekly_sf_by_day <- function(weekly_sf, corridors) {
    get_cor_weekly_avg_by_day(weekly_sf, corridors, "sf_freq", "cycles")
}


get_cor_weekly_qs_by_day <- function(weekly_qs, corridors) {
    get_cor_weekly_avg_by_day(weekly_qs, corridors, "qs_freq", "cycles")
}


get_cor_weekly_detector_uptime <- function(avg_weekly_detector_uptime, corridors) {
    get_cor_weekly_avg_by_day(avg_weekly_detector_uptime, corridors, "uptime", "all")
}


get_monthly_vpd <- function(vpd) {
    vpd <- filter(vpd, DOW %in% c(TUE,WED,THU))
    get_monthly_avg_by_day(vpd, "vpd", peak_only = FALSE)
}


get_monthly_flashevent <- function(flash) {
    flash <- flash %>%
        select(SignalID, Date)

    day(flash$Date) <- 1

    flash <- flash %>%
        group_by(SignalID, Date) %>%
        summarize(flash = n(), .groups = "drop")

    flash$CallPhase = 0 # set the dummy, 'CallPhase' is used in get_monthly_avg_by_day() function
    get_monthly_avg_by_day(flash, "flash", peak_only = FALSE)
}


get_monthly_papd <- function(papd) {
    papd <- filter(papd, DOW %in% c(TUE,WED,THU))
    get_monthly_avg_by_day(papd, "papd", peak_only = FALSE)
}


get_monthly_thruput <- function(throughput) {
    get_monthly_avg_by_day(throughput, "vph", peak_only = FALSE)
}


get_monthly_aog_by_day <- function(daily_aog) {
    get_monthly_avg_by_day(daily_aog, "aog", "vol", peak_only = TRUE)
}


get_monthly_pr_by_day <- function(daily_pr) {
    get_monthly_avg_by_day(daily_pr, "pr", "vol", peak_only = TRUE)
}


get_monthly_sf_by_day <- function(sf) {
    get_monthly_avg_by_day(sf, "sf_freq", "cycles", peak_only = TRUE)
}


get_monthly_qs_by_day <- function(qs) {
    get_monthly_avg_by_day(qs, "qs_freq", "cycles", peak_only = TRUE)
}


get_monthly_detector_uptime <- function(avg_daily_detector_uptime) {
    avg_daily_detector_uptime %>%
        mutate(CallPhase = 0) %>%
        get_monthly_avg_by_day("uptime", "all") %>%
        arrange(SignalID, Month)
}


get_cor_monthly_vpd <- function(monthly_vpd, corridors) {
    get_cor_monthly_avg_by_day(monthly_vpd, corridors, "vpd")
}

get_cor_monthly_flash <- function(monthly_flash, corridors) {
    get_cor_monthly_avg_by_day(monthly_flash, corridors, "flash")
}


get_cor_monthly_papd <- function(monthly_papd, corridors) {
    get_cor_monthly_avg_by_day(monthly_papd, corridors, "papd")
}


get_cor_monthly_thruput <- function(monthly_throughput, corridors) {
    get_cor_monthly_avg_by_day(monthly_throughput, corridors, "vph")
}


get_cor_monthly_aog_by_day <- function(monthly_aog_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_aog_by_day, corridors, "aog", "vol")
}


get_cor_monthly_pr_by_day <- function(monthly_pr_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_pr_by_day, corridors, "pr", "vol")
}


get_cor_monthly_sf_by_day <- function(monthly_sf_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_sf_by_day, corridors, "sf_freq", "cycles")
}


get_cor_monthly_qs_by_day <- function(monthly_qs_by_day, corridors) {
    get_cor_monthly_avg_by_day(monthly_qs_by_day, corridors, "qs_freq", "cycles")
}


get_cor_monthly_detector_uptime <- function(avg_daily_detector_uptime, corridors) {

    cor_daily_sb_uptime %<-% (avg_daily_detector_uptime %>%
        filter(!is.na(uptime.sb)) %>%
        mutate(Month = Date - days(day(Date) - 1)) %>%
        get_cor_monthly_avg_by_day(corridors, "uptime.sb", "all.sb"))
    cor_daily_pr_uptime %<-% (avg_daily_detector_uptime %>%
        filter(!is.na(uptime.pr)) %>%
        mutate(Month = Date - days(day(Date) - 1)) %>%
        get_cor_monthly_avg_by_day(corridors, "uptime.pr", "all.pr"))
    cor_daily_all_uptime %<-% (avg_daily_detector_uptime %>%
        filter(!is.na(uptime)) %>%
        mutate(Month = Date - days(day(Date) - 1)) %>%
        get_cor_monthly_avg_by_day(corridors, "uptime", "all"))

    full_join(dplyr::select(cor_daily_sb_uptime, -c(all.sb, delta)),
              dplyr::select(cor_daily_pr_uptime, -c(all.pr, delta)),
              by = c("Zone_Group", "Corridor", "Month")) %>%
        left_join(dplyr::select(cor_daily_all_uptime, -c(all)),
                  by = c("Zone_Group", "Corridor", "Month")) %>%
        mutate(Corridor = factor(Corridor),
               Zone_Group = factor(Zone_Group))
}


get_vph <- function(counts, mainline_only = TRUE) {

    if (mainline_only == TRUE) {
        counts <- counts %>%
            filter(CallPhase %in% c(2,6)) # sum over Phases 2,6
    }
    df <- counts %>%
        mutate(Date_Hour = floor_date(Timeperiod, "1 hour"))
    get_sum_by_hr(df, "vol") %>%
        group_by(SignalID, Week, DOW, Hour) %>%
        summarize(vph = sum(vol, na.rm = TRUE),
                  .groups = "drop")

    # SignalID | CallPhase | Week | DOW | Hour | aog | vph
}


get_aog_by_hr <- function(aog) {
    get_avg_by_hr(aog, "aog", "vol")
}


get_pr_by_hr <- function(aog) {
    get_avg_by_hr(aog, "pr", "vol")
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
        group_by(SignalID, Hour) %>%
        summarize(vph = sum(vph, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(vph = mean(vph, na.rm = TRUE),
                  .groups = "drop")
}


get_monthly_paph <- function(paph) {
    paph %>%
        rename(vph = paph) %>%
        get_monthly_vph() %>%
        rename(paph = vph)
}


get_monthly_aog_by_hr <- function(aog_by_hr) {
    aog_by_hr %>%
        group_by(SignalID, Hour) %>%
        summarize(aog = weighted.mean(aog, vol, na.rm = TRUE),
                  vol = sum(vol, na.rm = TRUE),
                  .groups = "drop_last") %>%
        mutate(Hour = Hour - days(day(Hour)) + days(1)) %>%
        group_by(SignalID, Hour) %>%
        summarize(aog = weighted.mean(aog, vol, na.rm = TRUE),
                  vol = sum(vol, na.rm = TRUE),
                  .groups = "drop")
}


get_monthly_pr_by_hr <- function(pr_by_hr) {
    get_monthly_aog_by_hr(rename(pr_by_hr, aog = pr)) %>%
        rename(pr = aog)
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
}


get_cor_monthly_paph <- function(monthly_paph, corridors) {
    get_cor_monthly_avg_by_hr(monthly_paph, corridors, "paph")
}


get_cor_monthly_aog_by_hr <- function(monthly_aog_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_aog_by_hr, corridors, "aog", "vol")
}


get_cor_monthly_pr_by_hr <- function(monthly_pr_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_pr_by_hr, corridors, "pr", "vol")
}


get_cor_monthly_sf_by_hr <- function(monthly_sf_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_sf_by_hr, corridors, "sf_freq", "cycles")
}


get_cor_monthly_qs_by_hr <- function(monthly_qs_by_hr, corridors) {
    get_cor_monthly_avg_by_hr(monthly_qs_by_hr, corridors, "qs_freq", "cycles")
}


get_cor_monthly_ti <- function(ti, cor_monthly_vph, corridors) {

    # Get share of volume (as pct) by hour over the day, for whole dataset
    day_dist <- cor_monthly_vph %>%
        group_by(Zone_Group, Zone, Corridor, month(Hour)) %>%
        mutate(pct = vph/sum(vph, na.rm = TRUE)) %>%
        group_by(Zone_Group, Zone, Corridor, hr = hour(Hour)) %>%
        summarize(pct = mean(pct, na.rm = TRUE),
                  .groups = "drop_last")

    left_join(ti,
              distinct(corridors, Zone_Group, Zone, Corridor),
              by = c("Zone_Group", "Zone", "Corridor")) %>%
        mutate(hr = hour(Hour)) %>%
        left_join(day_dist) %>%
        ungroup() %>%
        tidyr::replace_na(list(pct = 1))
}


get_cor_monthly_ti_by_hr <- function(ti, cor_monthly_vph, corridors) {

    df <- get_cor_monthly_ti(ti, cor_monthly_vph, corridors)

    if ("tti" %in% colnames(ti)) {
        tindx = "tti"
    } else if ("pti" %in% colnames(ti)) {
        tindx = "pti"
    } else if ("bi" %in% colnames(ti)) {
        tindx = "bi"
    } else if ("speed_mph" %in% colnames(ti)) {
        tindx = "speed_mph"
    } else {
        tindx = "oops"
    }

    get_cor_monthly_avg_by_hr(df, corridors, tindx, "pct")
}


get_cor_monthly_ti_by_day <- function(ti, cor_monthly_vph, corridors) {

    df <- get_cor_monthly_ti(ti, cor_monthly_vph, corridors)

    if ("tti" %in% colnames(ti)) {
        tindx = "tti"
    } else if ("pti" %in% colnames(ti)) {
        tindx = "pti"
    } else if ("bi" %in% colnames(ti)) {
        tindx = "bi"
    } else if ("speed_mph" %in% colnames(ti)) {
        tindx = "speed_mph"
    } else {
        tindx = "oops"
    }

    df %>%
        mutate(Month = as_date(Hour)) %>%
        get_cor_monthly_avg_by_day(corridors, tindx, "pct")
}


get_weekly_vph <- function(vph) {
    vph <- filter(vph, DOW %in% c(TUE,WED,THU))
    get_weekly_avg_by_hr(vph, "vph")
}


get_weekly_paph <- function(paph) {
    paph <- filter(paph, DOW %in% c(TUE,WED,THU))
    get_weekly_avg_by_hr(paph, "paph")
}


get_cor_weekly_vph <- function(weekly_vph, corridors) {
    get_cor_weekly_avg_by_hr(weekly_vph, corridors, "vph")
}


get_cor_weekly_paph <- function(weekly_paph, corridors) {
    get_cor_weekly_avg_by_hr(weekly_paph, corridors, "paph")
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
        weighted_mean_by_corridor_("Month", corridors, as.name("vph")) %>%
        select(-Zone)

    pm <- dplyr::filter(cor_monthly_vph, hour(Hour) %in% PM_PEAK_HOURS) %>%
        mutate(Month = date(Hour)) %>%
        weighted_mean_by_corridor_("Month", corridors, as.name("vph")) %>%
        select(-Zone)

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
        dplyr::select(-Date) %>%
        left_join(Tuesdays) %>%
        group_by(Date, Corridor, Zone_Group) %>%
        summarize(up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE),
                  uptime = sum(up, na.rm = TRUE)/sum(num, na.rm = TRUE),
                  .groups = "drop")
}


get_cor_monthly_cctv_uptime <- function(daily_cctv_uptime) {

    daily_cctv_uptime %>%
        mutate(Month = Date - days(day(Date) - 1)) %>%
        group_by(Month, Corridor, Zone_Group) %>%
        summarize(up = sum(up, na.rm = TRUE),
                  num = sum(num, na.rm = TRUE),
                  uptime = sum(up, na.rm = TRUE)/sum(num, na.rm = TRUE),
                  .groups = "drop")
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
                      !!wt_ := sum(!!wt_, na.rm = TRUE),
                      .groups = "drop_last")
    } else if (operation == "sum") {
        quarterly_df <- quarterly_df %>%
            summarize(!!var_ := sum(!!var_, na.rm = TRUE),
                      .groups = "drop_last")
    } else if (operation == "latest") {
        quarterly_df <- monthly_df %>%
            group_by(Corridor,
                     Zone_Group,
                     Quarter = as.character(lubridate::quarter(Month, with_year = TRUE))) %>%
            filter(Month == max(Month)) %>%
            dplyr::select(Corridor,
                          Zone_Group,
                          Quarter,
                          !!var_,
                          !!wt_) %>%
            group_by(Corridor, Zone_Group) %>%
            arrange(Zone_Group, Corridor, Quarter)
    }

    quarterly_df %>%
        mutate(lag_ = lag(!!var_),
               delta = ((!!var_) - lag_)/lag_) %>%
        ungroup() %>%
        dplyr::select(-lag_)
}

readRDS_multiple <- function(pattern) {
    lf <- list.files(pattern = pattern)
    lf <- lf[grepl(".rds", lf)]
    bind_rows(lapply(lf, readRDS))
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

# get_pau_high <- split_wrapper(get_pau_high_)



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
    # the number of acutations is more than 100 times the 80% percentile
    # number of daily presses for that pushbutton input
    # (i.e., it's a [high] outlier for that input)
    # - or -
    # between midnight and 6am, there is at least one hour in a day with
    # at least 300 actuations or at least three hours with over 60
    # (i.e., it is an outlier based on what would be expected
    # for any input in the early morning hours)

    too_high <- get_pau_high_(paph, wk_calcs_start_date)  # get_pau_high(paph, 200, wk_calcs_start_date)
    gc()

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


dbUpdateTable <- function(conn, table_name, df, asof = NULL) {
    # per is Month|Date|Hour|Quarter
    if ("Date" %in% names(df)) {
        per = "Date"
    } else if ("Hour" %in% names(df)) {
        per = "Hour"
    } else if ("Month" %in% names(df)) {
        per = "Month"
    } else if ("Quarter" %in% names(df)) {
        per = "Quarter"
    }

    if (!is.null(asof)) {
        df <- df[df[[per]] >= asof,]
    }
    df <- df[!is.na(df[[per]]),]

    min_per <- min(df[[per]])
    max_per <- max(df[[per]])

    count_query <- sql(glue("SELECT COUNT(*) FROM {table_name} WHERE {per} >= '{min_per}'"))
    num_del <- dbGetQuery(conn, count_query)[1,1]

    delete_query <- sql(glue("DELETE FROM {table_name} WHERE {per} >= '{min_per}'"))
    print(delete_query)
    print(glue("{num_del} records being deleted."))

    dbSendQuery(conn, delete_query)
    RMySQL::dbWriteTable(conn, table_name, df, append = TRUE, row.names = FALSE)
    print(glue("{nrow(df)} records uploaded."))
}



get_corridor_summary_data <- function(cor) {

    #' Converts cor data set to a single data frame for the current_month
    #' for use in get_corridor_summary_table function
    #'
    #' @param cor cor data
    #' @param current_month Current month in date format
    #' @return A data frame, monthly data of all metrics by Zone and Corridor


    data <- list(
        rename(cor$mo$du, du = uptime, du.delta = delta), # detector uptime - note that zone group is factor not character
        rename(cor$mo$pau, pau = uptime, pau.delta = delta),
        rename(cor$mo$cu, cu = uptime, cu.delta = delta),
        rename(cor$mo$tp, tp = vph, tp.delta = delta),
        rename(cor$mo$aogd, aog.delta = delta),
        rename(cor$mo$prd, pr.delta = delta),
        rename(cor$mo$qsd, qs = qs_freq, qs.delta = delta),
        rename(cor$mo$sfd, sf = sf_freq, sf.delta = delta),
        rename(cor$mo$tti, tti.delta = delta),
        rename(cor$mo$pti, pti.delta = delta)
    ) %>%
        reduce(left_join, by = c("Zone_Group", "Corridor", "Month")) %>%
        select(
            -uptime.sb,
            -uptime.pr,
            -starts_with("ones"),
            -starts_with("cycles"),
            -starts_with("pct"),
            -starts_with("vol"),
            -starts_with("Description")
        )
    return(data)
}


get_ped_delay <- function(date_, conf, signals_list, parallel = FALSE) {

    get_ped_events_one_signal <- function(signalid, date_, conf) {

        s3bucket = conf$bucket
        s3object = glue("atspm/date={date_}/atspm_{signalid}_{date_}.parquet")

        object_exists <- length(aws.s3::get_bucket(s3bucket, s3object))

        if (object_exists) {
            cat('.')
            tryCatch({
                # all 45/21/22/132 events
                pe <- s3_read_parquet(bucket = s3bucket, object = s3object) %>%
                    filter(EventCode %in% (c(45, 21, 22, 132))) %>%
                    mutate(CycleLength = ifelse(EventCode == 132, EventParam, NA)) %>%
                    arrange(Timestamp) %>%
                    tidyr::fill(CycleLength) %>%
                    rename(Phase = EventParam)
            }, error = function(e) {
                print(e)
            })
        }
    }


    ncores <- if (parallel & Sys.info()["sysname"] != "Windows") {
        usable_cores * 3
    } else {
        1
    }

    pe <- mclapply(signals_list, mc.cores = ncores, FUN = function(signalid) {
            get_ped_events_one_signal(signalid, date_, conf)
        }) %>% bind_rows()


    coord.type <- group_by(pe, SignalID) %>%
        summarise(CL = max(CycleLength, na.rm = T), .groups = "drop") %>%
        mutate(Pattern = ifelse( (CL == 0 | !is.finite(CL)), "Free", "Coordinated"))

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

    pe.free.summary <- filter(pe, Pattern == "Free") %>%
        group_by(SignalID) %>%
        summarise(
            Pattern = "Free",
            Avg.Max.Ped.Delay = sum(Delay) / n(),
            Events = n(),
            .groups = "drop"
        )

    pe.coordinated.summary.byphase <- filter(pe, Pattern == "Coordinated") %>%
        group_by(SignalID, Phase) %>%
        summarise(
            Pattern = "Coordinated",
            Max.Ped.Delay.per.Cycle = sum(Delay) / n(),
            Events = n(),
            .groups = "drop"
        )

    pe.coordinated.summary <- pe.coordinated.summary.byphase %>%
        group_by(SignalID) %>%
        summarise(
            Pattern = "Coordinated",
            Avg.Max.Ped.Delay = weighted.mean(Max.Ped.Delay.per.Cycle,Events),
            Events = sum(Events),
            .groups = "drop"
        )

    pe.summary.overall <- bind_rows(pe.free.summary, pe.coordinated.summary) %>%
        mutate(Date = date_)

    return(pe.summary.overall)
    
}





get_ped_delay_s3 <- function(date_, conf) {
    
    conn <- get_athena_connection(conf$athena)
    
    # Read in ped detector/phase configuration file - unique for each day
    ped_config <- s3read_using(
        read_csv, 
        bucket = "gdot-devices", 
        object = glue("maxtime_ped_plans/date={date_}/MaxTime_Ped_Plans.csv")
    ) %>% 
        select(-X1) %>%
        mutate(SignalID = factor(SignalID))
    
    ## Testing
    pe <- tbl(conn, sql(glue("select distinct * from {conf$athena$database}.{conf$athena$atspm_table} where date = '{date_}'"))) %>%
        filter(
            eventcode %in% c(21, 22, 90, 132)
        ) %>%
        distinct(
            timestamp, signalid, eventcode, eventparam
        ) %>%
        collect() %>%
        transmute(
            Timestamp = timestamp,
            SignalID = factor(signalid),
            EventCode = eventcode,
            EventParam = eventparam
        ) %>%
        arrange(SignalID, Timestamp) %>% 
        mutate(
            CycleLength = ifelse(EventCode == 132, EventParam, NA)
        ) %>%
        group_by(SignalID) %>%
        tidyr::fill(CycleLength) %>%
        ungroup() %>%
        filter(EventCode != 132)
    
    dbDisconnect(conn)
    
    
    # figure out corresponding phase for each event code
    # group 1 - if 21 (ped walk on)
    #           or 22 (ped clearance/FDW on)
    #       -> event parameter IS the phase (create new column called phase)
    pe1 <- filter(pe, EventCode %in% c(21,22)) %>% 
        mutate(Phase = EventParam)
    
    # group 2 - if 45 (ped call registered)
    #           or 90 (ped detector on)
    #       -> event parameter is the detector ID
    # first join to ped config table on (signalid == signalid, eventparam == detector)
    pe2 <- filter(pe, EventCode == 90) %>%
        left_join(
            ped_config, 
            by = c("SignalID" = "SignalID", 
                   "EventParam" = "Detector")) %>%
        mutate(
            Phase = ifelse(!is.na(CallPhase), CallPhase, EventParam)) %>%
        select(
            Timestamp, 
            SignalID, 
            EventCode, 
            EventParam,
            CycleLength,
            Phase)
    
    #union/rbind these tables back together
    pe <- bind_rows(pe1, pe2) %>%
        arrange(SignalID, Phase, Timestamp)
    rm(pe1)
    rm(pe2)
    
    # Calculate Delay for Each Ped Pushbutton Event
    pe <- pe %>% 
        group_by(SignalID, Phase) %>% 
        
        # Remove repeats of the same event code
        mutate(run = runner::streak_run(EventCode)) %>%
        filter(run == 1 | EventCode != 90) %>%  
        select(-run) %>%
        
        mutate(
            lag_eventcode = lag(EventCode), 
            lead_eventcode = lead(EventCode),
            lead_timestamp = lead(Timestamp)) %>% 
        select(
            Timestamp, 
            SignalID, 
            EventParam,
            CycleLength,
            Phase, 
            lag_eventcode, 
            EventCode, 
            lead_eventcode,
            lead_timestamp) %>% 
        
        # Define a sequence code for event triples (e.g., "90_21_22")
        mutate(
            sequence = paste(as.character(lag_eventcode), 
                             as.character(EventCode), 
                             as.character(lead_eventcode), 
                             sep = "_")) %>%
        #write.csv(pe, "ped_data_sequenced.csv")
        
        filter(
            sequence %in% c("NA_90_21", "22_90_21", "21_90_22", "22_90_22", 
                            "21_90_21", "90_21_22", "22_21_22")
        ) %>%
        
        # Calculate duration between subsequent (2nd and 3rd) events based on sequence triple
        mutate(delay = as.numeric(difftime(lead_timestamp, Timestamp), units="secs")) %>%
        mutate(delay = if_else(sequence == "21_90_22", 0, delay)) %>%
        
        ungroup() %>%
        filter(delay > -1) %>%
        select(Timestamp,
               SignalID,
               CallPhase = Phase,
               CycleLength,
               Sequence = sequence,
               Delay = delay)
    
    
    # 21-22 events - to get walk times and cycle lengths
    pe.walktimes <- pe %>%
        filter(Sequence %in% c("22_21_22", "90_21_22")) %>%
        rename(WalkTime = Delay)
    
    pe.walktimes.byphase <- pe.walktimes %>%
        group_by(SignalID, CallPhase) %>% 
        summarise(WalkTime = which.max(tabulate(WalkTime))) #takes most frequently occurring value
    
    
    # Now go back and resolve issues
    if(nrow(pe) > 0) {
        
        pe.updated <- pe %>%
            # Filter out the sequences that were only used to get walk times
            filter(!(Sequence %in% c("22_21_22", "90_21_22"))) %>% 
            left_join(
                pe.walktimes.byphase, 
                by = c("SignalID", "CallPhase")) %>%
            
            # Subtract out walktime for Case 3, make sure we don't go below 0
            mutate(
                Delay.Updated = ifelse(
                    Sequence == "22_90_22", 
                    pmax(Delay - ifelse(is.na(WalkTime), 0, WalkTime), 0), 
                    Delay) 
            ) %>%
            
            # Mike's 1st issue - long ped delay events
            mutate(
                Delay.Updated = case_when(
                    
                    # delay capped at 2*180 if cycle length is 0 or NA
                    (is.na(CycleLength) | CycleLength == 0) ~ pmin(Delay.Updated, 360),
                    
                    # delay for events with non-NA cycle lengths capped at 2*CL
                    (Delay.Updated > 2*CycleLength) ~ 2*CycleLength, 
                    
                    TRUE ~ Delay.Updated
                )
            ) %>%
            
            # Mike's 2nd issue - short delay events
            mutate(
                Delay.Updated = ifelse(
                    Sequence == "22_90_21" & Delay > 0 & Delay < 2, 
                    NA, 
                    Delay.Updated)  
            )
        
        df <- pe.updated %>%
            transmute(
                Date = date_,
                Timestamp,
                SignalID,
                CallPhase = as.integer(CallPhase),
                pd = Delay.Updated
            )
        
    } else {
        
        df <- data.frame(
            Date = as.Date(character()),
            Timestamp = as_datetime(character()),
            SignalID = integer(),
            CallPhase = integer(),
            pd = double()
        )
    }
    
}



write_signal_details <- function(plot_date, conf_athena, signals_list = NULL) {
    print(plot_date)
    #--- This takes approx one minute per day -----------------------
    rc <- s3_read_parquet(
        bucket = conf$bucket, 
        object = glue("mark/counts_1hr/date={plot_date}/counts_1hr_{plot_date}.parquet")) %>% 
        convert_to_utc() %>%
        select(
            SignalID, Date, Timeperiod, Detector, CallPhase, vol)
    
    fc <- s3_read_parquet(
        bucket = conf$bucket, 
        object = glue("mark/filtered_counts_1hr/date={plot_date}/filtered_counts_1hr_{plot_date}.parquet")) %>% 
        convert_to_utc() %>%
        select(
            SignalID, Date, Timeperiod, Detector, CallPhase, Good_Day)
    
    ac <- s3_read_parquet(
        bucket = conf$bucket, 
        object = glue("mark/adjusted_counts_1hr/date={plot_date}/adjusted_counts_1hr_{plot_date}.parquet")) %>% 
        convert_to_utc() %>%
        select(
            SignalID, Date, Timeperiod, Detector, CallPhase, vol)
    
    if (!is.null(signals_list)) { 
        rc <- rc %>% 
            filter(as.character(SignalID) %in% signals_list)
        fc <- fc %>% 
            filter(as.character(SignalID) %in% signals_list)
        ac <- ac %>% 
            filter(as.character(SignalID) %in% signals_list)
    }
    
    df <- list(
        rename(rc, vol_rc = vol),
        fc,
        rename(ac, vol_ac = vol)) %>%
        reduce(full_join, by = c("SignalID", "Date", "Timeperiod", "Detector", "CallPhase")
        ) %>%
        mutate(bad_day = if_else(Good_Day==0, TRUE, FALSE)) %>% 
        transmute(
            SignalID = factor(SignalID), 
            Timeperiod = Timeperiod, 
            Detector = factor(as.integer(Detector)), 
            CallPhase = factor(CallPhase),
            vol_rc = as.integer(vol_rc),
            vol_ac = ifelse(bad_day, as.integer(vol_ac), NA),
            bad_day) %>%
        arrange(SignalID, Detector, Timeperiod)
    #----------------------------------------------------------------
    
    df <- df %>% 
        nest(data = -c(SignalID, Timeperiod))
    df$data <- sapply(df$data, rjson::toJSON)
    df <- df %>% 
        spread(SignalID, data)
    
    s3write_using(
        df, 
        write_parquet, 
        use_deprecated_int96_timestamps = TRUE,
        bucket = conf$bucket, 
        object = glue("mark/signal_details/date={plot_date}/sg_{plot_date}.parquet"),
        opts = list(multipart=TRUE))
    
    conn <- get_athena_connection(conf_athena)
    table_name <- "signal_details"
    tryCatch({
        response <- dbGetQuery(conn,
                               sql(glue(paste("ALTER TABLE {conf_athena$database}.{table_name}",
                                              "ADD PARTITION (date='{plot_date}')"))))
        print(glue("Successfully created partition (date='{plot_date}') for {conf_athena$database}.{table_name}"))
    }, error = function(e) {
        message <- e
    })
    dbDisconnect(conn)
}




points_to_line <- function(data, long, lat, id_field = NULL, sort_field = NULL) {
    
    # Convert to SpatialPointsDataFrame
    coordinates(data) <- c(long, lat)
    
    # If there is a sort field...
    if (!is.null(sort_field)) {
        if (!is.null(id_field)) {
            data <- data[order(data[[id_field]], data[[sort_field]]), ]
        } else {
            data <- data[order(data[[sort_field]]), ]
        }
    }
    
    # If there is only one path...
    if (is.null(id_field)) {
        
        lines <- SpatialLines(list(Lines(list(Line(data)), "id")))
        
        return(lines)
        
        # Now, if we have multiple lines...
    } else if (!is.null(id_field)) {  
        
        # Split into a list by ID field
        paths <- sp::split(data, data[[id_field]])
        
        sp_lines <- SpatialLines(list(Lines(list(Line(paths[[1]])), "line1")))
        
        if (length(paths) > 1) {
            # I like for loops, what can I say...
            for (p in 2:length(paths)) {
                id <- paste0("line", as.character(p))
                l <- SpatialLines(list(Lines(list(Line(paths[[p]])), id)))
                sp_lines <- spRbind(sp_lines, l)
            }
        }
        
        return(sp_lines)
    }
}



get_tmc_coords <- function(coords_string) {
    coord2 <- str_extract(coords_string, pattern = "(?<=')(.*?)(?=')")
    coord_list <- str_split(str_split(coord2, ",")[[1]], " ")
    
    tmc_coords <- purrr::transpose(coord_list) %>%
        lapply(unlist) %>%
        as.data.frame(., col.names = c("longitude", "latitude")) %>%
        mutate(latitude = as.numeric(trimws(latitude)),
               longitude = as.numeric(trimws(longitude)))
    as_tibble(tmc_coords)
}
#----------------------------------------------------------
get_geom_coords <- function(coords_string) {
    if (!is.na(coords_string)) {
        coord_list <- str_split(unique(str_split(coords_string, ",|:")[[1]]), " ")
        
        geom_coords <- purrr::transpose(coord_list) %>%
            lapply(unlist) %>%
            as.data.frame(., col.names = c("longitude", "latitude")) %>%
            mutate(latitude = as.numeric(trimws(latitude)),
                   longitude = as.numeric(trimws(longitude)))
        as_tibble(geom_coords)
    }
}


get_signals_sp <- function(corridors) {
    
    corridor_palette <- c("#e41a1c", "#377eb8", "#4daf4a", "#984ea3", 
                          "#ff7f00", "#ffff33", "#a65628", "#f781bf")
    
    rtop_corridors <- corridors %>% 
        filter(grepl("^Zone", Zone)) %>%
        distinct(Corridor)
    
    num_corridors <- nrow(rtop_corridors)
    
    corridor_colors <- rtop_corridors %>% 
        mutate(
            color = rep(corridor_palette, ceiling(num_corridors/8))[1:num_corridors]) %>%
        #color = rep(RColorBrewer::brewer.pal(7, "Dark2"),
        #            ceiling(num_corridors/7))[1:num_corridors]) %>%
        bind_rows(data.frame(Corridor = c("None"), color = GRAY)) %>%
        mutate(Corridor = factor(Corridor))
    
    corr_levels <- levels(corridor_colors$Corridor)
    ordered_levels <- fct_relevel(corridor_colors$Corridor, c("None", corr_levels[corr_levels != "None"]))
    corridor_colors <- corridor_colors %>% 
        mutate(Corridor = factor(Corridor, levels = ordered_levels))
    
    
    most_recent_intersections_list_key <- max(
        aws.s3::get_bucket_df(
            bucket = "gdot-devices", 
            prefix = "maxv_atspm_intersections")$Key)
    ints <- s3read_using(
        read_csv, 
        col_types = cols(
            .default = col_double(),
            PrimaryName = col_character(),
            SecondaryName = col_character(),
            IPAddress = col_character(),
            Enabled = col_logical(),
            Note_atspm = col_character(),
            Start = col_datetime(format = ""),
            Name = col_character(),
            Note_maxv = col_character(),
            HostAddress = col_character()
        ),
        bucket = "gdot-devices", 
        object = most_recent_intersections_list_key,
    ) %>% 
        select(-X1) %>%
        filter(Latitude != 0, Longitude != 0) %>% 
        mutate(SignalID = factor(SignalID))
    
    signals_sp <- left_join(ints, corridors, by = c("SignalID")) %>%
        mutate(
            Corridor = forcats::fct_explicit_na(Corridor, na_level = "None")) %>%
        left_join(corridor_colors, by = c("Corridor")) %>%
        mutate(SignalID = factor(SignalID), Corridor = factor(Corridor),
               Description = if_else(
                   is.na(Description), 
                   glue("{as.character(SignalID)}: {PrimaryName} @ {SecondaryName}"), 
                   Description)
        ) %>%
        
        mutate(
            # If part of a Zone (RTOP), fill is black, otherwise white
            fill_color = ifelse(grepl("^Z", Zone), BLACK, WHITE),
            # If not part of a corridor, gray outer color, otherwise black
            stroke_color = ifelse(Corridor == "None", GRAY, BLACK),
            # If part of a Zone (RTOP), override black outer to corridor color
            stroke_color = ifelse(grepl("^Z", Zone), color, stroke_color))
    Encoding(signals_sp$Description) <- "utf-8"
    signals_sp
}


get_map_data <- function() {
    
    BLACK <- "#000000"
    WHITE <- "#FFFFFF"
    GRAY <- "#D0D0D0"
    DARK_GRAY <- "#7A7A7A"
    DARK_DARK_GRAY <- "#494949"
    
    corridor_palette <- c("#e41a1c", "#377eb8", "#4daf4a", "#984ea3", 
                          "#ff7f00", "#ffff33", "#a65628", "#f781bf")
    
    corridors <- s3read_using(
        read_feather,
        bucket = "gdot-spm",
        object = "all_Corridors_Latest.feather")
    
    rtop_corridors <- corridors %>% 
        filter(grepl("^Zone", Zone)) %>%
        distinct(Corridor)
    
    num_corridors <- nrow(rtop_corridors)
    
    corridor_colors <- rtop_corridors %>% 
        mutate(
            color = rep(corridor_palette, ceiling(num_corridors/8))[1:num_corridors]) %>%
        bind_rows(data.frame(Corridor = c("None"), color = GRAY)) %>%
        mutate(Corridor = factor(Corridor))
    
    corr_levels <- levels(corridor_colors$Corridor)
    ordered_levels <- fct_relevel(corridor_colors$Corridor, c("None", corr_levels[corr_levels != "None"]))
    corridor_colors <- corridor_colors %>% 
        mutate(Corridor = factor(Corridor, levels = ordered_levels))
    
    zone_colors <- data.frame(
        zone = glue("Zone {seq_len(8)}"), 
        color = corridor_palette) %>%
        mutate(color = if_else(color=="#ffff33", "#f7f733", as.character(color)))
    
    # this takes a while: sp::coordinates is slow
    tmcs <- s3read_using(
        read_excel,
        bucket = conf$bucket, 
        object = "Corridor_TMCs_Latest.xlsx") %>%
        mutate(
            Corridor = forcats::fct_explicit_na(Corridor, na_level = "None")) %>%
        left_join(corridor_colors, by = "Corridor") %>%
        mutate(
            Corridor = factor(Corridor),
            color = if_else(Corridor!="None" & is.na(color), DARK_DARK_GRAY, color),
            tmc_coords = purrr::map(coordinates, get_tmc_coords),
            sp_data = purrr::map(
                tmc_coords, function(y) {
                    points_to_line(data = y, long = "longitude", lat = "latitude")
            })
        )
    
    corridors_sp <- do.call(rbind, tmcs$sp_data) %>%
        SpatialLinesDataFrame(tmcs, match.ID = FALSE)
    
    subcor_tmcs <- tmcs %>% 
        filter(!is.na(Subcorridor))
    
    subcorridors_sp <- do.call(rbind, subcor_tmcs$sp_data) %>%
        SpatialLinesDataFrame(tmcs, match.ID = FALSE)
    
    signals_sp <- get_signals_sp()
    
    map_data <- list(
        corridors_sp = corridors_sp, 
        subcorridors_sp = subcorridors_sp, 
        signals_sp = signals_sp)
    
    map_data
}



compare_dfs <- function(df1, df2) {
    x <- dplyr::all_equal(df1, df2)
    y <- str_split(x, "\n")[[1]] %>% 
        str_extract(pattern = "\\d+.*") %>% 
        str_split(", ") %>% 
        lapply(as.integer)
    
    rows_in_x_but_not_in_y <- y[[1]]
    rows_in_y_but_not_in_x <- y[[2]]
    
    list(
        rows_in_x_but_not_in_y = df1[rows_in_x_but_not_in_y,],
        rows_in_y_but_not_in_x = df2[rows_in_y_but_not_in_x,]
    )
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


get_flash_events <- function(conf_athena, start_date, end_date) {
    
    conn <- get_athena_connection(conf_athena)
    x <- tbl(conn, sql(glue(paste(
            "select date, timestamp, signalid, eventcode, eventparam", 
            "from {conf_athena$database}.{conf_athena$atspm_table} where eventcode = 173", 
            "and date between '{start_date}' and '{end_date}'")))) %>% 
        collect()
    
    flashes <- if (nrow(x)) {
        x %>%
            transmute(
                Timestamp = ymd_hms(timestamp),
                SignalID = factor(signalid),
                EventCode = as.integer(eventcode),
                EventParam = as.integer(eventparam),
                Date = ymd(date)) %>% 
            arrange(SignalID, Timestamp) %>% 
            group_by(SignalID) %>% 
            filter(EventParam - lag(EventParam) > 0) %>%
            mutate(
                FlashDuration_s = as.numeric(Timestamp - lag(Timestamp)),
                EndParam = lead(EventParam)) %>%
            ungroup() %>%
            filter(!EventParam %in% c(2)) %>%
            transmute(
                SignalID,
                Timestamp,
                FlashMode = case_when(
                    EventParam == 1 ~ "other(1)",
                    EventParam == 2 ~ "notFlash(2)",
                    EventParam == 3 ~ "automatic(3)",
                    EventParam == 4 ~ "localManual(4)",
                    EventParam == 5 ~ "faultMonitor(5)",
                    EventParam == 6 ~ "mmu(6)",
                    EventParam == 7 ~ "startup(7)",
                    EventParam == 8 ~ "preempt (8)"),
                EndFlashMode = case_when(
                    EndParam == 1 ~ "other(1)",
                    EndParam == 2 ~ "notFlash(2)",
                    EndParam == 3 ~ "automatic(3)",
                    EndParam == 4 ~ "localManual(4)",
                    EndParam == 5 ~ "faultMonitor(5)",
                    EndParam == 6 ~ "mmu(6)",
                    EndParam == 7 ~ "startup(7)",
                    EndParam == 8 ~ "preempt (8)"),
                FlashDuration_s,
                Date) %>%
            arrange(SignalID, Timestamp)
    } else {
        data.frame()
    }
    flashes
}


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



get_latest_det_config <- function() {
    
    date_ <- today(tzone = "America/New_York")
    
    # Get most recent detector config file, start with today() and work backward
    while (TRUE) {
        x <- aws.s3::get_bucket(
            bucket = "gdot-devices", 
            prefix = glue("atspm_det_config_good/date={format(date_, '%F')}"))
        if (length(x)) {
            det_config <- s3read_using(
                arrow::read_feather, 
                bucket = "gdot-devices", 
                object = x$Contents$Key
            )
            break
        } else {
            date_ <- date_ - days(1)
        }
    }
    det_config
}
