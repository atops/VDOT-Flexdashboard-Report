suppressMessages({
    library(dplyr)
    library(tidyr)
    library(stringr)
    library(arrow)
    library(httr)
    library(fst)
    library(lubridate)
    library(runner)
    library(qs)
    library(glue)
    library(yaml)
    library(DBI)
    library(odbc)
})


source("Database_Functions.R")
source("GCSParquetIO.R")

conf <- read_yaml("Monthly_Report.yaml")

base_path = '.'

if (!dir.exists('logs')) { 
    dir.create('logs') 
}


read_zipped_feather <- function(x) {
    read_feather(unzip(x))
}


get_alerts <- function(conf) {

    objs <- s3_list_objects(bucket = conf$bucket, prefix = glue("{conf$key_prefix}/mark/watchdog"))
    objs <- split(objs, 1:nrow(objs))

    lapply(objs, function(obj) {
        key <- obj$Key
        print(key)
        f <- NULL
        if (endsWith(key, "feather.zip")) {
            f <- read_zipped_feather
        } else if (endsWith(key, "parquet") & !endsWith(key, "alerts.parquet")) {
            f <- read_parquet
        } else if (endsWith(key, "fst")) {
            f <- read_fst
        }
        if (!is.null(f)) {
            s3read_using(FUN = f,
                         object = key,
                         bucket = conf$bucket) %>%
                as_tibble() %>%
                mutate(across(where(is.factor), as.character),
                       Detector = as.integer(Detector),
                       Date = as_date(Date))
        }
    }) %>% bind_rows() %>%
        filter(!is.na(Corridor)) %>%
        mutate(
            CallPhase = as.numeric(CallPhase),
            Detector = as.numeric(Detector)) %>%
        replace_na(replace = list(CallPhase = 0, Detector = 0)) %>%
        transmute(
            Zone_Group = factor(Zone_Group),
            Zone = factor(Zone),
            Corridor = factor(Corridor),
            SignalID = factor(SignalID),
            CallPhase = factor(CallPhase),
            Detector = factor(Detector),
            Date = Date,
            Name = as.character(Name),
            Alert = factor(Alert),
            ApproachDesc) %>%
        filter(Date > today() - days(90)) %>%
        distinct() %>% # Hack to overcome configuration errors
        arrange(Alert, SignalID, CallPhase, Detector, Date) %>%
        group_by(
            Zone_Group, Zone, SignalID, CallPhase, Detector, Alert
        ) %>%

        mutate(
            start_streak = ifelse(
                as.integer(Date - lag(Date), unit = "days") > 1 |
                    Date == min(Date),
                Date,
                NA)) %>%
        fill(start_streak) %>%
        mutate(streak = streak_run(start_streak, k = 90)) %>%
        ungroup() %>%
        select(-start_streak)
}


alerts <- get_alerts(conf)



# Upload to S3 Bucket: mark/watchdog/
tryCatch({

    print(glue("{conf$key_prefix}/mark/watchdog/alerts.parquet"))
    s3write_using(
        alerts,
        write_parquet,
        bucket = conf$bucket,
        object = glue("{conf$key_prefix}/mark/watchdog/alerts.parquet"))

    write(
        glue(paste0(
            "{format(now(), '%F %H:%M:%S')}|SUCCESS|get_alerts.R|get_alerts|Line 173|",
            "Uploaded {conf$bucket}/{conf$key_prefix}/mark/watchdog/alerts.parquet")),
        file.path(base_path, glue("logs/get_alerts_{today()}.log")),
        append = TRUE
    )
}, error = function(e) {
    write(
        glue("{format(now(), '%F %H:%M:%S')}|ERROR|get_alerts.R|get_alerts|Line 173|Failed to upload to S3 - {e}"),
        file.path(base_path, glue("logs/get_alerts_{today()}.log")),
        append = TRUE
    )
})


# Write to Database
tryCatch({
    conn <- get_aurora_connection()
    dbExecute(conn, "TRUNCATE TABLE WatchdogAlerts")
    dbWriteTable(conn, "WatchdogAlerts", alerts, row.names = FALSE, append = TRUE, overwrite = FALSE)

    write(
        glue(paste0(
            "{format(now(), '%F %H:%M:%S')}|SUCCESS|get_alerts.R|get_alerts|Line 217|"),
            "Wrote to Aurora Database: WatchdogAlerts table"),
        file.path(base_path, glue("logs/get_alerts_{today()}.log")),
        append = TRUE
    )
}, error = function(e) {
    write(
        glue("{format(now(), '%F %H:%M:%S')}|ERROR|get_alerts.R|get_alerts|Line 217|Failed to write to Database - {e}"),
        file.path(base_path, glue("logs/get_alerts_{today()}.log")),
        append = TRUE
    )
})
