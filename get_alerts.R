
suppressMessages({
    library(yaml)
    library(aws.s3)
    library(dplyr)
    library(tidyr)
    library(arrow)
    library(httr)
    library(fst)
    library(lubridate)
    library(runner)
    library(qs)
})

read_zipped_feather <- function(x) {
    read_feather(unzip(x))
}

get_alerts <- function(conf) {
    
    objs <- aws.s3::get_bucket(bucket = conf$bucket, 
                               prefix = 'mark/watchdog')
    lapply(objs, function(obj) {
        key <- obj$Key
        print(key)
        f <- NULL
        if (endsWith(key, "feather.zip")) {
            f <- read_zipped_feather
        } else if (endsWith(key, "fst")) {
            f <- read_fst
        }
        if (!is.null(f)) {
            aws.s3::s3read_using(FUN = f, 
                                 object = key, 
                                 bucket = conf$bucket) %>% 
                as_tibble() %>%
                mutate(SignalID = factor(SignalID),
                       Detector = factor(Detector),
                       Date = date(Date))
        }
    }) %>% bind_rows() %>%
        filter(!is.na(Corridor)) %>%
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
            Alert = factor(Alert)) %>%
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

conf <- read_yaml("Monthly_Report.yaml")

alerts <- get_alerts(conf)

s3write_using(
    alerts, 
    qs::qsave, 
    bucket = conf$bucket, 
    object = "mark/watchdog/alerts.qs",
    opts = list(multipart = TRUE))
