
source("Monthly_Report_UI_Functions.R")

metric <- detector_uptime
level <- "corridor"
zone_group <- "Northern Region"
corridor <- "US 50 (Loudoun)"
before_start_date <- as_date("2022-07-18")
before_end_date <- as_date("2022-07-31")
after_start_date <- as_date("2022-08-01")
after_end_date <- as_date("2022-08-14")


query_before_after_data <- function(
        metric, 
        level = "corridor", 
        zone_group, 
        corridor = NULL,
        before_start_date,
        before_end_date,
        after_start_date,
        after_end_date) {
    
    
    
    # metric is one of {vpd, tti, aog, ...}
    # level is one of {corridor, subcorridor, signal}
    # resolution is one of {quarterly, monthly, weekly, daily}
    
    before_start_date <- as_datetime(before_start_date)
    before_end_date <- as_datetime(before_end_date)
    after_start_date <- as_datetime(after_start_date)
    after_end_date <- as_datetime(after_end_date)
    
    period_duration = max(
        before_end_date - before_start_date, 
        after_end_date - after_start_date
    )
    
    if (period_duration < days(3)) {
        resolution <- "daily"  # "hourly" -- for future if we want to add hourly calcs to the mix. Probably better to focus on timing plans.
    } else if (period_duration < weeks(6)) {
        resolution <- "daily"
    } else {
        resolution <- "weekly"
    }
    
    per <- switch(
        resolution,
        "quarterly" = "qu",
        "monthly" = "mo",
        "weekly" = "wk",
        "daily" = "dy")
    
    mr_ <- switch(
        level,
        "corridor" = "cor",
        "subcorridor" = "sub",
        "signal" = "sig")
    
    tab <- if (resolution == "hourly" & !is.null(metric$hourly_table)) {
        metric$hourly_table
    } else {
        metric$table
    }
    
    table <- glue("{mr_}_{per}_{tab}")
    
    if (!is.null(corridor)) {
        where_clause <- "WHERE Corridor = '{corridor}'"
    } else {
        where_clause <- "WHERE Zone_Group = '{zone_group}'"
    }

    query <- glue(paste(
        "SELECT * FROM {table}", 
        where_clause))
    

    datetimefield <- switch(
        resolution,
        "hourly" = "Hour",  # Future
        "daily" = "Date",
        "weekly" = "Date",
        "month" = "Month"  # Not used
    )
    
    query <- paste(
        query, 
        glue("AND {datetimefield} >= '{before_start_date}'"),
        glue("AND {datetimefield} < '{after_end_date + days(1)}'"))
    
    df <- data.frame()
    
    print(query)
    
    tryCatch({
        df <- dbGetQuery(sigops_connection_pool, query)
        
        if (!is.null(corridor)) {
            df <- filter(df, Corridor == corridor)
        }
        
        date_string <- intersect(c("Month", "Date"), names(df))
        if (length(date_string)) {
            df[[date_string]] = as_date(df[[date_string]])
        }
        
        datetime_string <- intersect(c("Hour"), names(df))
        if (length(datetime_string)) {
            df[[datetime_string]] = as_datetime(df[[datetime_string]])
        }
    }, error = function(e) {
        print(e)
    })
    
    list(
        before = filter(df, !!as.name(datetimefield) <= before_end_date),
        after = filter(df, !!as.name(datetimefield) >= after_start_date))
}
