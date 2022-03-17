
# Database Functions
library(odbc)
library(RMariaDB)
library(yaml)
library(duckdb)
library(tictoc)


cred <- read_yaml("Monthly_Report_AWS.yaml")

# -- Previously from Monthly_Report_Functions.R

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


get_duckdb_connection <- function(dbdir, read_only = FALSE, f = duckdb::dbConnect) {
    f(
        drv = duckdb::duckdb(),
        dbdir = dbdir,
        read_only = read_only
    )
}


get_duckdb_connection_pool <- function(dbdir, read_only = FALSE) {
    get_duckdb_connection(dbdir, read_only, pool::dbPool)
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



query_data <- function(
    metric, 
    level = "corridor", 
    resolution = "monthly", 
    hourly = FALSE, 
    zone_group, 
    corridor = NULL,
    month = NULL, 
    quarter = NULL, 
    upto = TRUE) {
    
    # metric is one of {vpd, tti, aog, ...}
    # level is one of {corridor, subcorridor, signal}
    # resolution is one of {quarterly, monthly, weekly, daily}
    
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
    
    tab <- if (hourly & !is.null(metric$hourly_table)) {
        metric$hourly_table
    } else {
        metric$table
    }
    
    table <- glue("{mr_}_{per}_{tab}")
    
    # Special cases--groups of corridors
    if (level == "corridor" & (grepl("RTOP", zone_group))) {
        if (zone_group == "All RTOP") {
            zones <- c("All RTOP", "RTOP1", "RTOP2", RTOP1_ZONES, RTOP2_ZONES)
        } else if (zone_group == "RTOP1") {
            zones <- c("All RTOP", "RTOP1", RTOP1_ZONES)
        } else if (zone_group == "RTOP2") {
            zones <- c("All RTOP", "RTOP2", RTOP2_ZONES)
        }
        zones <- paste(glue("'{zones}'"), collapse = ",")
        where_clause <- glue("WHERE Zone_Group in ({zones})")
        where_clause <- paste(where_clause, "AND Corridor NOT LIKE 'Zone%'")
    } else if (zone_group == "Zone 7" ) {
        zones <- c("Zone 7", "Zone 7m", "Zone 7d")
        zones <- paste(glue("'{zones}'"), collapse = ",")
        where_clause <- glue("WHERE Zone_Group in ({zones})")
    } else if (level == "signal" & zone_group == "All") {
        # Special case used by the map which currently shows signal-level data
        # for all signals all the time.
        where_clause <- "WHERE True"
    } else {
        where_clause <- "WHERE Zone_Group = '{zone_group}'"
    }
    
    query <- glue(paste(
        "SELECT * FROM {table}", 
        where_clause))
    
    comparison <- ifelse(upto, "<=", "=")
    
    if (typeof(month) == "character") {
        month <- as_date(month)
    }
    
    if (hourly & !is.null(metric$hourly_table)) {
        if (resolution == "monthly") {
            query <- paste(query, glue("AND Hour <= '{month + months(1) - hours(1)}'"))
            if (!upto) {
                query <- paste(query, glue("AND Hour >= '{month}'"))
            }
        }
    } else if (resolution == "monthly") {
        query <- paste(query, glue("AND Month {comparison} '{month}'"))
    } else if (resolution == "quarterly") { # current_quarter is not null
        query <- paste(query, glue("AND Quarter {comparison} {quarter}"))
        
    } else if (resolution == "weekly" | resolution == "daily") {
        query <- paste(query, glue("AND Date {comparison} '{month + months(1) - days(1)}'"))
        
    } else {
        "oops"
    }

    df <- data.frame()
    
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

    df
}


dbWriteTable <- function(conn, name, value, ...) {
    tic()
    print(class(conn)[1])
    if (class(conn)[1] == "MariaDBConnection") {
        fn <- tempfile()
        write_csv(value, fn)
        DBI::dbExecute(
            conn,
            glue("LOAD DATA LOCAL INFILE '{fn}' into table {name} fields terminated by ',' IGNORE 1 LINES"))
        file.remove(fn)
    } else {
        DBI::dbWriteTable(conn, name, value, ...)
    }
    toc()
}


get_athena_connection <- function(conf_athena, f = dbConnect) {
    f(odbc::odbc(), dsn = "athena")
}


get_athena_connection_pool <- function(conf_athena) {
    get_athena_connection(conf_athena, pool::dbPool)
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



