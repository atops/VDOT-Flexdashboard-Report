
# Database Functions
suppressMessages({
    library(odbc)
    library(RMariaDB)
    library(yaml)
})

cred <- read_yaml("Monthly_Report_AWS.yaml")

# My own function to perform multiple inserts at once.
# Hadn't found a way to do this through native functions.
mydbAppendTable <- function(conn, name, value, chunksize = 1e4) {

    df <- value %>%
        mutate(
            across(where(is.Date), ~format(., "%F")),
            across(where(is.POSIXct), ~format(., "%F %H:%M:%S")),
            across(where(is.factor), as.character),
            across(where(is.character), ~replace(., is.na(.), "")),
            across(where(is.character), ~str_replace_all(., "'", "\\\\'")),
            across(where(is.character), ~paste0("'", ., "'")),
            across(where(is.numeric), ~replace(., !is.finite(.), NA)))

    table_name <- name

    vals <- unite(df, "z", names(df), sep = ",") %>% pull(z)
    vals <- glue("({vals})") %>% str_replace_all("NA", "NULL")
    vals_list <- split(vals, ceiling(seq_along(vals)/chunksize))

    query0 <- glue("INSERT INTO {table_name} (`{paste0(colnames(df), collapse = '`, `')}`) VALUES ")

    for (v in vals_list) {
        query <- paste0(query0, paste0(v, collapse = ','))
        dbExecute(conn, query)
    }
}


# -- Previously from Monthly_Report_Functions.R

get_atspm_connection <- function(conf_atspm) {
    
    dbConnect(odbc::odbc(),
              dsn = conf_atspm$ATSPM_DSN,
              uid = conf_atspm$ATSPM_UID,
              pwd = conf_atspm$ATSPM_PWD)
}


get_aurora_connection <- function(
    f = RMariaDB::dbConnect,
    driver = RMariaDB::MariaDB(),
    load_data_local_infile = FALSE
) {

    f(drv = driver,
      host = cred$RDS_HOST,
      port = 3306,
      dbname = cred$RDS_DATABASE,
      username = cred$RDS_USERNAME,
      password = cred$RDS_PASSWORD,
      load_data_local_infile = load_data_local_infile)
}


get_athena_connection <- function(conf, f = dbConnect) {
    f(
        odbc::odbc(), 
        dsn = paste("athena", conf$profile),
        AwsRegion = conf$aws_region,
        S3OutputLocation=conf$athena$staging_dir,
        Schema = conf$athena$database 
    )
}


get_athena_connection_pool <- function(conf) {
    get_athena_connection(conf, pool::dbPool)
}


add_partition <- function(conf, table_name, date_) {
    tryCatch({
        conn_ <- get_athena_connection(conf)
        dbExecute(conn_,
                  sql(glue(paste("ALTER TABLE {conf$athena$database}.{table_name}",
                                 "ADD PARTITION (date='{date_}')"))))
        print(glue("Successfully created partition (date='{date_}') for {conf$athena$database}.{table_name}"))
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
    if (level == "signal" & zone_group == "All") {
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



