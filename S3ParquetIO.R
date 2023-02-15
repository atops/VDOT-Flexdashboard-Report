
library(aws.s3)


# Set credentials from ~/.aws/credentials file
aws.signature::use_credentials(profile = conf$profile)

# Need to set the default region as well. use_credentials doesn't do this.
credentials <- aws.signature::read_credentials()[[conf$profile]]
Sys.setenv(AWS_DEFAULT_REGION = conf$aws_region)



s3_list_objects <- function(...) {
    s3b <- aws.s3::get_bucket(...)
    
    df <- lapply(names(s3b$Contents), function(attr) {
        print(attr)
        unlist(lapply(s3b, function(x) x[[attr]]), use.names = FALSE)
    }) %>% as.data.frame()
    names(df) <- names(s3b$Contents)
    df
}


s3_upload_file <- function(file, bucket, object, ...) {
    aws.s3::put_object(file, bucket, object, ...)
}


s3read_using <- aws.s3::s3read_using


s3write_using <- aws.s3::s3write_using 


s3_write_parquet <- function(df, bucket, object, ...) {
    s3write_using(df, write_parquet, bucket, object, ...)
}


s3_upload_parquet <- function(df, date_, fn, bucket, table_name, conf) {
    
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
        object = join_path(conf$key_prefix, glue("mark/{table_name}/date={date_}/{fn}.parquet")),
        opts = list(multipart = TRUE)
    )
    
    add_partition(conf, table_name, date_)
}



s3_upload_parquet_date_split <- function(df, prefix, bucket, table_name, conf, parallel = TRUE) {
    
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
                          conf = conf)
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
                                      conf = conf)
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
                                      conf = conf)
                })
        }
    }
}


s3_read_parquet <- function(bucket, object, date_ = NULL) {
    
    if (is.null(date_)) {
        date_ <- str_extract(object, "\\d{4}-\\d{2}-\\d{2}")
    }
    tryCatch({
        df <- s3read_using(read_parquet, bucket = bucket, object = object) %>%
            select(-starts_with("__"))
        if (!is.na(date_)) {
            df <- mutate(df, Date = as_date(date_))
        }
        df
    }, error = function(e) {
        print(glue("Could not read {bucket}/{object} - {e}"))
        data.frame()
    })
}


s3_read_parquet_parallel <- function(table_name,
                                     start_date,
                                     end_date,
                                     signals_list = NULL,
                                     bucket = NULL,
                                     conf,
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
