
dummy <- function() {
f <- function(input, output) write_parquet(input, file = output)
gcs_upload(
    mtcars, 
    bucket = "its-apps-atspm-stage", 
    object_function = f, 
    name = "kimley-horn/mtcars.parquet")

df <- read_parquet(
    gcs_get_object("gs://its-apps-atspm-stage/kimley-horn/mtcars.parquet"))
}


s3_list_objects <- function(...) {
    gcs_list_objects(...) %>%
        rename(Key = name)
}


s3_upload_file <- function(file, bucket, object) {
    gcs_upload(file, bucket, name = object)
}


s3read_using <- function(FUN, bucket, object) {
    FUN(gcs_get_object(bucket = bucket, object_name = object))
}


s3write_using <- function(FUN, df, bucket, object) {
    gcs_upload(df, object_function = FUN, bucket = bucket, name = object)
}


s3_write_parquet <- function(df, bucket, object) {
    s3write_using(write_parquet, df, bucket, object)
}


s3_upload_parquet <- function(df, date_, fn, bucket, table_name) {
    
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
        gcs_upload,
        n_tries = 5,
        df,
        object_function = function(input, output) write_parquet(input, file = output),
        bucket = bucket,
        name = glue("{conf$key_prefix}/mark/{table_name}/date={date_}/{fn}.parquet")
    )
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
        df <- read_parquet(gcs_get_object(bucket = bucket, object_name = object)) %>%
            select(-starts_with("__"))
        if (!is.na(date_)) {
            df <- df %>%
                mutate(Date = as_date(date_))
        }
        df
    }, error = function(e) {
        print(e)
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
        prefix <- glue("{conf$key_prefix}/mark/{table_name}/date={date_}")
        objects = s3_list_objects(bucket = bucket, prefix = prefix)$Key
        lapply(objects, function(obj) {
            s3_read_parquet(bucket = bucket, object = obj, date_) %>%
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
