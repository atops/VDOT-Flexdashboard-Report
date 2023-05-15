
library(googleCloudStorageR)
library(gargle)

googleAuthR::gar_gce_auth()


get_bucket <- function(...) {
    df <- gcs_list_objects(...)
    if (nrow(df)) {
        df <- rename(df, LastModified = updated)
        list(Contents = df[1,])
    } else {
        NULL
    }
}


s3_list_objects <- function(...) {
    z <- gcs_list_objects(...)
    if (class(z) == "list") {
        print("list block")
        names(z) <- "Key"
    } else if (class(z) == "data.frame") {
        if (nrow(z) > 0) {
            z <- rename(z, Key = name)
        } else {
            z <- NULL
        }
    }
    z
}


s3_upload_file <- function(file, bucket, object) {
    gcs_upload(file = file, bucket = bucket, name = object, predefinedAcl = "bucketLevel")
}


s3read_using <- function(FUN, bucket, object) {
    fn <- tempfile()
    gcs_get_object(bucket = bucket, object_name = object, saveToDisk = fn)
    x <- FUN(fn)
    file.remove(fn)
    x
}


s3write_using <- function(df, FUN, bucket, object) {
    if (identical(FUN, write_parquet)) {
	 FUN = function(input, output) write_parquet(x = input, sink = output)
    }
    gcs_upload(file = df, object_function = FUN, bucket = bucket, name = object, predefinedAcl = "bucketLevel")
}


s3_write_parquet <- function(df, bucket, object) {
    s3write_using(df, write_parquet, bucket, object)
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
        gcs_upload,
        n_tries = 5,
        df,
        bucket = bucket,
        name = join_path(conf$key_prefix, glue("mark/{table_name}/date={date_}/{fn}.parquet")),
        object_function = function(input, output) write_parquet(x = input, sink = output),
        predefinedAcl = "bucketLevel")
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
        prefix <- glue("{conf$key_prefix}/mark/{table_name}/date={date_}")
        objects = s3_list_objects(bucket = bucket, prefix = prefix)$Key
        lapply(objects, function(obj) {
            df <- s3_read_parquet(bucket = bucket, object = obj, date_) %>%
                convert_to_utc() %>%
                callback()
            if (!is.null(signals_list)) {
                df <- filter(df, SignalID %in% signals_list)
            }
            df
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
