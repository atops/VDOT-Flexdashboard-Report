
# Monthly_Report_Calcs.R

source("renv/activate.R")

library(yaml)
library(glue)

source("Monthly_Report_Functions.R")


print(glue("{Sys.time()} Starting Calcs Script"))

if (interactive()) {
    plan(multisession)
} else {
    plan(multicore)
}
usable_cores <- get_usable_cores(4)
# usable_cores <- 1
doParallel::registerDoParallel(cores = usable_cores)


# aurora_pool <- get_aurora_connection_pool()
aurora <- get_aurora_connection()

#----- DEFINE DATE RANGE FOR CALCULATIONS ------------------------------------#

start_date <- get_date_from_string(conf$start_date)
end_date <- get_date_from_string(conf$end_date)

# Manual overrides
# start_date <- "2023-01-17"
# end_date <- "2023-01-17"

month_abbrs <- get_month_abbrs(start_date, end_date)
#-----------------------------------------------------------------------------#

# # GET CORRIDORS #############################################################

# -- Code to update corridors file/table from Excel file

xlsx_filename <- join_path(conf$key_prefix, conf$corridors_filename_s3)
x <- get_bucket(bucket = conf$bucket, prefix = xlsx_filename)
xlsx_last_modified <- if (!is.null(x)) {
    x$Contents$LastModified
} else {
    as_date("1900-01-01")
}

qs_filename <- sub("\\..*", ".qs", xlsx_filename)
x <- get_bucket(bucket = conf$bucket, prefix = qs_filename)
qs_last_modified <- if (!is.null(x)) {
    x$Contents$LastModified
} else {
    as_date("1900-01-01")
}

if (as_datetime(xlsx_last_modified) > as_datetime(qs_last_modified)) {
    corridors <- s3read_using(
        function(x) get_corridors(x, filter_signals = TRUE),
        object = xlsx_filename,
        bucket = conf$bucket
    )
    qsave(corridors, basename(qs_filename))
    s3_upload_file(
        file = basename(qs_filename),
        object = qs_filename,
        bucket = conf$bucket
    )
    dbExecute(aurora, "TRUNCATE TABLE Corridors")
    dbAppendTable(aurora, "Corridors", corridors)

    all_corridors <- s3read_using(
        function(x) get_corridors(x, filter_signals = FALSE),
        object = xlsx_filename,
        bucket = conf$bucket
    )
    qs_all_filename <- sub("\\..*", ".qs", paste0("all_", conf$corridors_filename_s3))
    qsave(all_corridors, basename(qs_all_filename))
    s3_upload_file(
        file = basename(qs_all_filename),
        object = qs_all_filename,
        bucket = conf$bucket
    )
    dbExecute(aurora, "TRUNCATE TABLE AllCorridors")
    dbAppendTable(aurora, "AllCorridors", all_corridors)
}

corridors <- dbReadTable(aurora, "Corridors")
dbDisconnect(aurora)


signals_list <- unique(corridors$SignalID)


# Add partitions that don't already exists to Athena ATSPM table
athena <- get_athena_connection(conf)
partitions <- dbGetQuery(athena, glue("SHOW PARTITIONS {conf$athena$atspm_table}"))$partition
partitions <- sapply(stringr::str_split(partitions, "="), last)
date_range <- seq(as_date(start_date), as_date(end_date), by = "1 day") %>% as.character()
missing_partitions <- setdiff(date_range, partitions)

if (length(missing_partitions) > 10) {
    print(glue("Adding missing partition: date={missing_partitions}"))
    dbExecute(athena, glue("MSCK REPAIR TABLE {conf$athena$atspm_table}"))
} else if (length(missing_partitions) > 0) {
    print("Adding missing partitions:")
    for (query in glue("ALTER TABLE {conf$athena$atspm_table} add partition (date='{missing_partitions}') location 's3://{conf$bucket}/atspm/date={missing_partitions}/'")) {
        print(query)
        dbExecute(athena, query)
    }
}
dbDisconnect(athena)

