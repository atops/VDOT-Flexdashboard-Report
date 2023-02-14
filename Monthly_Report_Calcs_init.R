
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
# usable_cores <- get_usable_cores(4)
usable_cores <- 1
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
