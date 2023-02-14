det_config_arrow_schema <- schema(
    SignalID = string(),  # int64(),
    Detector = int64(),
    ID = int64(),
    DetectorID = string(),
    DistanceFromStopBar = double(),
    MinSpeedFilter = double(),
    DateAdded = time64(),
    DateDisabled = time64(),
    LaneNumber = int64(),
    DecisionPoint = double(),
    MovementDelay = null(),
    LatencyCorrection = double(),
    MovementTypeDesc = string(),
    MovementTypeAbbr = string(),
    Name = string(),
    LaneTypeDesc = string(),
    LaneTypeAbbr = string(),
    DetectionTypeDesc = string(),
    ApproachDesc = string(),
    MPH = double(),
    ProtectedPhaseNumber = int64(),
    IsProtectedPhaseOverlap = bool(),
    PermissivePhaseNumber = double(),
    IsPermissivePhaseOverlap = bool(),
    DirectionTypeDesc = string(),
    DirectionTypeAbbr = string(),
    Latitude = string(),
    Longitude = string(),
    PrimaryName = string(),
    SecondaryName = string(),
    IPAddress = string(),
    RegionID = int64(),
    ControllerTypeID = int64(),
    Enabled = bool(),
    Note = string(),
    Start = time64(),
    TimeFromStopBar = double(),
    CallPhase = int32(),
    CountPriority = int64(),
    index = double())

ped_config_arrow_schema <- schema(
    SignalID = string(),
    Detector = int64(),
    CallPhase = int64())


get_corridors <- function(corr_fn, filter_signals = TRUE) {

    # Keep this up to date to reflect the Corridors_Latest.xlsx file
    cols <- list(SignalID = "text",
                 Zone_Group = "text",
                 Zone = "text",
                 Corridor = "text",
                 Subcorridor = "text",
                 Agency = "text",
                 `Main Street Name` = "text",
                 `Side Street Name` = "text",
                 Milepost = "numeric",
                 Asof = "date",
                 Duplicate = "numeric",
                 Include = "logical",
                 Modified = "date",
                 Note = "text",
                 Latitude = "numeric",
                 Longitude = "numeric")

    df <- readxl::read_xlsx(corr_fn, col_types = unlist(cols)) %>%
        rename(Zone_Group = Contract, Zone = District) %>%

        # Get the last modified record for the Signal|Zone|Corridor combination
        replace_na(replace = list(Modified = ymd("1900-01-01"))) %>%
        group_by(SignalID, Zone, Corridor) %>%
        filter(Modified == max(Modified)) %>%
        ungroup() %>%

        filter(!is.na(Corridor))

    # if filter_signals == FALSE, this creates all_corridors, which
    #   includes corridors without signals
    #   which is used for manual ped/det uptimes and camera uptimes
    if (filter_signals) {
        df <- df %>%
            filter(
                # SignalID > 0,
                Include == TRUE)
    }

    df %>%
        tidyr::unite(Name, c(`Main Street Name`, `Side Street Name`), sep = ' @ ') %>%
        transmute(SignalID = factor(SignalID),
                  Zone = as.factor(Zone),
                  Zone_Group = factor(Zone_Group),
                  Corridor = as.factor(Corridor),
                  Subcorridor = as.factor(Subcorridor),
                  Milepost = as.numeric(Milepost),
                  Agency,
                  Name,
                  Asof = date(Asof),
                  Latitude,
                  Longitude) %>%
        mutate(Description = paste(SignalID, Name, sep = ": "))

}


# This is a "function factory"
# It is meant to be used to create a get_det_config function that takes only the date:
# like: get_det_config <- get_det_config_(conf$bucket, "atspm_det_config_good")
get_det_config_  <- function(bucket, folder, type = "det") {

    function(date_range) {
        if (type == "det") {
            arrow_schema <- det_config_arrow_schema
        } else if (type == "ped") {
            arrow_schema <- ped_config_arrow_schema
        }

        tryCatch({
            dss <- lapply(date_range, function(date_) {
                arrow::open_dataset(
                sources = glue("s3://{bucket}/{folder}/date={date_}"),
                format="feather",
                schema = arrow_schema
                ) %>%
                mutate(
                    Date = date_,
                    SignalID = as.character(SignalID),
                    Detector = as.integer(Detector),
                    CallPhase = as.integer(CallPhase)
                ) %>%
                collect()
            }) %>%
                bind_rows()
        }, error = function(e) {
            stop(glue("Problem getting detector config file for {date_range}: {e}"))
            print(e)
        })
    }
}

get_det_config <- get_det_config_(conf$bucket, join_path(conf$key_prefix, config, atspm_det_config_good), type = "det")
get_ped_config <- get_det_config_(conf$bucket, join_path(conf$key_prefix, config, atspm_ped_config), type = "ped")



get_det_config_aog <- function(date_) {

    get_det_config(date_) %>%
        filter(!is.na(Detector)) %>%
        mutate(AOGPriority =
                   dplyr::case_when(
                       grepl("Exit", DetectionTypeDesc)  ~ 0,
                       grepl("Advanced Count", DetectionTypeDesc) ~ 1,
                       TRUE ~ 2)) %>%
        filter(AOGPriority < 2) %>%
        group_by(SignalID, CallPhase) %>%
        filter(AOGPriority == min(AOGPriority)) %>%
        ungroup() %>%

        transmute(SignalID = factor(SignalID),
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_))
}


get_det_config_qs <- function(date_) {

    # Detector config
    dc <- get_det_config(date_) %>%
        filter(grepl("Advanced Count", DetectionTypeDesc) |
                   grepl("Advanced Speed", DetectionTypeDesc)) %>%
        filter(!is.na(DistanceFromStopBar)) %>%
        filter(!is.na(Detector)) %>%

        transmute(SignalID = factor(SignalID),
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_))

    # Bad detectors
    bd <- s3_read_parquet(
        bucket = conf$bucket,
        object = join_path(conf$key_prefix, glue("mark/bad_detectors/date={date_}/bad_detectors_{date_}.parquet")) %>%
        transmute(SignalID = factor(SignalID),
                  Detector = factor(Detector),
                  Good_Day)

    # Join to take detector config for only good detectors for this day
    left_join(dc, bd, by=c("SignalID", "Detector")) %>%
        filter(is.na(Good_Day)) %>% select(-Good_Day)

}


get_det_config_sf <- function(date_) {

    get_det_config(date_) %>%
        filter(grepl("Stop Bar Presence", DetectionTypeDesc)) %>%
        filter(!is.na(Detector)) %>%


        transmute(SignalID = factor(SignalID),
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_))
}


get_det_config_vol <- function(date_) {

    get_det_config(date_) %>%
        transmute(SignalID = factor(SignalID),
                  Detector = factor(Detector),
                  CallPhase = factor(CallPhase),
                  CountPriority = as.integer(CountPriority),
                  TimeFromStopBar = TimeFromStopBar,
                  Date = date(date_)) %>%
        group_by(SignalID, CallPhase) %>%
        mutate(minCountPriority = min(CountPriority, na.rm = TRUE)) %>%
        ungroup() %>%
        filter(CountPriority == minCountPriority)
}
