
# Monthly_Report_Functions.R

suppressMessages({
    library(DBI)
    library(readxl)
    library(readr)
    library(dplyr)
    library(tidyr)
    library(stringr)
    library(purrr)
    library(lubridate)
    library(glue)
    library(data.table)
    library(formattable)
    library(forcats)
    library(fst)
    library(parallel)
    library(doParallel)
    library(future)
    library(pool)
    library(httr)
    library(sp)
    library(sf)
    library(yaml)
    library(utils)
    library(readxl)
    library(rjson)
    library(plotly)
    library(crosstalk)
    library(runner)
    library(fitdistrplus)
    library(foreach)
    library(arrow)
    # https://arrow.apache.org/install/
    library(qs)
    library(future.apply)
})


#options(dplyr.summarise.inform = FALSE)

select <- dplyr::select
filter <- dplyr::filter

options(future.rng.onMisue = "ignore")

conf <- read_yaml("Monthly_Report.yaml")
aws_conf <- read_yaml("Monthly_Report_AWS.yaml")

if (Sys.info()["sysname"] == "Windows") {
    home_path <- dirname(path.expand("~"))

} else if (Sys.info()["sysname"] == "Linux") {
    home_path <- "~"

} else {
    stop("Unknown operating system.")
}


# Colorbrewer Paired Palette Colors
LIGHT_BLUE = "#A6CEE3";   BLUE = "#1F78B4"
LIGHT_GREEN = "#B2DF8A";  GREEN = "#33A02C"
LIGHT_RED = "#FB9A99";    RED = "#E31A1C"
LIGHT_ORANGE = "#FDBF6F"; ORANGE = "#FF7F00"
LIGHT_PURPLE = "#CAB2D6"; PURPLE = "#6A3D9A"
LIGHT_BROWN = "#FFFF99";  BROWN = "#B15928"

RED2 = "#e41a1c"
VDOT_BLUE = "#005da9"

BLACK <- "#000000"
WHITE <- "#FFFFFF"
GRAY <- "#D0D0D0"
DARK_GRAY <- "#7A7A7A"
DARK_DARK_GRAY <- "#494949"


SUN = 1; MON = 2; TUE = 3; WED = 4; THU = 5; FRI = 6; SAT = 7

AM_PEAK_HOURS = conf$AM_PEAK_HOURS
PM_PEAK_HOURS = conf$PM_PEAK_HOURS

Sys.setenv(TZ="America/New_York")


source("Utilities.R")
source("GCSParquetIO.R")
source("Configs.R")
source("Counts.R")
source("Metrics.R")
source("Map.R")
source("Aggregations.R")
source("Database_Functions.R")

usable_cores <- get_usable_cores()






# Cross filter Daily Volumes Chart. For Monthly Report ------------------------
get_vpd_plot <- function(cor_weekly_vpd, cor_monthly_vpd) {

    sdw <- SharedData$new(cor_weekly_vpd, ~Corridor, group = "grp")
    sdm <- SharedData$new(dplyr::filter(cor_monthly_vpd, month(Month)==10), ~Corridor, group = "grp")

    font_ <- list(family = "Ubuntu")

    base <- plot_ly(sdw, color = I("gray")) %>%
        group_by(Corridor)
    base_m <- plot_ly(sdm, color = I("gray")) %>%
        group_by(Corridor)

    p1 <- base_m %>%
        summarise(vpd = mean(vpd, na.rm = TRUE)) %>% # This has to be just the current month's vpd
        arrange(vpd) %>%
        add_bars(x = ~vpd,
                 y = ~factor(Corridor, levels = Corridor),
                 text = ~scales::comma_format()(as.integer(vpd)),
                 textposition = "inside",
                 textfont=list(color="white"),
                 hoverinfo = "none") %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = "October Volume (vpd)", zeroline = FALSE),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = font_,
            margin = list(pad = 4)
        )
    p2 <- base %>%
        add_lines(x = ~Date,
                  y = ~vpd,
                  alpha = 0.3) %>%
        layout(xaxis = list(title = "Vehicles/day"),
               showlegend = FALSE)

    subplot(p1, p2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
        layout(margin = list(l = 80),
               title = "Volume (veh/day) Trend") %>%
        highlight(color = "#256194", opacityDim = 0.9, defaultValues = c("All"),
                  selected = attrs_selected(insidetextfont=list(color="white"),
                                            textposition = "inside"))
}


# Cross filter Hourly Volumes Chart. For Monthly Report -----------------------
get_vphpl_plot <- function(df, group_name, chart_title, bar_subtitle, mo) {

    sdw <- SharedData$new(df, ~Corridor, group = group_name)
    sdm <- SharedData$new(dplyr::filter(df, month(Date) == mo), ~Corridor, group = group_name)

    font_ <- list(family = "Ubuntu")

    base <- plot_ly(sdw, color = I("gray")) %>%
        group_by(Corridor)
    base_m <- plot_ly(sdm, color = I("gray")) %>%
        group_by(Corridor)

    p1 <- base_m %>%
        summarise(vphpl = mean(vphpl, na.rm = TRUE)) %>% # This has to be just the current month's vphpl
        arrange(vphpl) %>%
        add_bars(x = ~vphpl,
                 y = ~factor(Corridor, levels = Corridor),
                 text = ~scales::comma_format()(as.integer(vphpl)),
                 textposition = "inside",
                 textfont=list(color="white"),
                 hoverinfo = "none") %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = bar_subtitle, zeroline = FALSE),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = font_,
            margin = list(pad = 4)
        )
    p2 <- base %>%
        add_lines(x = ~Date, y = ~vphpl, alpha = 0.3) %>%
        layout(xaxis = list(title = "Date"),
               showlegend = FALSE,
               annotations = list(text = chart_title,
                                  font = list(family = "Ubuntu",
                                              size = 12),
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1,
                                  showarrow = FALSE)
        )


    subplot(p1, p2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
        layout(margin = list(l = 80, r = 40)) %>%
        highlight(color = "#256194", opacityDim = 0.9, defaultValues = c("All"),
                  selected = attrs_selected(insidetextfont=list(color="white"), textposition = "inside"))
}


get_vph_peak_plot <- function(df, group_name, chart_title, bar_subtitle, mo) {

    sdw <- SharedData$new(df, ~Corridor, group = group_name)
    sdm <- SharedData$new(dplyr::filter(df, month(Date) == mo), ~Corridor, group = group_name)

    font_ <- list(family = "Ubuntu")

    base <- plot_ly(sdw, color = I("gray")) %>%
        group_by(Corridor)
    base_m <- plot_ly(sdm, color = I("gray")) %>%
        group_by(Corridor)

    p1 <- base_m %>%
        summarise(vph = mean(vph, na.rm = TRUE)) %>% # This has to be just the current month's vph
        arrange(vph) %>%
        add_bars(x = ~vph,
                 y = ~factor(Corridor, levels = Corridor),
                 text = ~scales::comma_format()(as.integer(vph)),
                 textposition = "inside",
                 textfont=list(color="white"),
                 hoverinfo = "none") %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = bar_subtitle, zeroline = FALSE),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = font_,
            margin = list(pad = 4)
        )
    p2 <- base %>%
        add_lines(x = ~Date, y = ~vph, alpha = 0.3) %>%
        layout(xaxis = list(title = "Date"),
               showlegend = FALSE,
               annotations = list(text = chart_title,
                                  font = list(family = "Ubuntu",
                                              size = 12),
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1,
                                  showarrow = FALSE)
        )


    subplot(p1, p2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
        layout(margin = list(l = 80, r = 40)) %>%
        highlight(color = "#256194", opacityDim = 0.9, defaultValues = c("All"),
                  selected = attrs_selected(insidetextfont=list(color="white"), textposition = "inside"))
}





