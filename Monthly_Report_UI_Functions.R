# VDOT
# Monthly_Report_UI_Functions

suppressMessages({
    library(flexdashboard)
    library(shiny)
    library(yaml)
    library(dplyr)
    library(dbplyr)
    library(tidyr)
    library(lubridate)
    library(purrr)
    library(stringr)
    library(readr)
    library(glue)
#    library(arrow)
    library(fst)
    library(forcats)
    library(plotly)
    library(crosstalk)
    library(memoise)
    library(future)
    library(pool)
    library(rsconnect)
    library(formattable)
    library(data.table)
    library(htmltools)
    library(leaflet)
    library(sp)
    library(odbc)
    library(shinycssloaders)
    library(compiler)
})

plan(multisession)

suppressMessages(library(DT))

options(dplyr.summarise.inform = FALSE)

select <- dplyr::select
filter <- dplyr::filter
layout <- plotly::layout

# Colorbrewer Paired Palette Colors
LIGHT_BLUE = "#A6CEE3";   BLUE = "#1F78B4"
LIGHT_GREEN = "#B2DF8A";  GREEN = "#33A02C"
LIGHT_RED = "#FB9A99";    RED = "#E31A1C"
LIGHT_ORANGE = "#FDBF6F"; ORANGE = "#FF7F00"
LIGHT_PURPLE = "#CAB2D6"; PURPLE = "#6A3D9A"
LIGHT_BROWN = "#FFFF99";  BROWN = "#B15928"

DARK_GRAY = "#636363"
BLACK = "#000000"
DARK_GRAY_BAR = "#252525"
LIGHT_GRAY_BAR = "#bdbdbd"

RED2 = "#e41a1c"
GDOT_BLUE = "#004c7a"; GDOT_BLUE_RGB = "#315877"
VDOT_BLUE = "#005da9";
GDOT_YELLOW = "#EEB211"; GDOT_YELLOW_RGB = "rgba(238, 178, 17, 0.80)"

colrs <- c("1" = LIGHT_BLUE, "2" = BLUE, 
           "3" = LIGHT_GREEN, "4" = GREEN, 
           "5" = LIGHT_RED, "6" = RED, 
           "7" = LIGHT_ORANGE, "8" = ORANGE, 
           "9" = LIGHT_PURPLE, "10" = PURPLE, 
           "11" = LIGHT_BROWN, "12" = BROWN,
           "0" = DARK_GRAY)

RTOP1_ZONES <- c("Zone 1", "Zone 2", "Zone 3", "Zone 8")
RTOP2_ZONES <- c("Zone 4", "Zone 5", "Zone 6", "Zone 7m", "Zone 7d")


# Constants for Corridor Summary Table

metric.order <- c("du", "pau", "cctvu", "cu", "tp", "aog", "qs", "sf", "tti", "pti", "tasks")
metric.names <- c(
    "Vehicle Detector Availability",
    "Ped Detector Availability",
    "CCTV Availability",
    "Communications Uptime",
    "Traffic Volume (Throughput)",
    "Arrivals on Green",
    "Queue Spillback Rate",
    "Split Failure",
    "Travel Time Index",
    "Planning Time Index",
    "Outstanding Tasks"
)
metric.goals <- c(rep("> 95%", 4), 
                  "< 5% dec. from prev. mo", 
                  "> 80%", 
                  "< 10%", 
                  "< 5%", 
                  rep("< 2.0", 2),
                  "Dec. from prev. mo")
metric.goals.numeric <- c(rep(0.95, 4), -.05, .8, .10, .05, 2, 2, 0)
metric.goals.sign <- c(rep(">", 4), ">", ">", rep("<", 4), "<")

yes.color <- "green"
no.color <- "red"

# ###########################################################

conf <- read_yaml("Monthly_Report.yaml")

conf$mode <- conf_mode
aws_conf <- read_yaml("Monthly_Report_AWS.yaml")

conf$athena$uid <- aws_conf$AWS_ACCESS_KEY_ID
conf$athena$pwd <- aws_conf$AWS_SECRET_ACCESS_KEY
conf$athena$region <- aws_conf$AWS_DEFAULT_REGION

if (conf$mode == "production") {
    last_month <- ymd(conf$production_report_end_date)   # Production
    
} else if (conf$mode == "beta") {
    last_month <- floor_date(today() - days(6), unit = "months")   # Beta
    
} else {
    stop("mode defined in configuration yaml file must be either production or beta")
}    

endof_last_month <- last_month + months(1) - days(1)
first_month <- last_month - months(12)
report_months <- seq(last_month, first_month, by = "-1 month")
month_options <- report_months %>% format("%B %Y")

zone_group_options <- conf$zone_groups


s3checkFunc <- function(bucket, object, aws_conf) {
    function() {
	cat(file="dash.log", "checking\n", append=TRUE)
        aws.s3::get_bucket(
	    bucket = bucket, 
	    prefix = object,
	    key = aws_conf$AWS_ACCESS_KEY_ID,
            secret = aws_conf$AWS_SECRET_ACCESS_KEY)$Contents$LastModified
    }
}

s3valueFunc <- function(bucket, object, aws_conf) {
    cat(file="dash.log", "values\n", append=TRUE)
    cat(file="dash.log", object, append=TRUE)
    cat(file="dash.log", "\n", append=TRUE)
    read_function <- if (endsWith(object, ".qs")) {
        qs::qread
    } else if (endsWith(object, ".feather")) {
        read_feather
    }
    
    function() {
        x <- aws.s3::s3read_using(
            read_function,
            object = object,
            bucket = bucket,
            opts = list(
		key = aws_conf$AWS_ACCESS_KEY_ID,
                secret = aws_conf$AWS_SECRET_ACCESS_KEY))
        cat(file="dash.log", "values passed\n", append=TRUE)
	x
    }
}

s3reactivePoll <- function(intervalMillis, bucket, object, aws_s3) {
    reactivePoll(intervalMillis, session = NULL,
                 checkFunc = s3checkFunc(bucket = bucket, object = object, aws_s3),
                 valueFunc = s3valueFunc(bucket = bucket, object = object, aws_s3))
}

if (conf$mode == "production") {
    
    # corridors %<-% read_feather("all_corridors.feather")
    # cor <- readRDS("cor.rds")
    # sig %<-% readRDS("sig.rds")
    # sub %<-% readRDS("sub.rds")
    # teams_tables %<-% readRDS("teams_tables.rds")
    
} else if (conf$mode == "beta") {
    
    poll_interval <- 1000*3600 # 3600 seconds = 1 hour
    
    corridors_key <- sub("\\..*", ".qs", paste0("all_", conf$corridors_filename_s3))
    corridors <- s3reactivePoll(poll_interval, bucket = conf$bucket, object = corridors_key, aws_conf)

    cordata <- s3reactivePoll(poll_interval, bucket = conf$bucket, object = "cor_ec2.qs", aws_conf)
    sigdata <- s3reactivePoll(poll_interval, bucket = conf$bucket, object = "sig_ec2.qs", aws_conf)
    subdata <- s3reactivePoll(poll_interval, bucket = conf$bucket, object = "sub_ec2.qs", aws_conf)

    alerts <- s3reactivePoll(poll_interval, bucket = conf$bucket, object = "mark/watchdog/alerts.qs", aws_conf) 

} else {
    stop("mode defined in configuration yaml file must be either production or beta")
}


as_int <- function(x) {scales::comma_format()(as.integer(x))}
as_2dec <- function(x) {sprintf(x, fmt = "%.2f")}
as_pct <- function(x) {sprintf(x * 100, fmt = "%.1f%%")}
as_currency <- function(x) {scales::dollar_format(accuracy = 1)(x)}

goal <- list("tp" = NULL,
             "aogd" = 0.80,
             "prd" = 1.20,
             "qsd" = NULL,
             "sfd" = NULL,
             "tti" = 1.20,
             "pti" = 1.30,
             "du" = 0.95,
             "ped" = 0.95,
             "cctv" = 0.95,
             "cu" = 0.95,
             "pau" = 0.95)


get_athena_connection <- function(conf_athena, f = dbConnect) {
    f(odbc::odbc(), dsn = "athena")
}


get_athena_connection_pool <- function(conf_athena) {
    get_athena_connection(conf_athena, pool::dbPool)
}


read_zipped_feather <- function(x) {
    read_feather(unzip(x))
}


# Filter for zone_group and zone
filter_mr_data_ <- function(df, zone_group_) {
    if (zone_group_ == "All RTOP") {
       df %>% 
            filter(Zone_Group %in% c(RTOP1_ZONES, RTOP2_ZONES, "RTOP1", "RTOP2", "All RTOP"),
                   !Corridor %in% c(RTOP1_ZONES, RTOP2_ZONES))
        
    } else if (zone_group_ == "RTOP1") {
        df %>%
            filter(Zone_Group %in% c(RTOP1_ZONES, "RTOP1"),
                   !Corridor %in% c(RTOP1_ZONES))
        
    } else if (zone_group_ == "RTOP2") {
       df %>%
            filter(Zone_Group %in% c(RTOP2_ZONES, "RTOP2"),
                   !Corridor %in% c(RTOP2_ZONES))
        
    } else if (zone_group_ == "Zone 7") {
        df %>%
            filter(Zone_Group %in% c("Zone 7m", "Zone 7d", "Zone 7"))
        
    } else {
        df %>% 
            filter(Zone_Group == zone_group_)
    }
}
#filter_mr_data <- memoise(filter_mr_data_)
filter_mr_data <- cmpfun(filter_mr_data_)

get_valuebox_ <- function(cor_monthly_df, var_, var_fmt, break_ = FALSE, 
                          zone, mo, qu = NULL) {

    if (is.null(qu)) { # want monthly, not quarterly data
        vals <- cor_monthly_df %>%
            dplyr::filter(Corridor == zone & Month == mo) %>% as.list()
    } else {
        vals <- cor_monthly_df %>%
            dplyr::filter(Corridor == zone & Quarter == qu) %>% as.list()
    }

    vals$delta <- if_else(is.na(vals$delta), 0, vals$delta)
    
    val <- var_fmt(vals[[var_]])
    del <- paste0(if_else(vals$delta > 0, " (+", " ( "), as_pct(vals$delta), ")")
    
    shiny::validate(need(val, message = "NA"))
    
    if (break_) {
        tags$div(HTML(paste(
            val,  # tags$div(val, style = "font-family: Roboto Slab; font-weight: 700;"),
            tags$p(del, style = "font-size: 50%; line-height: 5px; padding: 0 0 0.2em 0;")
        )))
    } else {
        tags$div(HTML(paste(
            val, 
            tags$span(del, style = "font-size: 70%;")
        )))
    }
}
get_valuebox <- memoise(get_valuebox_)



perf_plot_no_data_ <- function(name_) {
    
    ax <- list(title = "", showticklabels = FALSE, showgrid = FALSE, zeroline = FALSE)
    ay <- list(title = "", showticklabels = FALSE, showgrid = FALSE, zeroline = FALSE)
    
    plot_ly(type = "scatter", mode = "markers") %>%
        layout(xaxis = ax, 
               yaxis = ay,
               annotations = list(x = -.02,
                                  y = 0.4,
                                  xref = "paper",
                                  yref = "paper",
                                  xanchor = "right",
                                  text = name_,
                                  font = list(size = 12),
                                  showarrow = FALSE),
               showlegend = FALSE,
               margin = list(l = 120,
                             r = 40)) %>% 
        plotly::config(displayModeBar = F)
}
perf_plot_no_data <- memoise(perf_plot_no_data_)


## ------------ With Goals and Fill Colors ---------------------------------------
perf_plot_beta_ <- function(data_, value_, name_, color_, fill_color_,
                       format_func = function(x) {x},
                       hoverformat_ = ",.0f",
                       goal_ = NULL) {

    ax <- list(title = "", showticklabels = TRUE, showgrid = FALSE)
    ay <- list(title = "", showticklabels = FALSE, showgrid = FALSE, zeroline = FALSE, hoverformat = hoverformat_)

    value_ <- as.name(value_)
    data_ <- dplyr::rename(data_, value = !!value_)

    first <- data_[which.min(data_$Month), ]
    last <- data_[which.max(data_$Month), ]

    p <- plot_ly(type = "scatter", mode = "markers")

    if (!is.null(goal_)) {
        p <- p %>%
            add_trace(data = data_,
                      x = ~Month, y = goal_,
                      name = "Goal",
                      line = list(color = DARK_GRAY), #, dash = "dot"),
                      mode = 'lines') %>%
            add_trace(data = data_,
                      x = ~Month, y = ~value,
                      name = name_,
                      line = list(color = color_),
                      mode = 'lines',
                      fill = 'tonexty',
                      fillcolor = fill_color_)
    } else {
        p <- p %>%
            add_trace(data = data_,
                      x = ~Month, y = ~value,
                      name = name_,
                      line = list(color = color_),
                      mode = 'lines')
    }
    p %>%
        add_annotations(x = 0,
                        y = first$value,
                        text = format_func(first$value),
                        showarrow = FALSE,
                        xanchor = "right",
                        xref = "paper",
                        width = 50,
                        align = "right",
                        borderpad = 5) %>%
        add_annotations(x = 1,
                        y = last$value,
                        text = format_func(last$value),
                        font = list(size = 16),
                        showarrow = FALSE,
                        xanchor = "left",
                        xref = "paper",
                        width = 60,
                        align = "left",
                        borderpad = 5) %>%
        layout(xaxis = ax,
               yaxis = ay,
               showlegend = FALSE,
               margin = list(l = 50, #120
                             r = 60,
                             t = 10,
                             b = 10)) %>%
        plotly::config(displayModeBar = F)
}
#perf_plot_beta <- memoise(perf_plot_beta_)
perf_plot_beta <- cmpfun(perf_plot_beta_)

## ------------ With Goals and Fill Colors ---------------------------------------


perf_plot_ <- function(data_, value_, name_, color_,
                      format_func = function(x) {x},
                      hoverformat_ = ",.0f",
                      goal_ = NULL) {

    ax <- list(title = "", showticklabels = TRUE, showgrid = FALSE)
    ay <- list(title = "", showticklabels = FALSE, showgrid = FALSE, zeroline = FALSE, hoverformat = hoverformat_)

    value_ <- as.name(value_)
    data_ <- dplyr::rename(data_, value = !!value_)

    first <- data_[which.min(data_$Month), ]
    last <- data_[which.max(data_$Month), ]

    p <- plot_ly(type = "scatter", mode = "markers") %>%

    if (!is.null(goal_)) {
        p <- p %>%
            add_trace(data = data_,
                      x = ~Month, y = goal_,
                      name = "Goal",
                      line = list(color = LIGHT_RED, dash = "dot"),
                      mode = 'lines')
    }
    p %>%
        add_trace(data = data_,
                  x = ~Month, y = ~value,
                  name = name_,
                  line = list(color = color_),
                  mode = 'lines+markers',
                  marker = list(size = 8,
                                color = color_,
                                line = list(width = 1,
                                            color = 'rgba(255, 255, 255, 255, .8)'))) %>%
        add_annotations(x = first$Month,
                        y = first$value,
                        text = format_func(first$value),
                        showarrow = FALSE,
                        xanchor = "right",
                        xshift = -10) %>%
        add_annotations(x = last$Month,
                        y = last$value,
                        text = format_func(last$value),
                        font = list(size = 16),
                        showarrow = FALSE,
                        xanchor = "left",
                        xshift = 20) %>%
        layout(xaxis = ax,
               yaxis = ay,
               annotations = list(x = -.02,
                                  y = 0.4,
                                  xref = "paper",
                                  yref = "paper",
                                  xanchor = "right",
                                  text = name_,
                                  font = list(size = 12),
                                  showarrow = FALSE),
               showlegend = FALSE,
               margin = list(l = 120,
                             r = 40)) %>%
        plotly::config(displayModeBar = F)
}
perf_plot <- memoise(perf_plot_)


get_perf_plot_updates <- function(data.set, var_, name_, color_, 
                                  format_func = function(x) {x},
                                  hoverformat_ = ",.0f",
                                  zone_group_, month_) {
    
    refreshed.data.set <- filter(data.set, Corridor==zone_group_ & Month <= month_)
    
    if (nrow(refreshed.data.set) > 0) {

        first <- refreshed.data.set[which.min(refreshed.data.set$Month), ]
        last <- refreshed.data.set[which.max(refreshed.data.set$Month), ]
        
        traces <- list(list(x = refreshed.data.set$Month,
                            y = refreshed.data.set[[var_]],
                            mode = 'lines',
                            name = name_,
                            line = list(color = color_)))
        
        layouts <- list(yaxis = list(title = "", 
                                     showticklabels = FALSE, 
                                     showgrid = FALSE, 
                                     zeroline = FALSE, 
                                     hoverformat = hoverformat_),
                        annotations = list(
                            list(x = 0,
                                 y = first[[var_]],
                                 text = format_func(first[[var_]]),
                                 showarrow = FALSE,
                                 xanchor = "right",
                                 xref = "paper",
                                 width = 50,
                                 align = "right",
                                 borderpad = 5),
                            
                            list(x = 1,
                                 y = last[[var_]],
                                 text = format_func(last[[var_]]),
                                 font = list(size = 16),
                                 showarrow = FALSE,
                                 xanchor = "left",
                                 xref = "paper",
                                 width = 60,
                                 align = "left",
                                 borderpad = 5)
                        ),
                        margin = list(l = 50, #120
                                      r = 60,
                                      t = 10,
                                      b = 10),
                        showlegend = FALSE)
    } else {
        traces <- list(list(x = NULL,  #unique(data.set$Month),
                            y = NULL,
                            mode = 'none',
                            name = name_,
                            line = list(color = color_)))
        
        layouts <- list(yaxis = list(title = "", 
                                     showticklabels = FALSE, 
                                     showgrid = FALSE, 
                                     zeroline = FALSE, 
                                     hoverformat = hoverformat_),
                        annotations = NULL)
    }
    
    list(traces = traces, layouts = layouts)
}
#get_perf_plot_updates <- memoise(get_perf_plot_updates_)


no_data_plot_ <- function(name_) {
    
    ax <- list(title = "", showticklabels = FALSE, showgrid = FALSE, zeroline = FALSE)
    ay <- list(title = "", showticklabels = FALSE, showgrid = FALSE, zeroline = FALSE)
    
    plot_ly(type = "scatter", mode = "markers") %>%
        layout(xaxis = ax, 
               yaxis = ay,
               annotations = list(list(x = -.02,
                                       y = 0.4,
                                       xref = "paper",
                                       yref = "paper",
                                       xanchor = "right",
                                       text = name_,
                                       font = list(size = 12),
                                       showarrow = FALSE),
                                  list(x = 0.5,
                                       y = 0.5,
                                       xref = "paper",
                                       yref = "paper",
                                       text = "NO DATA",
                                       font = list(size = 16),
                                       showarrow = FALSE)),
               
               margin = list(l = 180,
                             r = 100)) %>% 
        plotly::config(displayModeBar = F)
    
}
no_data_plot <- memoise(no_data_plot_)

# Empty plot - space filler
x0 <- list(zeroline = FALSE, ticks = "", showticklabels = FALSE, showgrid = FALSE)
p0 <- plot_ly(type = "scatter", mode = "markers") %>% layout(xaxis = x0, yaxis = x0)


get_bar_line_dashboard_plot_ <- function(cor_weekly, 
                                         cor_monthly, 
                                         cor_hourly = NULL, 
                                         var_,
                                         num_format, # percent, integer, decimal
                                         highlight_color,
                                         month_, 
                                         zone_group_, 
                                         x_bar_title = "___",
                                         x_line1_title = "___",
                                         x_line2_title = "___",
                                         plot_title = "___ ",
                                         goal = NULL) {
    
    var_ <- as.name(var_)
    if (num_format == "percent") {
        var_fmt <- as_pct
        tickformat_ <- ".0%"
    } else if (num_format == "integer") {
        var_fmt <- as_int
        tickformat_ <- ",.0"
    } else if (num_format == "decimal") {
        var_fmt <- as_2dec
        tickformat_ <- ".2f"
    }
    
    highlight_color_ <- highlight_color
    
    mdf <- filter_mr_data(cor_monthly, zone_group_)
    wdf <- filter_mr_data(cor_weekly, zone_group_)
    
    mdf <- filter(mdf, Month == month_)
    wdf <- filter(wdf, Date < month_ + months(1))
    
    
    if (nrow(mdf) > 0 & nrow(wdf) > 0) {
        # Current Month Data
        mdf <- mdf %>%
            arrange(!!var_) %>%
            mutate(var = !!var_,
                   col = factor(ifelse(Corridor==zone_group_, DARK_GRAY_BAR, LIGHT_GRAY_BAR)),
                   Corridor = factor(Corridor, levels = Corridor))
        
        sdm <- SharedData$new(mdf, ~Corridor, group = "grp")
        
        bar_chart <- plot_ly(sdm,
                             type = "bar",
                             x = ~var, 
                             y = ~Corridor,
                             marker = list(color = ~col),
                             text = ~var_fmt(var),
                             textposition = "auto",
                             insidetextfont = list(color = "black"),
                             name = "",
                             customdata = ~glue(paste(
                                 "<b>{Description}</b>",
                                 "<br>{plot_title}: <b>{var_fmt(var)}</b>")),
                             hovertemplate = "%{customdata}",
                             hoverlabel = list(font = list(family = "Source Sans Pro"))
        ) %>%
            layout(
                barmode = "overlay",
                xaxis = list(title = x_bar_title, 
                             zeroline = FALSE, 
                             tickformat = tickformat_),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4,
                              l = 100)
            )
        if (!is.null(goal)) {
            bar_chart <- bar_chart %>% 
                add_lines(x = goal,
                          y = ~Corridor,
                          mode = "lines",
                          marker = NULL,
                          line = list(color = LIGHT_RED),
                          name = "Goal (95%)",
                          showlegend = FALSE)
        }
        
        # Weekly Data - historical trend
        wdf <- wdf %>%
            mutate(var = !!var_,
                   col = factor(ifelse(Corridor == zone_group_, 0, 1))) %>%
            group_by(Corridor)
        
        sdw <- SharedData$new(wdf, ~Corridor, group = "grp")
        
        weekly_line_chart <- plot_ly(sdw,
                                     type = "scatter",
                                     mode = "lines",
                                     x = ~Date, 
                                     y = ~var, 
                                     color = ~col, colors = c(BLACK, LIGHT_GRAY_BAR),
                                     alpha = 0.6,
                                     name = "",
                                     customdata = ~glue(paste(
                                         "<b>{Description}</b>",
                                         "<br>Week of: <b>{format(Date, '%B %e, %Y')}</b>",
                                         "<br>{plot_title}: <b>{var_fmt(var)}</b>")),
                                     hovertemplate = "%{customdata}",
                                     hoverlabel = list(font = list(family = "Source Sans Pro"))
        ) %>% 
            layout(xaxis = list(title = x_line1_title),
                   yaxis = list(tickformat = tickformat_,
                                hoverformat = tickformat_),
                   title = "__plot1_title__",
                   showlegend = FALSE,
                   margin = list(t = 50)
            )
        # if (!is.null(goal)) {
        #     weekly_line_chart <- weekly_line_chart  %>% 
        #         add_lines(x = ~Date,
        #                   y = goal,
        #                   color = I(LIGHT_RED),
        #                   name = "Goal (95%)")
        # }
        
        if (!is.null(cor_hourly)) {
            
            hdf <- filter_mr_data(cor_hourly, zone_group_)
            
            hdf <- filter(hdf, date(Hour) == month_)
            
            # Hourly Data - current month
            hdf <- hdf %>%
                mutate(var = !!var_,
                       col = factor(ifelse(Corridor == zone_group_, 0, 1))) %>%
                group_by(Corridor)
            
            sdh <- SharedData$new(hdf, ~Corridor, group = "grp")
            
            hourly_line_chart <- plot_ly(sdh) %>%
                add_lines(x = ~Hour,
                          y = ~var,
                          color = ~col, colors = c(BLACK, LIGHT_GRAY_BAR),
                          alpha = 0.6,
                          name = "",
                          customdata = ~glue(paste(
                              "<b>{Description}</b>",
                              "<br>Hour: <b>{format(Hour, '%l:%M %p')}</b>",
                              "<br>{plot_title}: <b>{var_fmt(var)}</b>")),
                          hovertemplate = "%{customdata}",
                          hoverlabel = list(font = list(family = "Source Sans Pro"))
                ) %>%
                layout(xaxis = list(title = x_line2_title),
                       yaxis = list(tickformat = tickformat_),
                       title = "__plot2_title__",
                       showlegend = FALSE) %>%
                highlight(
                    color = highlight_color_, 
                    opacityDim = 0.9, 
                    defaultValues = c(zone_group_),
                    selected = attrs_selected(
                        insidetextfont = list(color = "white"), 
                        textposition = "auto"),
                    on = "plotly_click",
                    off = "plotly_doubleclick")
            
            ax0 <- list(zeroline = FALSE, ticks = "", showticklabels = FALSE, showgrid = FALSE)
            p0 <- plot_ly(type="scatter", mode="markers") %>% layout(xaxis = ax0, yaxis = ax0)
            
            s1 <- subplot(weekly_line_chart, p0, hourly_line_chart, 
                          titleX = TRUE, heights = c(0.6, 0.1, 0.3), nrows = 3)
        } else {
            s1 <- weekly_line_chart
        }
        
        subplot(bar_chart, s1, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
            layout(margin = list(l = 100),
                   title = plot_title) %>%
            highlight(
                color = highlight_color_, 
                opacityDim = 0.9, 
                defaultValues = c(zone_group_),
                selected = attrs_selected(
                    insidetextfont = list(color = "white"), 
                    textposition = "auto"),
                on = "plotly_click",
                off = "plotly_doubleclick")
        
    } else(
        no_data_plot("")
    )
}
#get_bar_line_dashboard_plot <- memoise(get_bar_line_dashboard_plot_)
get_bar_line_dashboard_plot <- cmpfun(get_bar_line_dashboard_plot_)

get_tt_plot_ <- function(cor_monthly_tti, cor_monthly_tti_by_hr, 
                         cor_monthly_pti, cor_monthly_pti_by_hr, 
                         highlight_color = RED2,
                         month_,
                         zone_group_,
                         x_bar_title = "___",
                         x_line1_title = "___",
                         x_line2_title = "___",
                         plot_title = "___ ") {
    
    var_fmt <- as_2dec
    tickformat <- ".2f"
    
    mott <- full_join(cor_monthly_tti, cor_monthly_pti,
                      by = c("Corridor", "Zone_Group", "Month"),
                      suffix = c(".tti", ".pti"))  %>%
        filter(!is.na(Corridor)) %>%
        mutate(bti = pti - tti) %>%
        ungroup() %>%
        filter(Month < month_ + months(1)) %>%
        select(Corridor, Zone_Group, Month, tti, pti, bti)
    
    hrtt <- full_join(cor_monthly_tti_by_hr, cor_monthly_pti_by_hr,
                      by = c("Corridor", "Zone_Group", "Hour"),
                      suffix = c(".tti", ".pti"))  %>%
        filter(!is.na(Corridor)) %>%#,
        mutate(bti = pti - tti) %>%
        ungroup() %>%
        select(Corridor, Zone_Group, Hour, tti, pti, bti)
    
    mott <- filter_mr_data(mott, zone_group_)
    hrtt <- filter_mr_data(hrtt, zone_group_)
    
    mo_max <- round(max(mott$pti), 1) + .1
    hr_max <- round(max(hrtt$pti), 1) + .1
    
    if (nrow(mott) > 0 & nrow(hrtt) > 0) {    
        sdb <- SharedData$new(dplyr::filter(mott, Month == month_), 
                              ~Corridor, group = "grp")
        sdm <- SharedData$new(mott, 
                              ~Corridor, group = "grp")
        sdh <- SharedData$new(dplyr::filter(hrtt, date(Hour) == month_), 
                              ~Corridor, group = "grp")
        
        highlight_color_ <- RED2 # Colorbrewer red
        
        base_b <- plot_ly(sdb, color = I("gray")) %>%
            group_by(Corridor)
        base_m <- plot_ly(sdm, color = I("gray")) %>%
            group_by(Corridor)
        base_h <- plot_ly(sdh, color = I("gray")) %>%
            group_by(Corridor)
        
        pbar <- base_b %>%
            
            arrange(tti) %>%
            add_bars(x = ~tti, 
                     y = ~factor(Corridor, levels = Corridor),
                     text = ~as_2dec(tti),
                     color = I("gray"),
                     textposition = "auto",
                     insidetextfont = list(color = "black"),
                     name = "",
                     customdata = ~glue(paste(
                         "<b>{Corridor}</b>",
                         "<br>Travel Time Index: <b>{var_fmt(tti)}</b>",
                         "<br>Planning Time Index: <b>{var_fmt(pti)}</b>")),
                     hovertemplate = "%{customdata}",
                     hoverlabel = list(font = list(family = "Source Sans Pro"))
            ) %>%
            add_bars(x = ~bti,
                     y = ~factor(Corridor, levels = Corridor),
                     text = ~as_2dec(pti),
                     color = I(LIGHT_BLUE),
                     textposition = "auto",
                     insidetextfont = list(color = "black"),
                     hoverinfo = "none") %>%
            layout(
                barmode = "stack",
                xaxis = list(title = x_bar_title, 
                             zeroline = FALSE, 
                             tickformat = tickformat,
                             range = c(0, 2)),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4)
            )
        pttimo <- base_m %>%
            add_lines(x = ~Month, 
                      y = ~tti, 
                      alpha = 0.6,
                      name = "",
                      customdata = ~glue(paste(
                          "<b>{Corridor}</b>",
                          "<br><b>{format(Month, '%B %Y')}</b>",
                          "<br>Travel Time Index: <b>{var_fmt(tti)}</b>")),
                      hovertemplate = "%{customdata}",
                      hoverlabel = list(font = list(family = "Source Sans Pro"))
            ) %>%
            layout(xaxis = list(title = "Travel Time Index (TTI"),
                   yaxis = list(range = c(0.9, mo_max),
                                tickformat = tickformat),
                   showlegend = FALSE)
        
        pttihr <- base_h %>%
            add_lines(x = ~Hour,
                      y = ~tti,
                      alpha = 0.6,
                      name = "",
                      customdata = ~glue(paste(
                          "<b>{Corridor}</b>",
                          "<br>Hour: <b>{format(Hour, '%l:%M %p')}</b>",
                          "<br>Travel Time Index: <b>{var_fmt(tti)}</b>")),
                      hovertemplate = "%{customdata}",
                      hoverlabel = list(font = list(family = "Source Sans Pro"))
            ) %>%
            layout(xaxis = list(title = x_line1_title),
                   yaxis = list(range = c(0.9, hr_max),
                                tickformat = tickformat),
                   showlegend = FALSE)
        
        pptimo <- base_m %>%
            add_lines(x = ~Month, 
                      y = ~pti, 
                      alpha = 0.6,
                      name = "",
                      customdata = ~glue(paste(
                          "<b>{Corridor}</b>",
                          "<br><b>{format(Month, '%B %Y')}</b>",
                          "<br>Planning Time Index: <b>{var_fmt(pti)}</b>")),
                      hovertemplate = "%{customdata}",
                      hoverlabel = list(font = list(family = "Source Sans Pro"))) %>%
            layout(xaxis = list(title = "Planning Time Index (PTI)"),
                   yaxis = list(range = c(0.9, mo_max),
                                tickformat = tickformat),
                   showlegend = FALSE)
        
        pptihr <- base_h %>%
            add_lines(x = ~Hour,
                      y = ~pti,
                      alpha = 0.6,
                      name = "",
                      customdata = ~glue(paste(
                          "<b>{Corridor}</b>",
                          "<br>Hour: <b>{format(Hour, '%l:%M %p')}</b>",
                          "<br>Planning Time Index: <b>{var_fmt(pti)}</b>")),
                      hovertemplate = "%{customdata}",
                      hoverlabel = list(font = list(family = "Source Sans Pro"))
            ) %>%
            layout(xaxis = list(title = x_line2_title),
                   yaxis = list(range = c(0.9, hr_max),
                                tickformat = tickformat),
                   showlegend = FALSE)
        
        x0 <- list(zeroline = FALSE, ticks = "", showticklabels = FALSE, showgrid = FALSE)
        p0 <- plot_ly(type = "scatter", mode = "markers") %>% 
            layout(xaxis = x0, 
                   yaxis = x0)
        
        stti <- subplot(pttimo, p0, pttihr, 
                        titleX = TRUE, heights = c(0.6, 0.1, 0.3), nrows = 3)
        
        sbti <- subplot(pptimo, p0, pptihr, 
                        titleX = TRUE, heights = c(0.6, 0.1, 0.3), nrows = 3)
        
        subplot(pbar, stti, sbti, titleX = TRUE, widths = c(0.2, 0.4, 0.4), margin = 0.03) %>%
            layout(margin = list(l = 120, r = 80),
                   title = "Travel Time and Planning Time Index") %>%
            highlight(color = highlight_color_, 
                      opacityDim = 0.9, 
                      defaultValues = c(zone_group_),
                      selected = attrs_selected(
                          insidetextfont = list(color = "white"), 
                          textposition = "auto", base = 0),
                      on = "plotly_click",
                      off = "plotly_doubleclick")
        
    } else {
        no_data_plot("")
    }
}
#get_tt_plot <- memoise(get_tt_plot_)
get_tt_plot <- cmpfun(get_tt_plot_)

get_pct_ch_plot_ <- function(cor_monthly_vpd,
                             month_,
                             zone_group_) {
    pl <- function(df) { 
        neg <- dplyr::filter(df, delta < 0)
        pos <- dplyr::filter(df, delta > 0)
        
        title_ <- df$Corridor[1]
        
        plot_ly() %>% 
            add_bars(data = neg,
                     x = ~Month, 
                     y = ~delta, 
                     marker = list(color = "#e31a1c")) %>%
            add_bars(data = pos,
                     x = ~Month, 
                     y = ~delta, 
                     marker = list(color = "#33a02c")) %>%
            layout(xaxis = list(title = "",
                                nticks = nrow(df)),
                   yaxis = list(title = "",
                                tickformat = "%"),
                   showlegend = FALSE,
                   annotations = list(text = title_,
                                      font = list(size = 12),
                                      xref = "paper",
                                      yref = "paper",
                                      yanchor = "bottom",
                                      xanchor = "center",
                                      align = "center",
                                      x = 0.5,
                                      y = 0.95,
                                      showarrow = FALSE))
    }
    
    df <- filter_mr_data(cor_monthly_vpd, zone_group_)
    
    df <- filter(df, Month <= month_)
    
    if (nrow(df) > 0) {
        
        pcts <- split(df, df$Corridor)
        plts <- lapply(pcts[lapply(pcts, nrow)>0], pl)
        subplot(plts,
                margin = 0.03, nrows = min(length(plts), 4), shareX = TRUE, shareY = TRUE) %>%
            layout(title = "Percent Change from Previous Month (vehicles/day)",
                   margin = list(t = 60))
    } else {
        no_data_plot("")
    }
}
get_pct_ch_plot <- memoise(get_pct_ch_plot_)

get_vph_peak_plot_ <- function(df, chart_title, bar_subtitle, 
                               month_ = current_month(), zone_group_ = zone_group()) {
    
    df <- filter_mr_data(df, zone_group_)

    if (nrow(df) > 0) {
        
        sdw <- SharedData$new(dplyr::filter(df, Month <= month_), ~Corridor, group = "grp")
        sdm <- SharedData$new(dplyr::filter(df, Month == month_), ~Corridor, group = "grp")
        
        base <- plot_ly(sdw, color = I("gray")) %>%
            group_by(Corridor)
        base_m <- plot_ly(sdm, color = I("gray")) %>%
            group_by(Corridor)
        
        p1 <- base_m %>%
            summarise(vph = mean(vph)) %>% # This has to be just the current month's vph
            arrange(vph) %>%
            add_bars(x = ~vph, 
                     y = ~factor(Corridor, levels = Corridor),
                     text = ~scales::comma_format()(as.integer(vph)),
                     textposition = "inside",
                     textfont = list(color = "black"),
                     hoverinfo = "none") %>%
            layout(
                barmode = "overlay",
                xaxis = list(title = bar_subtitle, zeroline = FALSE),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4)
            )
        p2 <- base %>%
            add_lines(x = ~Month, y = ~vph, alpha = 0.6) %>%
            layout(xaxis = list(title = "Date"),
                   showlegend = FALSE,
                   annotations = list(text = chart_title,
                                      font = list(size = 12),
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
            highlight(color = "#256194", opacityDim = 0.9, defaultValues = c(zone_group_),
                      selected = attrs_selected(insidetextfont = list(color = "white"), textposition = "inside"))
    } else {
        no_data_plot("")
    }
}
get_vph_peak_plot <- memoise(get_vph_peak_plot_)

get_minmax_hourly_plot_ <- function(cor_monthly_vph, 
                                    month_ = current_month(), 
                                    zone_group_ = zone_group()) {
    
    mm_pl <- function(mm, tm) {
        
        title_ <- mm$Corridor[1]
        
        p <- plot_ly() %>%
            add_bars(data = mm,
                     x = ~Hour,
                     y = ~min_vph,
                     opacity = 0) %>%
            add_bars(data = mm,
                     x = ~Hour,
                     y = ~(max_vph-min_vph),
                     marker = list(color = "#a6cee3"))
        if (!is.null(tm)) {
            p <- p %>% add_markers(data = tm,
                                   x = ~Hour,
                                   y = ~vph,
                                   marker = list(color = "#ca0020"))
                
        }
        p %>%
            layout(barmode = "stack",
                   showlegend = FALSE,
                   xaxis = list(title = "",
                                tickformat = "%H:%M"),
                   yaxis = list(title = "",
                                range = c(0, round(max(minmax$max_vph), -3) + 1000)),
                   title = "Current Month (veh/hr) compared to range over previous months",
                   annotations = list(text = title_,
                                      font = list(size = 12),
                                      xref = "paper",
                                      yref = "paper",
                                      yanchor = "bottom",
                                      xanchor = "center",
                                      align = "center",
                                      x = 0.5,
                                      y = 0.85,
                                      showarrow = FALSE))
    }
    
    df <- filter_mr_data(cor_monthly_vph, zone_group_)
    
    df <- filter(df, date(Hour) <= month_)
    
    
    # Current Month Data
    this_month <- df %>% 
        filter(date(Hour) == month_) 
    
    if (nrow(df) > 0) {
        
        minmax <- df %>% 
            mutate(Hour = (Hour + (date(max(Hour)) - date(Hour)))) %>%
            group_by(Corridor, Hour) %>% 
            summarize(min_vph = min(vph), max_vph = max(vph))
        
        mms <- split(minmax, minmax$Corridor)
        tms <- split(this_month, this_month$Corridor)
        mms_ <- mms[lapply(mms, nrow)>0]
        tms_ <- tms[lapply(mms, nrow)>0]
        
        plts <- lapply(seq_along(mms_), function(i) mm_pl(mms_[[i]], tms_[[i]]))
        subplot(plts, 
                margin = 0.02, nrows = min(length(plts), 4), shareX = TRUE, shareY = TRUE) %>%
            layout(title = "Current Month (veh/hr) compared to range over previous months",
                   yaxis = list(title = "vph"),
                   margin = list(t = 60))
    } else {
        no_data_plot("")
    }
}
get_minmax_hourly_plot <- memoise(get_minmax_hourly_plot_)


det_uptime_bar_plot_ <- function(df, xtitle, month_) {
    
    data = df %>%
        dplyr::filter(month(X)==month_) %>%
        group_by(Corridor) %>%
        summarize(Uptime = mean(Y)) %>%
        arrange(Uptime)
    
    
    plot_ly(data) %>%
        add_bars(x = ~Uptime,
                 y = ~factor(Corridor, levels = Corridor),
                 color = I(BLUE),
                 text = ~scales::percent(Uptime),
                 textposition = "inside",
                 textfont = list(color = "white"),
                 #hoverinfo = "y+x",
                 customdata = ~glue(paste(
                     "<b>{Description}</b>",
                     "<br>{plot_title}: <b>{var_fmt(var)}</b>")),
                 hovertemplate = "%{customdata}",
                 hoverlabel = list(font = list(family = "Source Sans Pro")),
                 showlegend = FALSE) %>%
        layout(
            barmode = "overlay",
            xaxis = list(title = xtitle,
                         zeroline = FALSE,
                         tickformat = "%"),
            yaxis = list(title = ""),
            showlegend = FALSE,
            font = list(size = 11),
            margin = list(pad = 4),
            paper_bgcolor = "#f0f0f0",
            plot_bgcolor = "#f0f0f0"
        )
}
det_uptime_bar_plot <- memoise(det_uptime_bar_plot_)

# Subplots
det_uptime_line_plot_ <- function(df, corr, showlegend_) {
    
    plot_ly(data = df) %>%
        add_lines(x = ~X,
                  y = ~Y,
                  color = ~C,
                  colors = cols,
                  legendgroup = ~C,
                  customdata = ~glue(paste(
                      "<b>{Description}</b>",
                      "<br>{format(Date, '%B %e, %Y')}</b>",
                      "<br><b>{as_pct(uptime)}</b>")),
                  hovertemplate = "%{customdata}",
                  hoverlabel = list(font = list(family = "Source Sans Pro")),
                  showlegend = showlegend_) %>%
        layout(yaxis = list(title = "",
                            range = c(0, 1.1),
                            tickformat = "%"),
               xaxis = list(title = ""),
               annotations = list(text = corr,
                                  font = list(size = 14),
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "left",
                                  align = "center",
                                  x = 0.1,
                                  y = 0.1,
                                  showarrow = FALSE),
               plot_bgcolor = "#ffffff")
}
det_uptime_line_plot <- memoise(det_uptime_line_plot_)

get_cor_det_uptime_plot_ <- function(avg_daily_uptime, 
                                     avg_monthly_uptime,
                                     month_,
                                     zone_group_,
                                     month_name) {
    
    # Plot Detector Uptime for a Corridor. The basis of subplot.
    plot_detector_uptime <- function(df, corr, showlegend_) {
        plot_ly(data = df) %>% 
            add_lines(x = ~Date, 
                      y = ~uptime.pr,
                      color = I(LIGHT_BLUE),
                      name = "Presence",
                      legendgroup = "Presence",
                      showlegend = showlegend_) %>%
            add_lines(x = ~Date, 
                      y = ~uptime.sb,
                      color = I(BLUE),
                      name = "Setback",
                      legendgroup = "Setback",
                      showlegend = showlegend_) %>%
            add_lines(x = ~Date,
                      y = 0.95,
                      color = I(LIGHT_RED),
                      name = "Goal (95%)",
                      legendgroup = "Goal",
                      showlegend = showlegend_) %>%
            layout(yaxis = list(title = "",
                                range = c(0, 1.1),
                                tickformat = "%"),
                   xaxis = list(title = ""),
                   annotations = list(text = corr,
                                      xref = "paper",
                                      yref = "paper",
                                      yanchor = "bottom",
                                      xanchor = "left",
                                      align = "center",
                                      x = 0.1,
                                      y = 0.95,
                                      showarrow = FALSE))
    }
    plot_detector_uptime_bar <- function(df) {
        
        df <- df %>% 
            rename(uptime = uptime) %>%
            arrange(uptime) %>% ungroup() %>%
            mutate(col = factor(ifelse(Corridor==zone_group_, DARK_GRAY_BAR, LIGHT_GRAY_BAR)),
                   Corridor = factor(Corridor, levels = Corridor))
        
        plot_ly(data = arrange(df, uptime),
                marker = list(color = ~col)) %>%
            add_bars(
                x = ~uptime, 
                y = ~Corridor,
                text = ~as_pct(uptime),
                textposition = "auto",
                insidetextfont = list(color = "black"),
                #hoverinfo = "y+x",
                showlegend = FALSE,
                name = "",
                customdata = ~glue(paste(
                 "<b>{Description}</b>",
                 "<br>Uptime: <b>{as_pct(uptime)}</b>")),
                hovertemplate = "%{customdata}",
                hoverlabel = list(font = list(family = "Source Sans Pro"))) %>%
            add_lines(x = ~0.95,
                      y = ~Corridor,
                      mode = "lines",
                      marker = NULL,
                      color = I(LIGHT_RED),
                      name = "Goal (95%)",
                      legendgroup = "Goal",
                      showlegend = FALSE) %>%
            
            layout(
                barmode = "overlay",
                xaxis = list(title = paste(month_name, "Detector Uptime (%)"), 
                             zeroline = FALSE,
                             tickformat = "%"),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4,
                              l = 100)
            )
    }
    
    #avg_daily_uptime <- filter_mr_data(avg_daily_uptime, zone_group_)
    #avg_daily_uptime <- filter(avg_daily_uptime, Date <= month_ + months(1))
    
    avg_daily_uptime <- filter_mr_data(avg_daily_uptime, zone_group_) %>%
        filter(Date <= month_ + months(1))
    
    avg_monthly_uptime <- filter_mr_data(avg_monthly_uptime, zone_group_) %>%
        filter(Month == month_)

    if (nrow(avg_daily_uptime) > 0) {
        # Create Uptime by Detector Type (Setback, Presence) by Corridor Subplots.
        cdfs <- split(avg_daily_uptime, avg_daily_uptime$Corridor)
        cdfs <- cdfs[lapply(cdfs, nrow)>0]
        
        p1 <- plot_detector_uptime_bar(avg_monthly_uptime) #(avg_daily_uptime)
        
        plts <- lapply(seq_along(cdfs), function(i) { 
            plot_detector_uptime(cdfs[[i]], names(cdfs)[i], ifelse(i==1, TRUE, FALSE)) 
        })
        s2 <- subplot(plts, nrows = min(length(plts), 4), shareX = TRUE, shareY = TRUE, which_layout = 1)
        subplot(p1, s2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
            layout(margin = list(l = 100))
    } else {
        no_data_plot("")
    }
}
get_cor_det_uptime_plot <- memoise(get_cor_det_uptime_plot_)

get_cor_comm_uptime_plot_ <- function(avg_daily_uptime,
                                      avg_monthly_uptime,
                                      month_,
                                      zone_group_,
                                      month_name) {
    
    # Plot Detector Uptime for a Corridor. The basis of subplot.
    plot_detector_uptime <- function(df, corr, showlegend_) {
        plot_ly(data = df) %>% 
            add_lines(x = ~Date, 
                      y = ~uptime, #
                      color = I(BLUE),
                      name = "Uptime", #
                      legendgroup = "Uptime", #
                      showlegend = showlegend_) %>%
            add_lines(x = ~Date,
                      y = 0.95,
                      color = I(LIGHT_RED),
                      name = "Goal (95%)",
                      legendgroup = "Goal",
                      showlegend = showlegend_) %>%
            layout(yaxis = list(title = "",
                                range = c(0, 1.1),
                                tickformat = "%"),
                   xaxis = list(title = ""),
                   annotations = list(text = corr,
                                      xref = "paper",
                                      yref = "paper",
                                      yanchor = "bottom",
                                      xanchor = "left",
                                      align = "center",
                                      x = 0.1,
                                      y = 0.95,
                                      showarrow = FALSE))
    }
    plot_detector_uptime_bar <- function(df) {
        
        df <- df %>% 
            arrange(uptime) %>% ungroup() %>%
            mutate(col = factor(ifelse(Corridor==zone_group_, DARK_GRAY_BAR, LIGHT_GRAY_BAR)),
                   Corridor = factor(Corridor, levels = Corridor))
        
        plot_ly(data = arrange(df, uptime),
                marker = list(color = ~col)) %>%
            add_bars(
                x = ~uptime, 
                y = ~Corridor,
                text = ~as_pct(uptime),
                textposition = "auto",
                insidetextfont = list(color = "black"),
                #hoverinfo = "y+x",
                showlegend = FALSE,
                name = "",
                customdata = ~glue(paste(
                    "<b>{Description}</b>",
                    "<br>Uptime: <b>{as_pct(uptime)}</b>")),
                hovertemplate = "%{customdata}",
                hoverlabel = list(font = list(family = "Source Sans Pro"))) %>%
            add_lines(x = ~0.95,
                      y = ~Corridor,
                      mode = "lines",
                      marker = NULL,
                      color = I(LIGHT_RED),
                      name = "Goal (95%)",
                      legendgroup = "Goal",
                      showlegend = FALSE) %>%
            
            layout(
                barmode = "overlay",
                xaxis = list(title = paste(month_name, "Comms Uptime (%)"), 
                             zeroline = FALSE,
                             tickformat = "%"),
                yaxis = list(title = ""),
                showlegend = FALSE,
                font = list(size = 11),
                margin = list(pad = 4,
                              l = 100)
            )
    }
    
    avg_daily_uptime <- filter_mr_data(avg_daily_uptime, zone_group_)
    avg_monthly_uptime <- filter_mr_data(avg_monthly_uptime, zone_group_)

    avg_daily_uptime <- filter(avg_daily_uptime, Date <= month_ + months(1))
    avg_monthly_uptime <- filter(avg_monthly_uptime, Month == month_)
    

    if (nrow(avg_daily_uptime) > 0) {
        # Create Uptime by Detector Type (Setback, Presence) by Corridor Subplots.
        cdfs <- split(avg_daily_uptime, avg_daily_uptime$Corridor)
        cdfs <- cdfs[lapply(cdfs, nrow)>0]
        
        p1 <- plot_detector_uptime_bar(avg_monthly_uptime)
        
        plts <- lapply(seq_along(cdfs), function(i) { 
            plot_detector_uptime(cdfs[[i]], names(cdfs)[i], ifelse(i==1, TRUE, FALSE)) 
        })
        s2 <- subplot(plts, nrows = min(length(plts), 4), shareX = TRUE, shareY = TRUE, which_layout = 1)
        subplot(p1, s2, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
            layout(margin = list(l = 100))
    } else {
        no_data_plot("")
    }
}
get_cor_comm_uptime_plot <- memoise(get_cor_comm_uptime_plot_)

# Reshape to show multiple on the same chart
gather_outstanding_events <- function(cor_monthly_events) {
    
    cor_monthly_events %>% 
        ungroup() %>% 
        select(Zone_Group, Corridor, Month, Reported, Resolved, Outstanding) %>%
        gather(Events, Status, -c(Month, Corridor, Zone_Group))
}

plot_teams_tasks_ <- function(tab, var_,
                              title_ = "", textpos = "auto", height_ = 300) { #
    
    var_ <- as.name(var_)
    
    tab <- tab %>% 
        mutate(var = fct_reorder(!!var_, Reported))
    
    p1 <- plot_ly(data = tab, height = height_) %>%
        add_bars(x = ~Reported,  # ~Rep, 
                 y = ~var,
                 color = I(LIGHT_BLUE),
                 name = "Reported",
                 text = ~Reported, #  ~Rep,
                 textposition = textpos,
                 insidetextfont = list(size = 11, color = "black"),
                 outsidetextfont = list(size = 11)) %>%
        layout(margin = list(l = 180),
               showlegend = FALSE,
               yaxis = list(title = ""),
               xaxis = list(title = "Reported",
                            zeroline = FALSE),
               margin = list(pad = 4))
    p2 <- plot_ly(data = tab) %>%
        add_bars(x = ~Resolved,  # ~Res, 
                 y = ~var,
                 color = I(BLUE),
                 name = "Resolved",
                 text = ~Resolved,  # ~Res,
                 textposition = textpos,
                 insidetextfont = list(size = 11, color = "white"),
                 outsidetextfont = list(size = 11)) %>%
        layout(margin = list(l = 180),
               showlegend = FALSE,
               yaxis = list(title = ""),
               xaxis = list(title = "Resolved",
                            zeroline = FALSE),
               margin = list(pad = 4))
    p3 <- plot_ly(data = tab) %>% 
        add_bars(x = ~Outstanding,  # ~outstanding, 
                 y = ~var, 
                 color = I(ORANGE),
                 name = "Outstanding",
                 text = ~Outstanding, 
                 textposition = textpos, 
                 insidetextfont = list(size = 11, color = "white"),
                 outsidetextfont = list(size = 11)) %>%
        layout(margin = list(l = 180),
               showlegend = FALSE,
               yaxis = list(title = ""),
               xaxis = list(title = "Cum. Outstanding",
                            zeroline = FALSE),
               margin = list(pad = 4))
    subplot(p1, p2, p3, shareY = TRUE, shareX = TRUE) %>% 
        layout(title = list(text = title_, font = list(size = 12)))
}
plot_teams_tasks <- memoise(plot_teams_tasks_)

plot_tasks_ <- function(type_plot, source_plot, priority_plot, 
                        month_name = input$month) {
    
    p1 <- subplot(type_plot, source_plot, priority_plot, 
                  heights = c(0.42, 0.42, 0.16), nrows = 3, margin = 0.05) %>% 
        layout(paper_bgcolor = "#f0f0f0", 
               annotations = list(
                   list(text = paste("Tasks by Type -", month_name), 
                        font = list(size = 11), 
                        xref = "paper", 
                        yref = "paper", 
                        yanchor = "bottom",
                        xanchor = "center",
                        x = 0.5, 
                        y = 1.0,
                        showarrow = FALSE,
                        showlegend = FALSE), 
                   list(text = paste("Tasks by Source -", month_name), 
                        font = list(size = 11), 
                        xref = "paper", 
                        yref = "paper", 
                        yanchor = "top",
                        xanchor = "center",
                        x = 0.5, 
                        y = 0.58,
                        showarrow = FALSE,
                        showlegend = FALSE), 
                   list(text = paste("Tasks by Priority -", month_name), 
                        font = list(size = 11), 
                        xref = "paper", 
                        yref = "paper", 
                        yanchor = "top",
                        xanchor = "center",
                        x = 0.5, 
                        y = 0.16,
                        showarrow = FALSE,
                        showlegend = FALSE)))
    
    p2 <- subtype_plot %>% 
        layout(annotations = list(text = paste("Tasks by Subtype -", month_name), 
                                  font = list(size = 11), 
                                  xref = "paper", 
                                  yref = "paper", 
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  x = 0.5, 
                                  y = 1.0,
                                  showarrow = FALSE))
    
    subplot(p1, p2, nrows = 1, margin = 0.1)
}
plot_tasks <- memoise(plot_tasks_)

# Number reported and resolved in each month. Side-by-side.
cum_events_plot_ <- function(df) {
    plot_ly(height = 300) %>%
        add_bars(data = filter(df, Events == "Reported"),
                 x = ~Month,
                 y = ~Status,
                 name = "Reported",
                 marker = list(color = LIGHT_BLUE)) %>%
        add_bars(data = filter(df, Events == "Resolved"),
                 x = ~Month,
                 y = ~Status,
                 name = "Resolved",
                 marker = list(color = BLUE)) %>%
        add_trace(data = filter(df, Events == "Outstanding"),
                  x = ~Month,
                  y = ~Status,
                  type = 'scatter', mode = 'lines', name = 'Outstanding', #fill = 'tozeroy',
                  line = list(color = ORANGE)) %>%
        add_trace(data = filter(df, Events == "Outstanding"),
                  x = ~Month,
                  y = ~Status,
                  type = 'scatter',
                  mode = 'markers', 
                  name = 'Outstanding',
                  marker = list(color = ORANGE),
                  showlegend = FALSE) %>%
        layout(barmode = "group",
               yaxis = list(title = "Events"),
               xaxis = list(title = ""),
               legend = list(x = 0.5, y = 0.9, orientation = "h"))
}
cum_events_plot <- memoise(cum_events_plot_)



# plot_individual_cctvs_ <- function(daily_cctv_df, 
#                                    month_ = current_month(), 
#                                    zone_group_ = zone_group()) {
#     
#     #df <- filter(daily_cctv_df, Date < month_ + months(1))
#     #df <- filter(df, Zone_Group == zone_group_)
#     
#     spr <- daily_cctv_df %>%
#         filter(Date < month_ + months(1),
#                Zone_Group == zone_group_, 
#                Corridor != Zone_Group) %>% 
#         rename(CameraID = Corridor, 
#                Corridor = Zone_Group) %>%
#         dplyr::select(-c(num, uptime, Corridor, Name, delta, Week)) %>% 
#         distinct() %>% 
#         spread(Date, up, fill = 0) %>%
#         arrange(desc(CameraID))
#     
#     m <- as.matrix(spr %>% dplyr::select(-CameraID))
#     row.names(m) <- spr$CameraID
#     m <- round(m,0)
#     
#     plot_ly(x = colnames(m), 
#             y = row.names(m), 
#             z = m, 
#             colors = c(LIGHT_GRAY_BAR, BROWN),
#             type = "heatmap",
#             #xgap = 1,
#             ygap = 1,
#             showscale = FALSE) %>% 
#         layout(yaxis = list(type = "category",
#                             title = ""),
#                margin = list(l = 150))
# }
plot_individual_cctvs_ <- function(daily_cctv_df, 
                                   month_ = current_month(), 
                                   zone_group_ = zone_group()) {
    
    spr <- daily_cctv_df %>%
        filter(Date < month_ + months(1),
               Zone_Group == zone_group_, 
               as.character(Corridor) != as.character(Zone_Group)) %>% 
        rename(CameraID = Corridor, 
               Corridor = Zone_Group) %>%
        dplyr::select(CameraID, Description, Date, up) %>%
        distinct() %>% 
        spread(Date, up, fill = 0) %>%
        arrange(desc(CameraID))
    
    m <- as.matrix(spr %>% dplyr::select(-CameraID, -Description))
    m <- round(m,0)
    
    row_names <- spr$CameraID
    col_names <- colnames(m)
    colnames(m) <- NULL
    
    status <- function(x) {
        options <- c("Camera Down", "Working at encoder, but not 511", "Working on 511")
        options[x+1]
    }
    bind_description <- function(camera_status) {
        glue("<b>{as.character(spr$Description)}</b><br>{camera_status}")
    }
    
    plot_ly(x = col_names, 
            y = row_names, 
            z = m, 
            colors = c(LIGHT_GRAY_BAR, "#e48c5b", BROWN),
            type = "heatmap",
            ygap = 1,
            showscale = FALSE,
            customdata = apply(apply(apply(m, 2, status), 2, bind_description), 1, as.list),
            hovertemplate="<br>%{customdata}<br>%{x}<extra></extra>",
            hoverlabel = list(font = list(family = "Source Sans Pro"))) %>% 
        layout(yaxis = list(type = "category",
                            title = ""),
               margin = list(l = 150))
}
plot_individual_cctvs <- memoise(plot_individual_cctvs_)



uptime_heatmap <- function(df_,
                           var_,
                           month_ = current_month(), 
                           zone_group_ = zone_group()) {
    
    start_date <- floor_date(min(df_$Date), "month")
    end_date <- ceiling_date(max(df_$Date), "month") - days(1)
    
    fill_func <- function(names_) { 
        setNames(as.list(rep(0, length(names_))), names_)
    }
    
    var <- as.name(var_)
    spr <- df_ %>% 
        filter(
            Date < month_ + months(1),
            Zone_Group == zone_group_) %>%
        select(SignalID = Corridor, Description, Date, !!var) %>%
        distinct() %>%
        complete(nesting(SignalID, Description), 
                 Date = seq(start_date, end_date, by = "days"),
                 fill = fill_func(names(df_))[as.character(var)]) %>%
        
        group_by(SignalID, Description, Date) %>% 
        summarize(!!var := sum(!!var)) %>% 
        ungroup() %>%
        spread(Date, !!var, fill = 0) %>% 
        arrange(desc(SignalID))
    
    m <- as.matrix(spr %>% select(-SignalID, -Description))
    
    row_names <- spr$SignalID
    col_names <- colnames(m)
    colnames(m) <- NULL
    
    bind_description <- function(m) {
        glue("<b>{as.character(spr$Description)}</b><br>Uptime: <b>{as_pct(m)}</b>")
    }
    
    plot_ly(x = col_names, 
            y = row_names,
            z = m, 
            colors = colorRamp(c("white", BLUE)),
            type = "heatmap",
            ygap = 1,
            showscale = FALSE,
            #name = "",
            customdata = apply(apply(m, 2, bind_description), 1, as.list),
            hovertemplate="<br>%{customdata}<br>%{x}<extra></extra>",
            hoverlabel = list(font = list(family = "Source Sans Pro"))) %>% 
        layout(yaxis = list(type = "category",
                            title = ""),
               showlegend = FALSE,
               margin = list(l = 150))
}


get_uptime_plot_ <- function(daily_df,
                             monthly_df,
                             var_,
                             num_format, # percent, integer, decimal
                             month_, 
                             zone_group_, 
                             x_bar_title = "___",
                             x_line1_title = "___",
                             x_line2_title = "___",
                             plot_title = "___ ",
                             goal = NULL) {
    
    var <- as.name(var_)
    if (num_format == "percent") {
        var_fmt <- as_pct
        tickformat_ <- ".0%"
    } else if (num_format == "integer") {
        var_fmt <- as_int
        tickformat_ <- ",.0"
    } else if (num_format == "decimal") {
        var_fmt <- as_2dec
        tickformat_ <- ".2f"
    }
    
    if (nrow(daily_df) > 0 & nrow(monthly_df) > 0) {
        
        monthly_df_ <- monthly_df %>%
            filter(Month == month_,
                   Zone_Group == zone_group_) %>%
            arrange(desc(Corridor)) %>%
            mutate(var = !!var,
                   col = factor(ifelse(Corridor==zone_group_, DARK_GRAY_BAR, LIGHT_GRAY_BAR)),
                   Corridor = factor(Corridor, levels = Corridor))
        
        if (nrow(monthly_df_) > 0) {
            # Current Month Data
            bar_chart <- plot_ly(monthly_df_,
                                 type = "bar",
                                 x = ~var, #uptime.all, 
                                 y = ~Corridor,
                                 marker = list(color = ~col),
                                 text = ~var_fmt(var),
                                 textposition = "auto",
                                 insidetextfont = list(color = "black"),
                                 name = "",
                                 customdata = ~glue(paste(
                                     "<b>{Description}</b>",
                                     "<br>Uptime: <b>{var_fmt(var)}</b>")),
                                 hovertemplate = "%{customdata}",
                                 hoverlabel = list(font = list(family = "Source Sans Pro"))) %>% 
                layout(
                    barmode = "overlay",
                    xaxis = list(title = x_bar_title, 
                                 zeroline = FALSE, 
                                 tickformat = tickformat_),
                    yaxis = list(title = ""),
                    showlegend = FALSE,
                    font = list(size = 11),
                    margin = list(pad = 4,
                                  l = 100,
                                  r = 50),
                    shapes=list(type = 'line', 
                                x0 = goal/0.2, 
                                x1 = goal/0.2, 
                                y0 = min(levels(monthly_df_$Corridor)), 
                                y1 = max(levels(monthly_df_$Corridor)), 
                                line = list(dash = 'dot', width = 1, color = RED))
                )
            
            # Daily Heatmap
            daily_heatmap <- uptime_heatmap(daily_df, 
                                            var,
                                            month_,
                                            zone_group_)
            
            
            
            subplot(bar_chart, daily_heatmap, titleX = TRUE, widths = c(0.2, 0.8), margin = 0.03) %>%
                layout(margin = list(l = 100),
                       title = plot_title) 
        } else {
            no_data_plot("")
        }
    } else {
        no_data_plot("")
    }
}
#get_uptime_plot <- memoise(get_uptime_plot_)
get_uptime_plot <- cmpfun(get_uptime_plot_)

# No longer used
plot_cctvs <- function(df, month_) {
    
    start_date <- ymd("2018-02-01")
    end_date <- month_ %m+% months(1) - days(1)
    
    df_ <- filter(df, Date >= start_date & Date <= end_date & Size > 0)
    
    if (nrow(df) > 0) {
        
        #date_range <- ymd(date_range)
        
        p <- ggplot() + 
            
            # tile plot
            geom_tile(data = df, 
                      aes(x = Date, 
                          y = CameraID), 
                      fill = "steelblue", 
                      color = "white") + 
            
            # fonts, text size and labels and such
            theme(panel.grid.major = element_blank(),
                  panel.grid.minor = element_blank(),
                  axis.ticks.x = element_line(color = "gray50"),
                  axis.text.x = element_text(size = 11),
                  axis.text.y = element_text(size = 11),
                  axis.ticks.y = element_blank(),
                  axis.title = element_text(size = 11)) +
            scale_x_date(position = "top", limits = c(start_date, end_date)) +
            labs(x = "", 
                 y = "Camera") +
            
            # draw white gridlines between tick labels
            geom_vline(xintercept = as.numeric(seq(start_date, end_date, by = "1 day")) - 0.5, 
                       color = "white")
        
        if (length(unique(df$CameraID)) > 1) {
            p <- p +
                geom_hline(yintercept = seq(1.5, length(unique(df$CameraID)) - 0.5, by = 1), 
                           color = "white")
            
        }
        p
    }
}

volplot_plotly <- function(df, title = "title", ymax = 1000) {
    
    if (is.null(ymax)) {
        yax <- list(rangemode = "tozero", tickformat = ",.0")
    } else {
        yax <- list(range = c(0, ymax), tickformat = ",.0")
    } 
    
    # Works but colors and labeling are not fully complete.
    pl <- function(dfi) {
        plot_ly(data = dfi) %>% 
            add_ribbons(x = ~Timeperiod, 
                        ymin = 0,
                        ymax = ~vol,
                        color = ~CallPhase,
                        colors = colrs,
                        name = paste('Phase', dfi$CallPhase[1])) %>%
            layout(yaxis = yax,
                   annotations = list(x = -.05,
                                      y = 0.5,
                                      xref = "paper",
                                      yref = "paper",
                                      xanchor = "right",
                                      text = paste0("[", dfi$Detector[1], "]"),
                                      font = list(size = 12),
                                      showarrow = FALSE)
            )
    }
    
    dfs <- split(df, df$Detector)
    
    plts <- lapply(dfs[lapply(dfs, nrow)>0], pl)
    subplot(plts, nrows = length(plts), shareX = TRUE) %>%
        layout(annotations = list(text = title,
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1,
                                  showarrow = FALSE))
}


volplot_plotly2 <- function(db, signalid, plot_start_date, plot_end_date, title = "title", ymax = 1000) {
    # db is either conf$athena (list) or aurora (Pool)
    
    if (is.null(ymax)) {
        yax <- list(rangemode = "tozero", tickformat = ",.0")
    } else {
        yax <- list(range = c(0, ymax), tickformat = ",.0")
    } 
    
    # Works. fill_colr is still an enigma, but it works as is.
    pl <- function(dfi, i) {
        
        dfi <- dfi %>% 
            mutate(maxy = if_else(bad_day==1, as.integer(max(1,max(vol_rc, na.rm = TRUE))), as.integer(0)),
                   colr = colrs[CallPhase], 
                   fill_colr = "")
        
        plot_ly(data = dfi) %>%
            add_trace(
                x = ~Timeperiod, 
                y = ~vol_rc, 
                type = "scatter", 
                mode = "lines", 
                fill = "tozeroy",
                line = list(color = ~colr),
                fillcolor = ~fill_colr,
                name = paste('Phase', dfi$CallPhase[1]),
                customdata = ~glue(paste(
                    "<b>Detector: {Detector}</b>",
                    "<br>{format(ymd_hms(Timeperiod), '%a %d %B %I:%M %p')}",
                    "<br>Volume: <b>{as_int(vol_rc)}</b>")),
                hovertemplate = "%{customdata}",
                hoverlabel = list(font = list(family = "Source Sans Pro")),
                showlegend = FALSE) %>%
            add_trace(
                x = ~Timeperiod, 
                y = ~vol_ac, 
                type = "scatter", 
                mode = "lines", 
                #fill = "tozeroy",
                line = list(color = DARK_GRAY),
                #fillcolor = ~fill_colr,
                name = "Adjusted Count",
                customdata = ~glue(paste(
                    "<b>Detector: {Detector}</b>",
                    "<br>{format(ymd_hms(Timeperiod), '%a %d %B %I:%M %p')}",
                    "<br>Volume: <b>{as_int(vol_ac)}</b>")),
                hovertemplate = "%{customdata}",
                hoverlabel = list(font = list(family = "Source Sans Pro")),
                showlegend = (i==1)) %>%
            add_trace(
                x = ~Timeperiod,
                y = ~maxy,
                type = "scatter",
                mode = "lines",
                fill = "tozeroy",
                line = list(
                    color = "rgba(0,0,0,0.1)",
                    shape = "vh"),
                fillcolor = "rgba(0,0,0,0.2)",
                name = "Bad Days",
                
                customdata = ~glue(paste(
                    "<b>Detector: {Detector}</b>",
                    "<br>{format(date(Timeperiod), '%a %d %B %Y')}",
                    "<br><b>{if_else(bad_day==1, 'Bad Day', 'Good Day')}</b>")),
                hovertemplate = "%{customdata}",
                hoverlabel = list(font = list(family = "Source Sans Pro")),
                showlegend = (i==1)) %>%
            layout(yaxis = yax,
                   annotations = list(x = -.03,
                                      y = 0.5,
                                      xref = "paper",
                                      yref = "paper",
                                      xanchor = "right",
                                      text = paste0("Det #", dfi$Detector[1]),
                                      font = list(size = 12),
                                      showarrow = FALSE))
    }
    # db = conf$athena
    withProgress(message = "Loading chart", value = 0, {
        athena <- get_athena_connection(db)
        incProgress(amount = 0.1)
        rc <- tbl(athena, sql(glue(paste(
            "SELECT signalid, date, timeperiod, detector, callphase, vol",
            "FROM {db$database}.counts_1hr",
            "WHERE signalid = '{signalid}'",
            "AND date BETWEEN '{plot_start_date}' AND '{plot_end_date}'"))))
        incProgress(amount = 0.1)
        fc <- tbl(athena, sql(glue(paste(
            "SELECT signalid, date, timeperiod, detector, callphase, vol",
            "FROM {db$database}.filtered_counts_1hr",
            "WHERE signalid = '{signalid}'",
            "AND date BETWEEN '{plot_start_date}' AND '{plot_end_date}'"))))
        incProgress(amount = 0.1)
        ac <- tbl(athena, sql(glue(paste(
            "SELECT signalid, date, timeperiod, detector, callphase, vol",
            "FROM {db$database}.adjusted_counts_1hr",
            "WHERE signalid = '{signalid}'",
            "AND date BETWEEN '{plot_start_date}' AND '{plot_end_date}'"))))
        incProgress(amount = 0.2)
        df <- list(
            rename(rc, vol_rc = vol),
            rename(fc, vol_fc = vol),
            rename(ac, vol_ac = vol)) %>%
            reduce(full_join, by = c("signalid", "date", "timeperiod", "detector", "callphase")
            ) %>%
            mutate(bad_day = if_else(is.na(vol_fc), TRUE, FALSE)) %>% 
            #select(-date, -vol_fc) %>% 
            collect() %>%
            transmute(
                SignalID = factor(signalid), 
                Timeperiod = timeperiod, 
                Detector = factor(as.integer(detector)), 
                CallPhase = factor(callphase),
                vol_rc = as.integer(vol_rc),
                vol_ac = ifelse(is.na(vol_fc), as.integer(vol_ac), NA),
                bad_day) %>%
            arrange(SignalID, Detector, Timeperiod)
        incProgress(amount = 0.4)
    })
        

    
    dfs <- split(df, df$Detector)
    dfs <- dfs[lapply(dfs, nrow)>0]
    names(dfs) <- NULL
    
    plts <- purrr::imap(dfs, ~pl(.x, .y))
    
    #plts <- lapply(dfs[lapply(dfs, nrow)>0], pl)
    subplot(plts, nrows = length(plts), shareX = TRUE) %>%
        layout(annotations = list(text = title,
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1,
                                  showarrow = FALSE),
               showlegend = TRUE,
               margin = list(l = 120),
               xaxis = list(
                   type = 'date'))
}


udcplot_plotly <- function(hourly_udc) {
    
    corridor_udc_plot <- function(hourly_udc, i) {
        
        current_month <- date(max(hourly_udc$Month))  # hourly_udc$month_hour))  # year_month
        last_month <- current_month - months(1)
        last_year <- current_month - years(1)
        
        current_month_str <- format(current_month, "%B %Y")
        last_month_str <- format(last_month, "%B %Y")
        last_year_str <- format(last_year, "%B %Y")
        
        this_month_hrly <- hourly_udc %>% 
            filter(
                Month == current_month)
        last_month_hrly <- hourly_udc %>% 
            filter(
                Month == last_month) %>%
            mutate(month_hour = month_hour + months(1))
        last_year_hrly <- hourly_udc %>%
            filter(
                Month == last_year) %>%
            mutate(month_hour = month_hour + years(1))
        
        DARK_BLUE <- "#0068B2"
        LIGHT_LIGHT_BLUE <- "#BED6E2"
        DARK_GRAY = "#636363"
        DARK_GRAY_BAR = "#252525"
        
        title_ <- hourly_udc$Corridor[1]
        ymax_ <- hourly_udc$max_delay_cost[1]
        
        plot_ly() %>% 
            add_lines(
                data = last_year_hrly, # same month, a year ago
                x = ~month_hour, 
                y = ~delay_cost, 
                name = last_year_str,   # "Last Year",  # 
                line = list(color = LIGHT_BLUE), 
                fill = "tozeroy", 
                fillcolor = LIGHT_LIGHT_BLUE,
                customdata = ~glue(paste(
                    "<b>{Corridor}</b>",
                    "<br><b>{format(month_hour, '%I:%M %p')}</b>",
                    "<br>User Delay Cost: <b>{as_currency(delay_cost)}</b>")),
                hovertemplate = "%{customdata}",
                hoverlabel = list(font = list(family = "Source Sans Pro")),
                showlegend = (i==1)) %>% 
            add_lines(
                data = last_month_hrly, # last month, this year
                x = ~month_hour, 
                y = ~delay_cost, 
                name = last_month_str, 
                line = list(color = BLUE),
                customdata = ~glue(paste(
                    "<b>{Corridor}</b>",
                    "<br><b>{format(month_hour, '%I:%M %p')}</b>",
                    "<br>User Delay Cost: <b>{as_currency(delay_cost)}</b>")),
                hovertemplate = "%{customdata}",
                hoverlabel = list(font = list(family = "Source Sans Pro")),
                showlegend = (i==1)) %>% 
            add_lines(
                data = this_month_hrly, # this month, this year
                x = ~month_hour, 
                y = ~delay_cost, 
                name = current_month_str, 
                line = list(color = ORANGE),
                customdata = ~glue(paste(
                    "<b>{Corridor}</b>",
                    "<br><b>{format(month_hour, '%I:%M %p')}</b>",
                    "<br>User Delay Cost: <b>{as_currency(delay_cost)}</b>")),
                hovertemplate = "%{customdata}",
                hoverlabel = list(font = list(family = "Source Sans Pro")),
                showlegend = (i==1)) %>% 
            layout(xaxis = list(title = "",
                                tickformat = "%I:%M %p"),
                   yaxis = list(title = "",
                                tickformat = "$,.2",
                                range = c(0, ymax_)),
                   annotations = list(text = glue("<b>{title_}</b>"),
                                      font = list(size = 14),
                                      xref = "paper",
                                      yref = "paper",
                                      yanchor = "bottom",
                                      xanchor = "center",
                                      align = "center",
                                      x = 0.2,
                                      y = 0.5,
                                      showarrow = FALSE))
    }
    
    df <- hourly_udc %>% mutate(max_delay_cost = round(max(delay_cost), -3) + 1000)
    dfs <- split(df, df$Corridor)
    dfs <- dfs[lapply(dfs, nrow)>0]
    names(dfs) <- NULL
    
    plts <- purrr::imap(dfs, ~corridor_udc_plot(.x, .y))
    
    
    subplot(plts,
            margin = 0.03, nrows = ceiling(length(plts)/2), shareX = FALSE, shareY = FALSE) %>%
        layout(title = "User Delay Costs ($)",
               margin = list(t = 60))
}


perf_plotly <- function(df, per_, var_, range_max = 1.1, number_format = ",.0%", title = "title") {
    
    per_ <- as.name(per_)
    var_ <- as.name(var_)
    df <- rename(df, per = !!per_, var = !!var_)
    
    plot_ly(data = df) %>% 
        add_ribbons(x = ~per, 
                    ymin = 0,
                    ymax = ~var, 
                    color = I(DARK_GRAY),
                    line = list(shape = "vh")) %>%
        layout(yaxis = list(range = c(0, range_max),
                            tickformat = number_format),
               annotations = list(text = title,
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1.1,
                                  showarrow = FALSE))
}

perf_plotly_by_phase <- function(df, per_, var_, range_max = 1.1, number_format = ".0%", title = "title") {
    
    # Works but colors and labeling are not fully complete.
    pl <- function(dfi) {
        plot_ly(data = dfi) %>% 
            add_ribbons(x = ~per, 
                        ymin = 0,
                        ymax = ~var,
                        color = ~CallPhase,
                        colors = colrs,
                        name = paste('Phase', dfi$CallPhase[1]),
                        line = list(shape = "vh")) %>%
            layout(yaxis = list(range = c(0, range_max),
                                tickformat = number_format))
    }
    
    per__ <- as.name(per_)
    var__ <- as.name(var_)
    df <- rename(df, per = !!per__, var = !!var__)
    
    dfs <- split(df, df$CallPhase)
    
    plts <- lapply(dfs[lapply(dfs, nrow)>0], pl)
    subplot(plts, nrows = length(plts), shareX = TRUE, margin = 0.03) %>%
        layout(annotations = list(text = title,
                                  xref = "paper",
                                  yref = "paper",
                                  yanchor = "bottom",
                                  xanchor = "center",
                                  align = "center",
                                  x = 0.5,
                                  y = 1,
                                  showarrow = FALSE))
}











signal_dashboard_athena <- function(sigid, start_date, conf_athena, pth = "s3") {
    
    #conn <- get_athena_connection_pool(conf_athena)
    
    #tryCatch({
    if (is.na(sigid) || sigid == "Select") {
        no_data_plot("")
    } else {
        #------------------------
        start_date <- ymd(start_date)
        end_date <- start_date + months(1) - days(1)
        
        plan(multisession)
        #------------------------
        withProgress(message = "Loading chart", value = 0, {
            
            p_rc %<-% tryCatch({sigid; start_date; end_date;
                conn <- get_athena_connection(conf_athena)
                #------------------------
                df <- tbl(conn, sql(glue(paste(
                    "select signalid, detector, callphase, timeperiod, vol",
                    "from {conf$athena$database}.counts_1hr",
                    "where signalid = '{sigid}'",
                    "and date between '{start_date}' and '{end_date}'")))) %>%
                    collect()
                df <- df %>%
                    transmute(vol = ifelse(is.na(vol), 0, vol),
                           SignalID = factor(signalid),
                           Detector = factor(detector, levels = sort(as.integer(unique(df$detector)))),
                           CallPhase = factor(callphase, levels = sort(as.integer(unique(df$callphase)))),
                           Timeperiod = as_datetime(timeperiod))
                dbDisconnect(conn)
                
                volplot_plotly(df, title = "Raw 1 hr Aggregated Counts") %>% 
                    layout(showlegend = FALSE)
            },
            error = function(cond) {
                print(cond)
                no_data_plot_("")
            })
            
            incProgress(amount = 0.01)
            
            p_fc %<-% tryCatch({sigid; start_date; end_date;
                conn <- get_athena_connection(conf_athena)
                #------------------------
                df <- tbl(conn, sql(glue(paste(
                    "select signalid, detector, callphase, timeperiod, vol", 
                    "from {conf$athena$database}.filtered_counts_1hr", 
                    "where signalid = '{sigid}'",
                    "and date between '{start_date}' and '{end_date}'")))) %>%
                    collect()
                df <- df %>%
                    transmute(vol = ifelse(is.na(vol), 0, vol),
                           SignalID = factor(signalid),
                           Detector = factor(detector, levels = sort(as.integer(unique(df$detector)))),
                           CallPhase = factor(callphase, levels = sort(as.integer(unique(df$callphase)))),
                           Timeperiod = as_datetime(timeperiod))
                dbDisconnect(conn)
                
                volplot_plotly(df, title = "Filtered 1 hr Aggregated Counts") %>% 
                    layout(showlegend = FALSE)
            }, error = function(cond) {
                no_data_plot_("")
            })
            
            incProgress(amount = 0.01)
            
            p_vpd %<-% tryCatch({sigid; start_date; end_date;
                conn <- get_athena_connection(conf_athena)
                #------------------------
                df <- tbl(conn, sql(glue(paste(
                    "select signalid, callphase, date, vpd", 
                    "from {conf$athena$database}.vehicles_pd", 
                    "where signalid = '{sigid}'", 
                    "and date between '{start_date}' and '{end_date}'")))) %>%
                    collect()
                df <- df %>%
                    filter(!is.na(vpd)) %>%
                    transmute(SignalID = factor(signalid),
                           CallPhase = factor(callphase, levels = sort(as.integer(unique(df$callphase)))),
                           Date = as_date(date),
                           vpd)
                df_ <- df %>% 
                    group_by(SignalID, CallPhase) %>% 
                    filter(Date == max(Date)) %>% 
                    ungroup() %>% 
                    mutate(Date = Date + days(1))
                df <- df %>% bind_rows(df, df_)
                dbDisconnect(conn)
                
                perf_plotly_by_phase(df, "Date", "vpd", 
                                     range_max = max(df$vpd), 
                                     number_format = ",.0",
                                     title = "Daily Volume by Phase") %>% 
                    layout(showlegend = TRUE)
            }, error = function(cond) {
                no_data_plot_("")
            })
            
            incProgress(amount = 0.01)
            
            p_ddu %<-% tryCatch({sigid; start_date; end_date;
                conn <- get_athena_connection(conf_athena)
                #------------------------
                df <- tbl(conn, sql(glue(paste(
                    "select signalid, detector, callphase, timeperiod, good_day", 
                    "from {conf$athena$database}.filtered_counts_1hr", 
                    "where signalid = '{sigid}'", 
                    "and date between '{start_date}' and '{end_date}'")))) %>%
                    collect()
                df <- df %>%
                    transmute(vol = good_day,
                           SignalID = factor(signalid),
                           Detector = factor(detector, levels = sort(as.integer(unique(df$detector)))),
                           CallPhase = factor(callphase, levels = sort(as.integer(unique(df$callphase)))),
                           Timeperiod = as_datetime(timeperiod))
                dbDisconnect(conn)
                
                volplot_plotly(df, title = "Daily Detector Uptime", ymax = 1.1) %>% 
                    layout(showlegend = FALSE)
            }, error = function(cond) {
                no_data_plot_("")
            })
            
            incProgress(amount = 0.01)
            
            p_com %<-% tryCatch({sigid; start_date; end_date;
                conn <- get_athena_connection(conf_athena)
                #------------------------
                df <- tbl(conn, sql(glue(paste(
                    "select signalid, callphase, date, date_hour, uptime", 
                    "from {conf$athena$database}.comm_uptime", 
                    "where signalid = '{sigid}'", 
                    "and date between '{start_date}' and '{end_date}'")))) %>%
                    collect()
                df <- df %>%
                    transmute(SignalID = factor(signalid),
                           CallPhase = factor(callphase, levels = sort(as.integer(unique(df$callphase)))),
                           Date_Hour = as_datetime(date_hour),
                           Date = as_date(date),
                           uptime)
                dbDisconnect(conn)
                
                perf_plotly(df, 
                            "Date", "uptime", 
                            title = "Daily Communications Uptime") %>% 
                    layout(showlegend = FALSE)
            }, error = function(cond) {
                no_data_plot_("")
            })
            
            incProgress(amount = 0.015)
            
            p_aog %<-% tryCatch({sigid; start_date; end_date;
                conn <- get_athena_connection(conf_athena)
                #------------------------
                df <- tbl(conn, sql(glue(paste(
                    "select signalid, callphase, date, date_hour, aog", 
                    "from {conf$athena$database}.arrivals_on_green", 
                    "where signalid = '{sigid}'", 
                    "and date between '{start_date}' and '{end_date}'")))) %>%
                    collect()
                df <- df %>%
                    transmute(SignalID = factor(signalid),
                           CallPhase = factor(callphase, levels = sort(as.integer(unique(df$callphase)))),
                           Date_Hour = as_datetime(date_hour),
                           Date = as_date(date),
                           aog) %>% 
                    complete(nesting(SignalID, CallPhase), 
                             Date_Hour = seq(min(Date_Hour), max(Date_Hour), by = "1 hour"),
                             fill = list("aog" = 0)) %>% 
                    arrange(SignalID, CallPhase, Date_Hour)
                dbDisconnect(conn)
                perf_plotly_by_phase(df,
                                     "Date_Hour", "aog", 
                                     title = "Arrivals on Green") %>% 
                    layout(showlegend = FALSE)
            }, error = function(cond) {
                print(paste("ERROR:", cond))
            })
            
            incProgress(amount = 0.015)
            
            p_qs %<-% tryCatch({sigid; start_date; end_date;
                conn <- get_athena_connection(conf_athena)
                #------------------------
                df <- tbl(conn, sql(glue(paste(
                    "select signalid, callphase, date, date_hour, qs_freq", 
                    "from {conf$athena$database}.queue_spillback", 
                    "where signalid = '{sigid}'", 
                    "and date between '{start_date}' and '{end_date}'")))) %>%
                    collect()
                df <- df %>%
                    transmute(
                        SignalID = factor(signalid),
                        CallPhase = factor(callphase, levels = sort(as.integer(unique(df$callphase)))),
                        Date_Hour = as_datetime(date_hour),
                        Date = as_date(date),
                        qs_freq) %>% 
                    complete(nesting(SignalID, CallPhase), 
                             Date_Hour = seq(min(Date_Hour), max(Date_Hour), by = "1 hour"),
                             fill = list("qs_freq" = 0)) %>% 
                    arrange(SignalID, CallPhase, Date_Hour)
                dbDisconnect(conn)
                perf_plotly_by_phase(df,
                                     "Date_Hour", "qs_freq", 
                                     #range_max = 0.8, 
                                     title = "Queue Spillback Rate") %>% 
                    layout(showlegend = FALSE)
            }, error = function(cond) {
                no_data_plot_("")
            })
            
            incProgress(amount = 0.015)
            
            p_sf %<-% tryCatch({sigid; start_date; end_date;
                conn <- get_athena_connection(conf_athena)
                #------------------------
                df <- tbl(conn, sql(glue(paste(
                    "select signalid, callphase, date, date_hour, sf_freq", 
                    "from {conf$athena$database}.split_failures", 
                    "where signalid = '{sigid}'", 
                    "and date between '{start_date}' and '{end_date}'")))) %>%
                    collect()
                df <- df %>%
                    transmute(
                        SignalID = factor(signalid),
                        CallPhase = factor(callphase, levels = sort(as.integer(unique(df$callphase)))),
                        Date_Hour = as_datetime(date_hour),
                        Date = as_date(date),
                        sf_freq) %>%
                    complete(nesting(SignalID, CallPhase),
                             Date_Hour = seq(min(Date_Hour), max(Date_Hour), by = "1 hour"),
                             fill = list("sf_freq" = 0)) %>%
                    arrange(SignalID, CallPhase, Date_Hour)
                dbDisconnect(conn)
                
                perf_plotly_by_phase(df,
                                     "Date_Hour", "sf_freq",
                                     range_max = 1.0,
                                     title = "Split Failure Rate") %>%
                    layout(showlegend = FALSE)
            }, error = function(cond) {
                no_data_plot_("")
            })
            
            incProgress(amount = 0.015)
            
            sr1a %<-% subplot(list(p_rc, p_vpd), 
                              nrows = 2, 
                              margin = 0.04, 
                              heights = c(0.6, 0.4), 
                              shareX = TRUE)
            
            incProgress(amount = 0.3)
            
            sr1b %<-% subplot(list(p_fc, p_ddu), 
                              nrows = 2, 
                              margin = 0.04, 
                              heights = c(0.6, 0.4), 
                              shareX = TRUE)
            
            incProgress(amount = 0.3)
            
            sr2 %<-% subplot(list(p_com, p_aog, p_qs, p_sf), 
                             nrows = 4, 
                             margin = 0.04, 
                             heights = c(0.1, 0.2, 0.2, 0.5), 
                             shareX = TRUE)
            
            incProgress(amount = 0.3)
            
            subplot(sr1a, sr1b, sr2)
        })
    }    
    
    
    #}, error = function(e) {
    #    no_data_plot("")
    #})
}


detector_dashboard_athena <- function(sigid, start_date, conf_athena, pth = "s3") {
    
    if (is.na(sigid) || sigid == "Select") {
        no_data_plot("")
    } else {
        #------------------------
        start_date <- ymd(start_date)
        end_date <- start_date + months(1) - days(1)
        
        plan(multisession)
        #------------------------
        withProgress(message = "Loading chart", value = 0, {
        
            p_rc %<-% tryCatch({sigid; start_date; end_date;
                conn <- get_athena_connection(conf_athena)
                #------------------------
                df <- tbl(conn, sql(glue(paste(
                    "select signalid, detector, callphase, timeperiod, vol", 
                    "from {conf$athena$database}.counts_1hr", 
                    "where signalid = '{sigid}' and date between '{start_date}' and '{end_date}'")))) %>%
                    collect()
                df <- df %>%
                    transmute(
                        vol = ifelse(is.na(vol), 0, vol),
                        SignalID = factor(signalid),
                        Detector = factor(detector, levels = sort(as.integer(unique(df$detector)))),
                        CallPhase = factor(callphase, levels = sort(as.integer(unique(df$callphase)))),
                        Timeperiod = as_datetime(timeperiod))
                dbDisconnect(conn)
                
                if (nrow(df) > 0) {
                    volplot_plotly(df, ymax = NULL, title = "Raw 1 hr Aggregated Counts") %>% 
                        layout(showlegend = FALSE)
                } else {
                    no_data_plot("")
                }
            },
            error = function(cond) {
                print(cond)
                no_data_plot("")
            })
            
            incProgress(amount = 0.3)
            
            p_fc %<-% tryCatch({sigid; start_date; end_date;
                conn <- get_athena_connection(conf_athena)
                #------------------------
                df <- tbl(conn, sql(glue(paste(
                    "select signalid, detector, callphase, timeperiod, vol", 
                    "from {conf$athena$database}.filtered_counts_1hr",
                    "where signalid = '{sigid}' and date between '{start_date}' and '{end_date}'")))) %>%
                    collect()
                df <- df %>%
                    transmute(
                        vol = ifelse(is.na(vol), 0, vol),
                        SignalID = factor(signalid),
                        Detector = factor(detector, levels = sort(as.integer(unique(df$detector)))),
                        CallPhase = factor(callphase, levels = sort(as.integer(unique(df$callphase)))),
                        Timeperiod = as_datetime(timeperiod))
                dbDisconnect(conn)
                
                volplot_plotly(df, ymax = NULL, title = "Filtered 1 hr Aggregated Counts") %>% 
                    layout(showlegend = FALSE)
            }, error = function(cond) {
                no_data_plot_("")
            })
            
            incProgress(amount = 0.3)
            
            p_ddu %<-% tryCatch({sigid; start_date; end_date;
                conn <- get_athena_connection(conf_athena)
                #------------------------
                df <- tbl(conn, sql(glue(paste(
                    "select signalid, detector, callphase, timeperiod, good_day", 
                    "from {conf$athena$database}.filtered_counts_1hr", 
                    "where signalid = '{sigid}' and date between '{start_date}' and '{end_date}'")))) %>%
                    collect()
                df <- df %>%
                    transmute(
                        vol = good_day,
                        SignalID = factor(signalid),
                        Detector = factor(detector, levels = sort(as.integer(unique(df$detector)))),
                        CallPhase = factor(callphase, levels = sort(as.integer(unique(df$callphase)))),
                        Timeperiod = as_datetime(timeperiod))
                dbDisconnect(conn)
                
                volplot_plotly(df, title = "Daily Detector Uptime", ymax = 1.1) %>% 
                    layout(showlegend = FALSE)
            }, error = function(cond) {
                no_data_plot_("")
            })
            
            incProgress(amount = 0.3)
            
            subplot(p_rc, p_fc, p_ddu)
        })
    }
}



filter_alerts_by_date <- function(alerts, dr) {
    
    start_date <- dr[1]
    end_date <- dr[2]
    
    alerts %>%
        filter(Date >= start_date & Date <= end_date)
}


filter_alerts <- function(alerts, alert_type_, zone_group_, corridor_, phase_, id_filter_)  {
    
    most_recent_date <- max(alerts$Date)
    #df <- alerts
    df <- filter(alerts, Alert == alert_type_)
    
    if (nrow(df)) {
        
        # Filter by Zone Group if "All Corridors" selected
        if (corridor_ == "All Corridors") {
            if (zone_group_ == "All RTOP") {
                df <- filter(df, Zone_Group %in% c("RTOP1", "RTOP2"))
                
            } else if (zone_group_ == "Zone 7") {
                df <- filter(df, Zone %in% c("Zone 7m", "Zone 7d"))
                
            } else if (grepl("^Zone", zone_group_)) {
                df <- filter(df, Zone == zone_group_)
                
            } else {
                df <- filter(df, Zone_Group == zone_group_)
            }
            # Otherwise filter by Corridor
        } else {
            df <- filter(df, Corridor == corridor_)
        }
        
        # Filter filter bar text (regular expression) within Name, Corridor, SignalID
        df <- filter(df, grepl(pattern = id_filter_, x = Name, ignore.case = TRUE, perl = TRUE) |
                         grepl(pattern = id_filter_, x = Corridor, ignore.case = TRUE, perl = TRUE) |
                         grepl(pattern = id_filter_, x = SignalID, ignore.case = TRUE, perl = TRUE))
    }
    
    # Filter by phase, but not for Missing Records, which doesn't have a phase
    if (nrow(df)) {
        if (alert_type_ != "Missing Records" & phase_ != "All") {
            df <- filter(df, CallPhase == as.numeric(phase_)) # filter
        }
        
    }
    
    # If there is anything left in the alerts table, create the table and plots data frames
    if (nrow(df)) {
        
        table_df <- df %>%
            group_by(Zone, Corridor, SignalID, CallPhase, Detector, Name, Alert) %>%
            mutate(Streak = streak[Date == max(Date)]) %>%
            summarize(
                Occurrences = n(), 
                Streak = max(Streak)) %>% 
            ungroup() %>%
            arrange(desc(Streak), desc(Occurrences))
        
        if (alert_type_ == "Missing Records") {
            
            plot_df <- df %>%
                mutate(SignalID2 = SignalID) %>%
                unite(Name2, SignalID2, Name, sep = ": ") %>%
                mutate(signal_phase = factor(Name2))
            
            table_df <- table_df %>% select(-c(CallPhase, Detector))
            
        } else if (alert_type_ == "Bad Vehicle Detection" || alert_type_ == "Bad Ped Detection") {
            
            plot_df <- df %>%
                mutate(Detector = as.character(Detector)) %>%
                unite(signal_phase2, Name, Detector, sep = " | det ") %>%
                
                mutate(SignalID2 = SignalID) %>%
                unite(signal_phase, SignalID2, signal_phase2, sep = ": ") %>% 
                mutate(signal_phase = factor(signal_phase))
            
        } else if (alert_type_ == "No Camera Image") {
            
            plot_df <- df %>%
                mutate(SignalID2 = SignalID) %>%
                unite(signal_phase, SignalID2, Name, sep = ": ") %>% 
                mutate(signal_phase = factor(signal_phase))
            
            table_df <- table_df %>% select(-c(CallPhase, Detector))
            
        # "Bad Ped Detection", "Pedestrian Activations", "Force Offs", "Max Outs", "Count"
        } else { 
            
            plot_df <- df %>%
                unite(signal_phase2, Name, CallPhase, sep = " | ph ") %>% # potential problem
                
                mutate(SignalID2 = SignalID) %>%
                unite(signal_phase, SignalID2, signal_phase2, sep = ": ") %>% 
                mutate(signal_phase = factor(signal_phase))
            
            if (alert_type_ != "Count") {
                table_df <- table_df %>% dplyr::select(-Detector)
            }
        }
        
        intersections <- length(unique(plot_df$signal_phase))
        
    } else { #df_is_empty
        
        plot_df <- data.frame()
        table_df <- data.frame()
        intersections <- 0
    }
    
    list("plot" = plot_df, 
         "table" = table_df, 
         "intersections" = intersections)
}



filter_alerts_prestreak <- function(alerts, alert_type_, zone_group_, phase_, id_filter_)  {
    
    df_is_empty <- FALSE
    
    if (nrow(alerts) == 0) {
        df_is_empty <- TRUE
    } else {
        df <- filter(alerts, Alert == alert_type_,
                     grepl(pattern = id_filter_, x = Name, ignore.case = TRUE))
        
        if (nrow(df) == 0) {
            df_is_empty <- TRUE
        } else {
            
            if (zone_group_ == "All RTOP") {
                df <- filter(df, Zone_Group %in% c("RTOP1", "RTOP2"))
                
            } else if (zone_group_ == "Zone 7") {
                df <- filter(df, Zone %in% c("Zone 7m", "Zone 7d"))
                
            } else if (grepl("^Zone", zone_group_)) {
                df <- filter(df, Zone == zone_group_)
                
            } else {
                df <- filter(df, Zone_Group == zone_group_)
            }
            
            if (nrow(df) == 0) {
                df_is_empty <- TRUE
            } else {
                
                if (alert_type_ != "Missing Records" & phase_ != "All") {
                    df <- filter(df, Phase == as.numeric(phase_)) # filter
                }
                
                if (nrow(df) == 0) {
                    df_is_empty <- TRUE
                }
            }
        }
    }
    
    if (!df_is_empty) {
        
        if (alert_type_ == "Missing Records") {
            
            table_df <- df %>%
                group_by(Zone, SignalID, Name, Alert) %>% 
                summarize("Occurrences" = n()) %>% 
                ungroup() 
            
            plot_df <- df %>%
                mutate(SignalID2 = SignalID) %>%
                unite(Name2, SignalID2, Name, sep = ": ") %>%
                mutate(signal_phase = factor(Name2)) 
            
        } else if (alert_type_ == "Bad Vehicle Detection") {
            
            table_df <- df %>%
                group_by(
                    Zone, 
                    SignalID, 
                    Name, 
                    Detector = as.integer(as.character(Detector)), 
                    Alert) %>% 
                summarize("Occurrences" = n()) %>% 
                ungroup() 
            
            plot_df <- df %>%
                mutate(Detector = as.character(Detector)) %>%
                unite(signal_phase2, Name, Detector, sep = " | det ") %>%
                
                mutate(SignalID2 = SignalID) %>%
                unite(signal_phase, SignalID2, signal_phase2, sep = ": ") %>% 
                mutate(signal_phase = factor(signal_phase))
            
        } else if (alert_type_ == "No Camera Image") {
            
            table_df <- df %>%
                group_by(Zone, SignalID, Name, Alert) %>% 
                summarize("Occurrences" = n()) %>% 
                ungroup() 
            
            plot_df <- df %>%
                mutate(SignalID2 = SignalID) %>%
                unite(signal_phase, SignalID2, Name, sep = ": ") %>% 
                mutate(signal_phase = factor(signal_phase))
            
        } else {
            
            table_df <- df %>%
                group_by(Zone, SignalID, Name, Phase, Alert) %>% 
                summarize("Occurrences" = n()) %>% 
                ungroup() 
            plot_df <- df %>%
                # mutate(Phase = as.character(Phase)) %>% 
                unite(signal_phase2, Name, Phase, sep = " | ph ") %>% # potential problem
                
                mutate(SignalID2 = SignalID) %>%
                unite(signal_phase, SignalID2, signal_phase2, sep = ": ") %>% 
                mutate(signal_phase = factor(signal_phase))
            
        }
        
        intersections <- length(unique(plot_df$signal_phase))
        
    } else { #df_is_empty
        
        plot_df <- data.frame()
        table_df <- data.frame()
        intersections <- 0
    }
    
    list("plot" = plot_df, 
         "table" = table_df, 
         "intersections" = intersections)
}



plot_alerts <- function(df, date_range) {
    
    if (nrow(df) > 0) {
        
        start_date <- max(min(df$Date), date_range[1])
        end_date <- max(max(df$Date), date_range[2])
        
        p <- ggplot() + 
            
            # tile plot
            geom_tile(data = df, 
                      aes(x = Date, 
                          y = signal_phase,
                          fill = streak), 
                      color = "white") + 
            
            scale_fill_gradient(low = "#fc8d59", high = "#7f0000", limits = c(0,90)) +
            
            # fonts, text size and labels and such
            theme(panel.grid.major = element_blank(),
                  panel.grid.minor = element_blank(),
                  axis.ticks.x = element_line(color = "gray50"),
                  axis.text.x = element_text(size = 11),
                  axis.text.y = element_text(size = 11),
                  axis.ticks.y = element_blank(),
                  axis.title = element_text(size = 11),
                  legend.position = "right") +
            
            scale_x_date(position = "top") + #, limits = date_range) #+
            scale_y_discrete(limits = rev(levels(df$signal_phase))) +
            labs(x = "",
                 y = "") + #Intersection (and phase, if applicable)") +
            
            
            # draw white gridlines between tick labels
            geom_vline(xintercept = as.numeric(seq(start_date, end_date, by = "1 day")) - 0.5,
                       color = "white")
        
        if (length(unique(df$signal_phase)) > 1) {
            p <- p +
                geom_hline(yintercept = seq(1.5, length(unique(df$signal_phase)) - 0.5, by = 1), 
                           color = "white")
        }
        p
    }
}



plot_empty <- function(zone) {
    
    ggplot() + 
        annotate("text", x = 1, y = 1, 
                 label = "No Data") + 
        theme(panel.grid.major = element_blank(), 
              panel.grid.minor = element_blank(), 
              axis.ticks = element_blank(), 
              axis.text = element_blank(), 
              axis.title = element_blank())
}    

reconstitute <- function(df, col_name) { 
    col_name <- as.name(col_name)
    lapply(names(df), function(n) { mutate(df[[n]], !!col_name := n) }) %>% 
        bind_rows() %>% 
        mutate(!!col_name := factor(!!col_name))
}


m1dynq_ <- function(res, per, tab, start_date, end_date = NULL, corridor = NULL, zone_group = NULL) {
    cid <- glue("{res}-{per}-{tab}")
    
    if (class(start_date) == "Date") {
        start_date <- format(start_date, "%Y-%m-%d")
    }
    
    if (is.null(end_date)) {
        df <- query_dynamodb_beta(cid, start_date, corridor = corridor, zone_group = zone_group)
    } else {
        if (class(end_date) == "Date") {
            end_date <- format(end_date, "%Y-%m-%d")
        }
        df <- query_dynamodb_beta(cid, start_date, end_date, corridor = corridor, zone_group = zone_group)
    }

    if ("Corridor" %in% names(df)) {
        df <- mutate(df, Corridor = factor(Corridor))
    }
    if ("Zone_Group" %in% names(df)) {
        df <- mutate(df, Zone_Group = factor(Zone_Group))
    }
    if ("Month" %in% names(df)) {
        df <- mutate(df, Month = ymd(Month))
    }
    if ("Date" %in% names(df)) {
        df <- mutate(df, Date = ymd(Date))
    }
    if ("Hour" %in% names(df)) {
        df <- mutate(df, Hour = ymd_hms(sub("(\\d{4}-\\d{2}-\\d{2})$", "\\1 00:00:00", Hour)))
    }
    
    df
}
m1dynq <- memoise(m1dynq_)

rds_vb_query_ <- function(mr, per, tab, zone_group, current_month = NULL, current_quarter = NULL) {
    
    table <- glue("{mr}_{per}_{tab}")
    df <- tbl(conn, table)
    if (!is.null(current_month)) {
        df %>% 
            filter(Month == current_month,
                   Corridor == zone_group) %>%
            collect() %>%
            mutate(Month = date(Month))
    } else { # current_quarter is not null
        df %>% 
            filter(Quarter == current_quarter,
                   Corridor == zone_group) %>%
            collect()
    }
}
rds_vb_query <- memoise(rds_vb_query_)

rds_pp_query_ <- function(mr, per, tab, first_month, current_month, zone_group = NULL) {
    
    table <- glue("{mr}_{per}_{tab}")
    df <- tbl(conn, table)
    
    if ("Month" %in% names(df)) {
        df <- filter(df, Month >= first_month, Month <= current_month)
    }
    if ("Date" %in% names(df)) {
        df <- filter(Date >= first_month, Date <= current_month)
    }
    if ("Hour" %in% names(df)) {
        df <- filter(Hour >= first_month, Hour <= current_month)
    }
    
    if (!is.null(zone_group)) {
        df <- filter(df, Corridor == zone_group)
    }
    
    df <- collect(df)
    
    if ("Month" %in% names(df)) {
        df <- mutate(df, Month = date(Month))
    }
    if ("Date" %in% names(df)) {
        df <- mutate(df, Date = date(Date))
    }
    if ("Hour" %in% names(df)) {
        df <- mutate(df, Hour = ymd_hms(Hour))
    }
    
    if ("Corridor" %in% names(df)) {
        df <- mutate(df, Corridor = factor(Corridor))
    }
    if ("Zone_Group" %in% names(df)) {
        df <- mutate(df, Zone_Group = factor(Zone_Group))
    }
    df
}
rds_pp_query <- memoise(rds_pp_query_)






get_counts_plot <- function(sigid, start_date, end_date, conf_athena, pth = "s3", table_name) {
    #------------------------
    start_date <- ymd(start_date)
    end_date <- ymd(end_date)
    
    conn <- get_athena_connection(conf_athena)
    #------------------------
    df <- tbl(conn, sql(glue("select * from {conf_athena$database}.{table_name}"))) %>%
        filter(signalid == sigid,
               between(date, start_date, end_date)) %>%
        select(signalid, detector, callphase, timeperiod, vol) %>%
        collect()
    dbDisconnect(conn)
    #------------------------
    
    df <- df %>%
        mutate(vol = ifelse(is.na(vol), 0, vol),
               SignalID = factor(signalid),
               Detector = factor(detector, levels = sort(as.integer(unique(df$detector)))),
               CallPhase = factor(callphase, levels = sort(as.integer(unique(df$callphase)))),
               Timeperiod = as_datetime(timeperiod))
    volplot_plotly(df, title = "Counts") %>% 
        layout(showlegend = TRUE)
}

get_raw_counts_plot <- function(sigid, start_date, end_date, conf_athena, pth = "s3") {
    get_counts_plot(sigid, start_date, end_date, conf_athena, pth = "s3", table_name = "counts_1hr")
}
get_filtered_counts_plot <- function(sigid, start_date, end_date, conf_athena, pth = "s3") {
    get_counts_plot(sigid, start_date, end_date, conf_athena, pth = "s3", table_name = "filtered_counts_1hr")
}
get_adjusted_counts_plot <- function(sigid, start_date, end_date, conf_athena, pth = "s3") {
    get_counts_plot(sigid, start_date, end_date, conf_athena, pth = "s3", table_name = "adjusted_counts_1hr")
}



# Functions for Corridor Summary Table

metric_formats <- function(x) { # workaround function to format the metrics differently
    sapply(x, function(x) {
        if (!is.na(x)) {
            if (x <= 1) {
                as_pct(x)
            } else if ((x <= 10) & (x - round(x, 0) != 0)) { # TTI and PTI, hack for oustanding tasks
                as_2dec(x)
            } else { # throughput and outstanding tasks
                as_int(x)
            }
        } else {
            return("N/A")
        }
    })
}


getcolorder <- function(no.corridors) {
    getcolorder <- c(1, 2)
    for (i in 1:no.corridors) {
        getcolorder <- c(getcolorder, i + 2, i + no.corridors + 2)
    }
    for (i in 1:no.corridors) {
        getcolorder <- c(getcolorder, i + no.corridors * 2 + 2, i + no.corridors * 3 + 2) #updated to include checks for deltas
    }
    getcolorder
}




get_corridor_summary_table <- function(data, current_month, zone_group) {
    
    #' Creates an html table for a corridor by zone in a month
    #' with all metrics color coded by whether they met goal
    #' and whether they're trending up or down.
    #' 
    #' @param data corridor_summary_table
    #' @param zone_group a Zone Group
    #' @return An html table for markdown or shiny
    
    if (zone_group %in% unique(data$Zone_Group)) {
        # table for plotting
        dt <- filter(data, Month == current_month, Zone_Group == zone_group) %>%
            select(-c(seq(5, 25, 2)), -Zone_Group, -Month) %>%
            gather("Metric", "value", 2:12) %>%
            spread(Corridor, value)
        dt <- dt[order(match(dt$Metric, metric.order)), ]
        dt$Metrics <- metric.names
        dt$Goal <- metric.goals
        dt <- select(dt, c((ncol(dt) - 1):ncol(dt), 2:(ncol(dt) - 2))) # vary based on # of columns
        setDT(dt)
        
        # table with deltas - to be used for formatting data table
        dt.deltas <- filter(data, Month == current_month, Zone_Group == zone_group) %>%
            select(-c(seq(4, 24, 2)), -Zone_Group, -Month) %>%
            rename_at(vars(ends_with(".delta")), funs(str_replace(., ".delta", ""))) %>%
            gather("Metric", "value", 2:12) %>%
            spread(Corridor, value)
        dt.deltas <- dt.deltas[order(match(dt.deltas$Metric, metric.order)), ]
        dt.deltas$Metrics <- metric.names
        dt.deltas$Goal <- metric.goals
        # vary based on # of columns
        dt.deltas <- select(
            dt.deltas, 
            c((ncol(dt.deltas) - 1):ncol(dt.deltas), 2:(ncol(dt.deltas) - 2)))
        setDT(dt.deltas)
        
        # table with checks on whether or not we're meeting 
        # the various metrics for the month - also to be used formatting data table
        dt.checks <- copy(dt)
        setDT(dt.checks)
        dt.checks[Metrics == "Traffic Volume (Throughput)", (3:ncol(dt.checks)) := dt.deltas[Metrics == "Traffic Volume (Throughput)", 3:ncol(dt.deltas)]] # replace out throughput check with delta
        dt.checks[Metrics == "Outstanding Tasks", (3:ncol(dt.checks)) := dt.deltas[Metrics == "Outstanding Tasks", 3:ncol(dt.deltas)]] # replace out tasks check with delta
        dt.checks[, Goal.Numeric := metric.goals.numeric]
        dt.checks[, Goal.Sign := metric.goals.sign]
        dt.checks[, (3:(ncol(dt.checks) - 2)) := lapply(.SD, function(x) {
            ifelse((x >= Goal.Numeric & Goal.Sign == ">") | (x <= Goal.Numeric & Goal.Sign == "<"), "Meeting Goal", "Not Meeting Goal")
        }), .SDcols = 3:(ncol(dt.checks) - 2)]
        dt.checks[, c((ncol(dt.checks) - 1):ncol(dt.checks)) := NULL]
        
        
        #table with checks on whether or not delta shows improvement or deterioration - also to be used formatting data table
        dt.deltas.checks <- copy(dt.deltas)
        setDT(dt.deltas.checks)
        dt.deltas.checks[,Goal.Sign := metric.goals.sign]
        dt.deltas.checks[,(3:(ncol(dt.deltas.checks)-1)) := lapply(.SD,function(x) {
            ifelse((x >= 0 & Goal.Sign == ">") | (x <= 0 & Goal.Sign == "<"), "Improvement", "Deterioration")
        }),.SDcols = 3:(ncol(dt.deltas.checks) - 1)]
        dt.deltas.checks[, ncol(dt.deltas.checks) := NULL]
        
        # overall data table
        dt.overall <- dt[dt.deltas, on = .(Metrics = Metrics, Goal = Goal)] %>%
            rename_at(vars(starts_with("i.")), funs(str_replace(., "i.", "Delta.")))
        dt.overall <- dt.overall[dt.checks, on = .(Metrics = Metrics, Goal = Goal)] %>%
            rename_at(vars(starts_with("i.")), funs(str_replace(., "i.", "Check.Goal.")))
        dt.overall <- dt.overall[dt.deltas.checks,on = .(Metrics=Metrics,Goal=Goal)] %>%
            rename_at(vars(starts_with("i.")), funs(str_replace(., "i.", "Check.Delta.")))
        
        no.corridors <- (ncol(dt.overall) - 2) / 4 # how many corridors are we working with?
        
        ###########
        # DATA TABLE DISPLAY AND FORMATTING
        ###########
        
        # update formatting for individual rows
        dt.overall[, (3:(3 + no.corridors - 1)) := lapply(.SD, function(x) metric_formats(x)), .SDcols = (3:(3 + no.corridors - 1))]
        dt.overall[11, (3:(3+no.corridors - 1)) := lapply(.SD, function (x) {
            ifelse(x=="100.0%", "1", ifelse(x == "0.0%", "0", x))
        }),.SDcols = (3:(3 + no.corridors - 1))] #hack for tasks
        
        # function to get columns lined up with deltas immediately after the metrics
        setcolorder(dt.overall, getcolorder(no.corridors))
        
        
        # formatter for just metrics (deltas in separate column)
        format.metrics <- sapply(names(dt.overall)[seq(3, no.corridors * 2 + 2, 2)], function(x) {
            eval(
                parse(
                    text = sub(
                        "_SUB_",
                        paste0("`Check.Goal.", x, "`"),
                        "formatter(\"span\", style = ~ style(display = \"block\",
                \"border-radius\" = \"4px\", \"padding-right\" = \"4px\",
                \"color\" = ifelse(_SUB_=='Meeting Goal', yes.color, no.color)))"
                    )
                )
            )
        }, simplify = F, USE.NAMES = T)
        
        # formatter for deltas (separate column - arrows)
        format.deltas <- sapply(names(dt.overall)[seq(4, no.corridors * 2 + 2, 2)], function(x) {
            eval( # evaluate the following expression
                parse(
                    text = # parse the following string to an expression
                        gsub(
                            "_SUB_", # find "_SUB_"
                            x, 
                            "formatter(\"span\", style = ~ style(display = \"block\",
                        \"border-radius\" = \"4px\", \"padding-right\" = \"4px\",
                        \"color\" = ifelse(`Check._SUB_`=='Improvement', yes.color, no.color)),
                        ~ icontext(sapply(`_SUB_`, function(x) if (!is.na(x)) {if (x > 0) 'arrow-up' else if (x < 0) 'arrow-down'} else '')))")))
        }, simplify = F, USE.NAMES = T)
        
        
        # columns checking whether we're meeting checks - HIDE in formattable
        hide.checks <- sapply(names(dt.overall[, (ncol(dt.overall) - no.corridors * 2 + 1):(ncol(dt.overall))]),
                              function(x) eval(parse(text = sub("_SUB_", as.character(x), "`_SUB_` = F"))),
                              simplify = F, USE.NAMES = T
        )
        
        
        formattable::formattable(dt.overall,
                                 align = c("l", "c", rep(c("r", "l"), no.corridors)),
                                 c( # `Goal` = formatter("span", style = ~ style(width="1000px")),
                                     format.metrics,
                                     format.deltas,
                                     hide.checks
                                 )
        )
        
    } else {
        NULL
    }
}


get_zone_group_text_table <- function(month, zone_group) {

    #' Creates an html table for a corridor by zone in a month
    #' 
    #' @param month Month in date format
    #' @param zone_group a Zone Group
    #' @return An html table

    #current_month <- months[order(months)][match(month, months.formatted)]
    tt <- filter(df.text, Month == ymd(current_month), Zone == zone_group)
    colnames(tt)[3] <- "&nbsp"
    formattable::formattable(
        tt,
        align = "l",
        list(
            Month = F,
            Zone = F
        )
    )
}
