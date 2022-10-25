
source("Monthly_Report_UI_Functions.R")
source("Before-After.R")



metric <- travel_time_index
level <- "corridor"
zone_group <- "US 50 (Fairfax)"
corridor <-  "US 50 (Fairfax)"
before_start_date <- as_date("2022-05-01")
before_end_date <- as_date("2022-06-30")
after_start_date <- as_date("2022-07-01")
after_end_date <- as_date("2022-08-30")

level <- "corridor"
zone_group <- "Northern Region"
corridor <- "All Corridors"



df <- query_before_after_data(
    metric, level, zone_group, corridor, 
    before_start_date, before_end_date, 
    after_start_date, after_end_date)

before_df <- df$before
after_df <- df$after

get_before_after_line_plot(before_df, after_df, metric)



plot_ly(sd1,
        type = "scatter",
        mode = "lines",
        x = ~Date, 
        y = ~var, 
        color = ~col, colors = c(LIGHT_GRAY_BAR, BLACK),
        alpha = 0.6,
        name = "",
        customdata = ~glue(paste(
            "<b>{Description}</b>",
            "<br>Week of: <b>{format(Date, '%B %e, %Y')}</b>",
            "<br>{metric$label}: <b>{data_format(metric$data_type)(var)}</b>")),
        hovertemplate = "%{customdata}",
        hoverlabel = list(font = list(family = "Source Sans Pro"))
) %>% layout(xaxis = list(title = "Before"),
             yaxis = list(tickformat = ",.r",   #tickformat = tick_format(metric$data_type),
                          hoverformat = tick_format(metric$data_type)),
             title = "__plot1_title__",
             showlegend = FALSE,
             margin = list(t = 50)
)
