
query_before_after_data <- function(
        metric, 
        level = "corridor", 
        zone_group, 
        corridor = "All Corridors",
        before_start_date,
        before_end_date,
        after_start_date,
        after_end_date) {
    
    # metric is one of {vpd, tti, aog, ...}
    # level is one of {corridor, subcorridor, signal}
    # resolution is one of {quarterly, monthly, weekly, daily}
    
    before_start_date <- as_datetime(before_start_date)
    before_end_date <- as_datetime(before_end_date)
    after_start_date <- as_datetime(after_start_date)
    after_end_date <- as_datetime(after_end_date)
    
    period_duration = max(
        before_end_date - before_start_date, 
        after_end_date - after_start_date
    )
    
    if (period_duration < days(3)) {
        resolution <- "daily"  
        # "hourly" -- for future if we want to add hourly calcs to the mix. 
        # Probably better to focus on timing plans.
    } else if (period_duration < weeks(6)) {
        resolution <- "daily"
    } else {
        resolution <- "weekly"
    }
    
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
    
    if (resolution == "hourly" && !is.null(metric$hourly_table)) {
        tab <- metric$hourly_table
    } else {
        tab <- metric$table
    }
    
    table <- glue("{mr_}_{per}_{tab}")
    
    if (table %in% dbListTables(sigops_connection_pool)) {
        where_clause <- "WHERE Zone_Group = '{zone_group}'"
        
        datetimefield <- switch(
            resolution,
            "hourly" = "Hour",  # Future
            "daily" = "Date",
            "weekly" = "Date",
            "month" = "Month"  # Not used. Not for chart, maybe for scatter figure.
        )
        
        query <- glue(paste(
            "SELECT Zone_Group, Corridor, {datetimefield}, {metric$variable} FROM {table}", 
            where_clause))
        
        query <- paste(
            query, 
            glue("AND {datetimefield} >= '{before_start_date}'"),
            glue("AND {datetimefield} < '{after_end_date + days(1)}'"))
        
        df <- data.frame()
        
        print(query)
        
        tryCatch({
            df <- dbGetQuery(sigops_connection_pool, query)
            
            datetime_string <- intersect(c("Month", "Date", "Hour"), names(df))
            
            if (datetime_string %in% c("Month", "Date")) {
                df[[datetime_string]] = as_date(df[[datetime_string]])
            } else if (datetime_string == "Hour") {
                df[[datetime_string]] = as_datetime(df[[datetime_string]])
            }
            

            df <- left_join(df, select(corridors, SignalID, Description), by = c("Corridor" = "SignalID")) %>% 
                mutate(Description = coalesce(as.character(Description), as.character(Corridor)))
            
            before_df <- filter(df, !!as.name(datetimefield) <= before_end_date)
            after_df <- filter(df, !!as.name(datetimefield) >= after_start_date)
            
            compare1 <- before_df %>% 
                group_by(Corridor, Description) %>% 
                summarize(before = mean(!!as.name(metric$variable)), .groups = "drop")
            compare2 <- after_df %>% 
                group_by(Corridor, Description) %>% 
                summarize(after = mean(!!as.name(metric$variable)), .groups = "drop")
            
            scatter_df <- full_join(compare1, compare2, by=c("Corridor", "Description"))
            
            compare_df <- scatter_df %>% 
                arrange(desc(after)) %>%
                mutate(
                    delta = as_pct((after-before)/before),
                    before = data_format(metric$data_type)(before),
                    after = data_format(metric$data_type)(after)
                )
            # Debug steps
            # tfn <- tempfile(tmpdir = '.', fileext = ".qs")
            # qs::qsave(scatter_df, tfn)
            
        }, error = function(e) {
            print(e)
        })
        
        list(
            before = before_df,
            after = after_df,
            compare = compare_df,
            scatter = scatter_df
        )
    } else {
        list(
            before = data.frame(), 
            after = data.frame(),
            compare = data.frame(),
            scatter = data.frame()
        )
    }

}








get_before_after_line_plot <- function(before_df, after_df, metric, line_chart = "weekly", accent_average = TRUE) {
    
    # var_ <- as.name(metric$variable)
    zone_group <- before_df$Zone_Group[1]

    
    # # Before Line Chart
    # before_df <- before_df %>%
    #     mutate(var = !!var_,
    #            col = factor(ifelse(accent_average & Corridor == Zone_Group, 1, 0), levels = c(0, 1)),
    #            Corridor = factor(Corridor)) %>%
    #     filter(!is.na(var)) %>%
    #     group_by(Corridor)
    
    before_chart <- plot_ly(before_df,
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
                 yaxis = list(tickformat = tick_format(metric$data_type),
                              hoverformat = tick_format(metric$data_type)),
                 title = "__plot1_title__",
                 showlegend = FALSE,
                 margin = list(t = 50)
    )
        

    
    # # Before Line Chart
    # after_df <- after_df %>%
    #     mutate(var = !!var_,
    #            col = factor(ifelse(accent_average & Corridor == Zone_Group, 1, 0), levels = c(0, 1)),
    #            Corridor = factor(Corridor)) %>%
    #     filter(!is.na(var)) %>%
    #     group_by(Corridor)
    
    after_chart <- plot_ly(after_df,
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
    ) %>% layout(xaxis = list(title = "After"),
                 yaxis = list(tickformat = tick_format(metric$data_type),
                              hoverformat = tick_format(metric$data_type)),
                 title = "__plot1_title__",
                 showlegend = FALSE,
                 margin = list(t = 50)
    )
    
    
    subplot(before_chart, after_chart, titleX = TRUE, widths = c(0.5, 0.5), margin = 0.03, shareY = TRUE) %>%
        layout(margin = list(l = 100),
               title = metric$label,
               yaxis = list(title = NULL)) %>%
        highlight(
            color = metric$highlight_color, 
            opacityDim = 0.9, 
            defaultValues = c(zone_group),
            selected = attrs_selected(
                insidetextfont = list(color = "white"), 
                textposition = "auto"),
            on = "plotly_click",
            off = "plotly_doubleclick")
    
}


get_before_after_scatter_plot <- function(df_x_scatter, df_y_scatter, metric_x, metric_y) {
    df <- full_join(
        df_y_scatter, 
        df_x_scatter, 
        by = c("Corridor", "Description"), 
        suffix = c("_y", "_x")
    )
    
    plot_ly(data = df) %>%
        add_trace(
            x = ~before_x, y = ~before_y, name = 'Before', type = "scatter", mode = 'markers',
            marker = list(
                color = LIGHT_BLUE,
                opacity = 1,
                size = 20
            ),
            customdata = ~glue(paste(
                "<b>{Description}</b>",
                "<br>Before:\nX: {data_format(metric_x$data_type)(before_x)}\nY: {data_format(metric_y$data_type)(before_y)}")),
            hovertemplate = "%{customdata}",
            hoverlabel = list(font = list(family = "Source Sans Pro"))
        ) %>%
        add_trace(
            x = ~after_x, y = ~after_y, name = 'After', type = "scatter", mode = 'markers',
            marker = list(
                color = BLUE,
                size = 20
            ),
            customdata = ~glue(paste(
                "<b>{Description}</b>",
                "<br>After:\nX: {data_format(metric_x$data_type)(after_x)}\nY: {data_format(metric_y$data_type)(after_y)}")),
            hovertemplate = "%{customdata}",
            hoverlabel = list(font = list(family = "Source Sans Pro"))
        ) %>%
        add_trace(
            x = ~after_x, y = ~after_y, name = 'Label', type = "scatter", mode = 'text', 
            text = ~Description, textposition = 'middle right', texttemplate = "    %{text}"
        ) %>% 
        add_annotations(data = df[complete.cases(df),],
                        ax = ~before_x,
                        x = ~after_x,
                        ay = ~before_y,
                        y = ~after_y,
                        xref = "x", yref = "y",
                        axref = "x", ayref = "y",
                        text = "",
                        standoff = 8,
                        startstandoff = 8,
                        arrowhead = 2,
                        showarrow = T) %>%
        layout(
            xaxis = list(
                title = metric_x$label,
                tickformat = tick_format(metric_x$data_type)
            ),
            yaxis = list(
                title = metric_y$label,
                tickformat = tick_format(metric_y$data_type)
            )
        )
}
