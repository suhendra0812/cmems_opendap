library(tidync)
library(dplyr)
library(ncdf4)

get_depth <- function(data_depths, given_depth) {
    # get nearest depth index
    depth_idx <- which.min(
        mapply(
            function(x, y) abs(x - y),
            data_depths,
            given_depth
        )
    )
    # get depth value based on depth index
    depth_value <- data_depths[depth_idx]

    return(depth_value)
}

get_time <- function(data_times, given_time) {
    # get nearest time index
    time_idx <- which.min(
        mapply(
            function(x, y) abs(x - y),
            data_times,
            given_time
        )
    )
    # get time value based on time index
    time_value <- data_times[time_idx]

    return(time_value)
}

get_data <- function(url,
                     variables,
                     start_date,
                     stop_date,
                     lon_min,
                     lon_max,
                     lat_min,
                     lat_max,
                     depth_min,
                     depth_max) {
    # read opendap url
    data <- tidync::tidync(url)

    # filter data based on longitude and latitude
    data <- data %>% tidync::hyper_filter(
        longitude = dplyr::between(longitude, lon_min, lon_max),
        latitude = dplyr::between(latitude, lat_min, lat_max)
    )

    # filter data based on time
    if (start_date != stop_date) {
        data <- data %>% tidync::hyper_filter(
            time = dplyr::between(time, start_date, stop_date)
        )
    } else {
        # get data time list
        data_times <- (
            data %>% tidync::activate("D3") %>% tidync::hyper_array()
        )$time
        data <- data %>% tidync::hyper_filter(
            time = time == get_time(data_times, start_date)
        )
    }

    # define depth query
    if (!any(variables %in% c("ZSD", "VHM0"))) {
        if (depth_min != depth_max) {
            data <- data %>% tidync::hyper_filter(
                depth = dplyr::between(depth, depth_min, depth_max)
            )
        } else {
            # get data depth list
            data_depths <- (
                data %>% tidync::activate("D0") %>% tidync::hyper_array()
            )$depth
            data <- data %>% tidync::hyper_filter(
                depth = depth == get_depth(data_depths, depth_min)
            )
        }
    }

    # convert data in tidync to dataframe and select variable
    df <- data %>% tidync::hyper_tibble(select_var = variables)

    return(df)
}

# read source csv
param_df <- read.csv("./sources.csv")

# define parameter list
param_list <- list(
    arus = "sea_water_velocity",
    sst = "thetao",
    salinitas = "so",
    klorofil = "chl",
    ph = "ph",
    gelombang = "VHM0",
    kecerahan = "ZSD"
)

# define parameters
parameter <- "arus"
temporal <- "monthly"
start_date <- as.Date("2021-01-01")
stop_date <- Sys.Date()
depth_min <- 0 # meter
depth_max <- 0 # meter
lon_min <- 114.35 # degree
lon_max <- 116 # degree
lat_min <- -8.35 # degree
lat_max <- -7 # degree

if (!parameter %in% names(param_list)) {
    stop(paste(parameter, "is not valid parameter"))
}

variable <- param_list[[parameter]]

if (parameter == "arus") {
    variables <- c("uo", "vo")
} else {
    variables <- variable
}

# filter parameter dataframe based on defined variable
param_df <- param_df[param_df$parameter == variable, ]

# filter parameter dataframe based on temporal resolution
if (parameter == "gelombang") {
    param_df <- param_df[param_df$temporal == "3-hourly", ]
} else {
    param_df <- param_df[param_df$temporal == temporal, ]
}

# get initial and near-realtime date
init_date <- as.Date(param_df$init_date)
nrt_date <- as.Date(param_df$nrt_date)

# convert date to timestamp
start_ts <- as.numeric(start_date - init_date) * 24
stop_ts <- as.numeric(stop_date - init_date) * 24
nrt_ts <- as.numeric(nrt_date - init_date) * 24

# check start and stop date to nrt date and populate the opendap url
urls <- c()
if (start_ts < nrt_ts && stop_ts < nrt_ts) {
    url <- param_df$opendap_my
    urls <- append(urls, url)
} else if (start_ts >= nrt_ts && stop_ts > nrt_ts) {
    url <- param_df$opendap_nrt
    urls <- append(urls, url)
} else if (start_ts < nrt_ts && stop_ts >= nrt_ts) {
    url_my <- param_df$opendap_my
    url_nrt <- param_df$opendap_nrt
    urls <- append(urls, url_my)
    urls <- append(urls, url_nrt)
} else {
    stop("Start date must be less than stop date")
}

param_title <- param_df$title
value_min <- param_df$value_min
value_max <- param_df$value_max

# set empty list of dataframe
dfs <- list()

# loop through urls and get data
for (i in seq_along(urls)) {
    df <- get_data(
        urls[i],
        variables,
        start_ts,
        stop_ts,
        lon_min,
        lon_max,
        lat_min,
        lat_max,
        depth_min,
        depth_max
    )

    # convert datetime from numeric to datetime format
    df <- df %>% dplyr::mutate(
        time = as.POSIXct(
            time * 3600,
            origin = init_date,
            tz = "UTC"
        )
    )

    # append dataframe to dataframe list
    dfs[[i]] <- df
}

# bind dataframe list by rows
all_df <- dplyr::bind_rows(dfs)
