library(tidync)
library(dplyr)
library(ncdf4)

get_depth <- function(
    data_depths, given_depth
) {
    # get nearest depth index
    depth_idx <- which.min(
        mapply(
            function (x, y) abs(x-y),
            data_depths,
            given_depth
        )
    )
    # get depth value based on depth index
    depth_value <- data_depths[depth_idx]

    return(depth_value)
}

get_data <- function(
    url, variables,
    start_date, stop_date,
    lon_min, lon_max,
    lat_min, lat_max,
    depth_min, depth_max
) {
    # read opendap url
    data <- tidync::tidync(url)

    # get data depth list
    data_depths <- (
        data %>% tidync::activate("D0") %>% tidync::hyper_array()
    )$depth

    # get nearest depth based on data depth list
    depth_min <- get_depth(data_depths, depth_min)
    depth_max <- get_depth(data_depths, depth_max)

    # filter data based on longitude, latitude, time and depth
    data <- data %>% tidync::hyper_filter(
        longitude = dplyr::between(longitude, lon_min, lon_max),
        latitude = dplyr::between(latitude, lat_min, lat_max),
        time = dplyr::between(time, start_date, stop_date),
        depth = dplyr::between(depth, depth_min, depth_max)
    )

    # check if there is "sea_water_velocity" in variables,
    # and append "uo" and "vo" if not exists
    if ("sea_water_velocity" %in% variables) {
        variables <- variables[!variables %in% c("sea_water_velocity")]
        if (!"uo" %in% variables) {
            variables <- append(variables, "uo")
        }
        if (!"vo" %in% variables) {
            variables <- append(variables, "vo")
        }
    }

    # convert data in tidync to dataframe and select variable
    df <- data %>% tidync::hyper_tibble(select_var = variables)

    # calculate "sea_water_velocity"
    if (any(variables %in% c("uo", "vo"))) {
        df <- df %>% dplyr::mutate(
            sea_water_velocity = sqrt(uo^2 + vo^2)
        )
    }

    return(df)

}

# read source csv
param_df <- read.csv("./sources.csv")

# define parameters
variables <- c("uo", "vo")
temporal <- "monthly"
start_date <- as.Date("2021-01-01")
stop_date <- Sys.Date()
depth_min <- 0 # meter
depth_max <- 10 # meter
lon_min <- 114.35 # degree
lon_max <- 116 # degree
lat_min <- -8.35 # degree
lat_max <- -7 # degree

# filter parameter dataframe based on defined variables
if (any(variables %in% c("uo", "vo", "sea_water_velocity"))) {
    param_df <- param_df[param_df$parameter == "sea_water_velocity", ]
} else {
    param_df <- param_df[param_df$parameter %in% variables, ]
}

# filter parameter dataframe based on temporal resolution
param_df <- param_df[param_df$temporal == temporal, ]

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
}

# set empty list of dataframe
dfs <- list()

# loop through urls and get data
for (i in seq_along(urls)) {
    df <- get_data(
        urls[i], variables,
        start_ts, stop_ts,
        lon_min, lon_max,
        lat_min, lat_max,
        depth_min, depth_max
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

# get number of rows and columns
n_rows <- length(unique(all_df$longitude))
n_cols <- length(unique(all_df$latitude))

# get xmin, xmax, ymin, ymax
x_vals <- unique(all_df$longitude)
x_vals <- x_vals[order(x_vals)]
y_vals <- unique(all_df$latitude)
y_vals <- y_vals[order(y_vals)]
time_vals <- unique(all_df$time)
time_vals <- time_vals[order(time_vals)]
time_vals <- as.numeric(time_vals - as.POSIXlt(init_date)) * 24
depth_vals <- unique(all_df$depth)
depth_vals <- depth_vals[order(depth_vals)]

x_dim <- ncdim_def("longitude", "degrees_east", x_vals)
y_dim <- ncdim_def("latitude", "degrees_north", y_vals)
time_dim <- ncdim_def("time", "h", time_vals)
depth_dim <- ncdim_def("depth", "m", depth_vals)