convert_to_default_dt_string <- function(datetime_val = NA, date_val) {
  # Function which converts the datetime strings to the standard format of "%Y-%m-%d %H:%M:%S"
  # or "%Y-%m-%d" if only date_val provided
  
  # Iso 8601 format: "%Y-%m-%dT%H:%M:%S"
  iso8601_format = "[:digit:]{4}-[:digit:]{2}-[:digit:]{2}T[:digit:]{2}:[:digit:]{2}:[:digit:]{2}"
  # hour_minute format: "%H.%M"
  hour_minute_format = "[:digit:]{2}.[:digit:]{2}$"
  
  iso_date_format = "[:digit:]{4}-[:digit:]{2}-[:digit:]{2}$"
  # date format: "%Y.%m.%d"
  date_format = "[:digit:]{4}.[:digit:]{2}.[:digit:]{2}$"
  
  if (!is.na(datetime_val)) {
    # Extract date from date_val in case it is given in datetime format
    date_val = str_extract(date_val, "[:digit:]{4}-[:digit:]{2}-[:digit:]{2}")
    
    # If datetime_val matches Iso 8601 format: Replace "T" with " " and return
    if (stringr::str_detect(datetime_val, iso8601_format)) {
      return(stringr::str_replace(datetime_val, "[:digit:]{4}-[:digit:]{2}-[:digit:]{2}T",
                                  paste0(date_val, " ")))
    }
    # If datetime_val matches hour_minute_format: Add date and seconds
    if (stringr::str_detect(datetime_val, hour_minute_format)) {
      # Replace "." with ":" in datetime_val
      datetime_val = gsub("\\.", ":", datetime_val)
      return(paste0(date_val, " ", datetime_val, ":00"))
    }
  }
  if (is.na(datetime_val)) {
    # date value in iso date format: return it as it is
    if (stringr::str_detect(date_val, iso_date_format)) {
      return(date_val)
    }
    # if date value in date_format: convert it to iso_date_format
    if (stringr::str_detect(date_val, date_format)) {
      return(gsub( "\\.", "-", date_val))
    }# if date value in iso8601_format: convert it to iso_date_format
    if (stringr::str_detect(date_val, iso8601_format)) {
      return(str_extract(date_val, "[:digit:]{4}-[:digit:]{2}-[:digit:]{2}"))
    }
  }
  warning("Datetime value had undefined format")
  NA
  
}


muc_odp_fetch_bike_count_resource_ids <- function(
  baseUrl = "https://www.opengov-muenchen.de",
  path = "/api/3/action/package_search?q=daten-der-raddauerzaehlstellen-muenchen",
  limit = "100") {
  require(dplyr)
  require(tidyr)
  
  # get raw_results
  raw_result <- httr::GET(url = "https://www.opengov-muenchen.de",
                          path = paste0(path, "&rows=", limit))
  results <- httr::content(raw_result)$result$results
  
  # Extract dataset name and id from results
  resource_information <- list(
    data_collection_name = unlist(lapply(results, function(x) x$name)),
    datasets = lapply(results, function(x) bind_rows(
      lapply(x$resources, function(y) data.frame(
        name = y$name,
        resource_id = y$id,
        stringsAsFactors = F)))
    )
  ) %>% 
    bind_rows() %>% 
    tidyr::unnest()
  
  # extract month and year from name
  german_month_names <- c("Januar", "Februar", "März", "April", "Mai", "Juni", "Juli", "August", "September",
                          "Oktober", "November", "Dezember")
  
  correct_name <- function(name) {
    # Change name string of Juli 2018 (15 Minutes missing in name) - as of 13.10.2018
    if (name == "Daten der Raddauerzählstellen München Juli 2018") {
      print("FOUND JULI")
      name <- "15 Minuten-Werte - Daten der Raddauerzählstellen München Juli 2018"
    }
    # Change name for "#Month#" 2018 to März 2017 (data_collection_name says 2017!)
    if (name == "15 Minuten-Werte - Daten der Raddauerzählstellen München #Monat# 2018") {
      print("FOUND MARCH")
      name <- "15 Minuten-Werte - Daten der Raddauerzählstellen München März 2017"
    }
    
    return(name)
  }
  
  resource_information <- resource_information %>% 
    mutate(name = sapply(name, function(x) correct_name(x)),
           year = stringr::str_extract(name, "[:digit:]{4}"),
           month = stringr::str_extract(name, paste(german_month_names, collapse = "|")),
           month = match(month, german_month_names))
  
  ids_15_min <- resource_information %>% 
    filter(stringr::str_detect(name, "15 Minuten")) %>% 
    arrange(year, month)
  
  #filter for names containing "Tageswerte und Wetter"
  ids_daily <- resource_information %>% 
    filter(stringr::str_detect(name, "Tageswerte und Wetter")) %>% 
    arrange(year, month)
  
  return(list(
    ids_15_min = ids_15_min,
    ids_daily = ids_daily
  ))
}


muc_odp_fetch_data <- function(baseUrl = "https://www.opengov-muenchen.de",
                               path = "api/3/action/datastore_search",
                               limit = "100000",
                               resource_id,
                               destination = "./data/",
                               force_fetch = FALSE) {
  
  file_name = paste0(destination, resource_id, ".RDS")
  
  # check if data already fetched
  if (file.exists(file_name) & !force_fetch) {
    print(paste("File for resource already exists at", file_name))
    return(readRDS(file_name))
  }
  else {
    # get raw results
    raw_result <- httr::GET(url = baseUrl, path = paste0(path,
                                                         "?resource_id=", 
                                                         resource_id,
                                                         "&limit=",
                                                         limit))
    
    print(paste0("Fetching data from ", 
                 path,
                 "?resource_id=", 
                 resource_id,
                 "&limit=",
                 limit))
    
    result <- httr::content(raw_result)
    print(paste('Fetched', length(result$result$records), 'results...'))
    
    res <- dplyr::bind_rows(result$result$records)
    
    # write results to ./data/ folder
    saveRDS(res, file = file_name)
    
    return(res)
  }
}


muc_odp_convert_bike_counter_types <- function(dt) {
  require(dplyr)
  require(readr)
  
  dt %>% 
    transmute(
      besonderheiten = as.character(besonderheiten),
      zaehlstelle = as.factor(zaehlstelle),
      longitude = as.numeric(longitude),
      latitude = as.numeric(latitude),
      richtung_1 = as.character(richtung_1),
      richtung_2 = as.character(richtung_2),
      id = `_id`
    )
}


muc_odp_convert_bike_cnt_15_min_types <- function(dt) {
  require(dplyr)
  
  dt %>% 
    transmute(
      datum = lubridate::fast_strptime(sapply(datum, function(x)
        convert_to_default_dt_string(date_val = x)),
        format = "%Y-%m-%d", lt = FALSE),
      uhrzeit_start = lubridate::fast_strptime(mapply(function(x, y) convert_to_default_dt_string(x, y),
                                                      uhrzeit_start,
                                                      datum), format = "%Y-%m-%d %H:%M:%S", lt = FALSE),
      uhrzeit_ende = lubridate::fast_strptime(mapply(function(x, y) convert_to_default_dt_string(x, y),
                                                     uhrzeit_ende,
                                                     datum), format = "%Y-%m-%d %H:%M:%S", lt = FALSE),
      gesamt = as.integer(gesamt),
      zaehlstelle = as.factor(zaehlstelle),
      richtung_1 = as.integer(richtung_1),
      richtung_2 = as.integer(richtung_2)
    )
}


muc_odp_convert_bike_cnt_day_types <- function(dt) {
  require(dplyr)
  
  
  dt %>% 
    transmute(
      datum = lubridate::fast_strptime(sapply(datum, function(x)
        convert_to_default_dt_string(date_val = x)),
        format = "%Y-%m-%d", lt = FALSE),
      uhrzeit_start = lubridate::fast_strptime(mapply(function(x, y) convert_to_default_dt_string(x, y),
                                                      uhrzeit_start,
                                                      datum), format = "%Y-%m-%d %H:%M:%S", lt = FALSE),
      gesamt = as.integer(gesamt),
      zaehlstelle = as.factor(zaehlstelle),
      richtung_1 = as.integer(richtung_1),
      richtung_2 = as.integer(richtung_2),
      bewoelkung = as.numeric(bewoelkung),
      sonnenstunden = as.numeric(gsub(",", ".", `sonnenstunden`)),
      niederschlag = as.numeric(gsub(",", ".", `niederschlag`)),
      max_temp = as.numeric(gsub(",", ".", `max-temp`)),
      min_temp = as.numeric(gsub(",", ".", `min-temp`)),
      id = `_id`
    )
}

muc_odp_add_direction_info <- function(dt) {
  require(dplyr)
  
  OUT_OF_CENTER = "out of center"
  INTO_CENTER = "into center"
  direction_mapper <- list(
    Olympia = list(
      richtung_1 = INTO_CENTER,
      richtung_2 = OUT_OF_CENTER
    ),
    Hirsch = list(
      richtung_1 = OUT_OF_CENTER,
      richtung_2 = INTO_CENTER
    ),
    Margareten = list(
      richtung_1 = OUT_OF_CENTER,
      richtung_2 = INTO_CENTER
    ),
    Erhardt = list(
      richtung_1 = OUT_OF_CENTER,
      richtung_2 = INTO_CENTER
    ),
    Kreuther = list(
      richtung_1 = INTO_CENTER,
      richtung_2 = OUT_OF_CENTER
    ),
    Arnulf = list(
      richtung_1 = INTO_CENTER,
      richtung_2 = OUT_OF_CENTER
    )
  )
  
  # Gather columns -> 1 column with values, other column with direction information
  dt <- dt %>% 
    gather(key = direction, value = count, richtung_1, richtung_2)
  
  # Change direction names
  dt$direction <- factor(unlist(mapply(function(x, y) direction_mapper[[x]][y], 
                                       dt$zaehlstelle, dt$direction)))
  
  return(dt)
}