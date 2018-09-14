muc_odp_fetch_data = function(baseUrl = "https://www.opengov-muenchen.de",
                              path = "api/action/datastore_search",
                              ressource_id) {
  # get raw results
  raw_result <- httr::GET(url = baseUrl, path = paste0(path, "?resource_id=", ressource_id))
  
  result <- httr::content(raw_result)
  
  dplyr::bind_rows(result$result$records)
}
