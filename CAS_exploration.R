library(odbc)


#' Open CAS Database Connection
#'
#' Return an open connection to a CAS database
#'
#' @param db_filename File name of the CAS access database
#'
#' @return A connection to the database
#'
#' @importFrom dplyr %>% select_all as_tibble
#' @importFrom odbc odbc dbConnect dbGetQuery dbDisconnect
#'
#' @export
#'
#'
#'could the driver name be MS Access Database
openCasConnection <- function(db_filename) {
  if (!requireNamespace("odbc", quietly = TRUE)) {
    stop("The package 'odbc' is needed for this function to work -
         and may only work in MS Windows. Please install it.",
         call. = FALSE)
  }
  
  driver_name <-
    paste0("Driver={Microsoft Access Driver (*.mdb, *.accdb)};",
           "DBQ=",
           db_filename)
  
  db_conn <- dbConnect(odbc(), .connection_string = driver_name)
  return(db_conn)
}

openCasConnection("CAMP2022BE.accdb")
getwd()

odbc::odbcListDrivers()

casdb<-openCasConnection(file.path(getwd(), "CAMP2022BE.accdb"))
dbListTables(casdb)
