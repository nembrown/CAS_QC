library(odbc)
library(tidyverse)
remotes::install_git("https://github.com/Pacific-salmon-assess/tagFisheryMapping")
library(tagFisheryMapping)
library(dbplyr)
library(janitor)
library(xlsx)
library(purrr)
library(writexl)

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

odbc::odbcListDrivers()

casdb<-openCasConnection(file.path(getwd(), "CAMP2022BE.accdb"))
dbListTables(casdb)

#Functions are generally get something and then when you run it you need to assign to a df
getCasFisheryTbl <- function(db_conn, stateprov_prefix) {
  fishery_sql <-
    paste0("SELECT Name as ctc_fishery_name, Description as ctc_fishery_description, CWDBFishery as psc_fishery_id, ",
           "StateProvince as state_province, WaterType as water_type, Sector, Region, Area, Location, SubLocation, ",
           "StartMonth as start_month, StartDay as start_day, EndMonth as end_month, EndDay as end_day, ",
           "StartPeriod as start_period, EndPeriod as end_period, Stock ",
           "FROM FisheryLookup x ",
           "INNER JOIN (SELECT xc.Fishery, cc.Name, cc.Description FROM FisheryCFileFishery xc ",
           "            INNER JOIN CFileFishery cc ON xc.CFileFishery = cc.Id) xc ON x.Fishery = xc.Fishery ",
           "WHERE StateProvince = '", stateprov_prefix, "'")
  
  fishery_map_df <-
    dbGetQuery(db_conn, fishery_sql) %>%
    mutate(across(where(is.factor), as.character)) %>%
    as_tibble() %>%
    formatFisheryDef()
  
  return(fishery_map_df)
}

bc_fishery_table<-getCasFisheryTbl(casdb, "2")

#translate_sql translates TO SQL from R in brackets
translate_sql(merge(x, y))

fisherylkup<-tbl(casdb, "FisheryLookup")
wiretagcode<-tbl(casdb, "WireTagCode")
cwdbrecovery<-tbl(casdb, "CWDBRecovery")

wiretagcode_unq<-wiretagcode %>% as_tibble() %>% select(TagCode) %>% as.data.frame()
cwdbrecovery_unq<-cwdbrecovery %>% as_tibble() %>% select(TagCode) %>% as.data.frame()
  
#Explorations
wire_dupes<-wiretagcode %>% as_tibble() %>% get_dupes(TagCode)
cwd_notin_wire<-cwdbrecovery %>% as_tibble() %>% filter(TagCode %notin% wiretagcode_unq$TagCode)
wire_notin_cwb<-wiretagcode %>% as_tibble() %>% filter(TagCode %notin% cwdbrecovery_unq$TagCode)



#Summary table
explore_summary <- data.frame(Issue=character(), Count=integer(),
                 Definition=character(), 
                 stringsAsFactors=FALSE)

explore_summary <- explore_summary  %>% add_row(Issue="Duplicate WireTagCodes", Count=nrow(wire_dupes), Definition="There are duplicate wiretagcodes in the WireTagCode table") %>% 
                                        add_row(Issue="CWD not in WireTag", Count=nrow(cwd_notin_wire), Definition="Wire Tag Codes in the CWD Recoveries table but not in the WireTagCode table") %>% 
                                        add_row(Issue="WireTag not in CWD", Count=nrow(wire_notin_cwb), Definition="Wire Tag Codes in the WireTagCodes table but not in the CWD Recoveries table")

sheet_list<-list(Summary=explore_summary,Wire_Dupes=wire_dupes,CWD_notin_wire=cwd_notin_wire, Wire_notin_CWD=wire_notin_cwb)

writexl::write_xlsx(sheet_list, path="CAS_QC.xlsx")
