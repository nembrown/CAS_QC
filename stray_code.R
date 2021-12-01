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

odbc::odbcListDrivers()


# #Issue 2 Recovery Id and run years in CAS but not in MRP
# CAS_not_MRP_recid<-anti_join(cwdbrecovery, mrp_recoveries_ind, by=c("RecoveryId"="recovery_id","RunYear"= "recovery_year"))
