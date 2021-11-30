
# load libraries ----------------------------------------------------------

library(odbc)
library(tidyverse)
# remotes::install_git("https://github.com/Pacific-salmon-assess/tagFisheryMapping")
library(tagFisheryMapping)
library(dbplyr)
library(janitor)
library(xlsx)
library(purrr)
library(writexl)
library(stringr)


# Open database connection ------------------------------------------------

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


casdb<-openCasConnection(file.path(getwd(), "CAMP2022BE.accdb"))
dbListTables(casdb)


# Explorations in CAS ------------------------------------------------------------
# Pull tables from CAS
wiretagcode<-tbl(casdb, "WireTagCode")
cwdbrecovery<-tbl(casdb, "CWDBRecovery")

#some tidying to make the explorations work
"%notin%" <- Negate("%in%")
wiretagcode<- wiretagcode %>% as_tibble()
cwdbrecovery<- cwdbrecovery %>% as_tibble()
wiretagcode_unq<-wiretagcode %>% select(TagCode) 
cwdbrecovery_unq<-cwdbrecovery %>% select(TagCode)
  
#Explorations
wire_dupes<-wiretagcode %>% get_dupes(TagCode)
wire_tagcode_nas<-wiretagcode %>% filter(is.na(TagCode))
wire_tagcode_length<-wiretagcode %>% add_column(wt_string = str_length(wiretagcode$TagCode))%>% filter(wt_string != 6)
cwd_tagcode_nas<-cwdbrecovery %>% filter(is.na(TagCode))
cwd_tagcode_length<-cwdbrecovery %>% add_column(wt_string = str_length(cwdbrecovery$TagCode))%>% filter(wt_string != 6)
cwd_recid_dupes<-cwdbrecovery %>% get_dupes(RecoveryId, RunYear)
cwd_recid_nas<-cwdbrecovery %>% filter(is.na(RecoveryId))
cwd_notin_wire<-cwdbrecovery %>% filter(TagCode %notin% wiretagcode_unq$TagCode)
wire_notin_cwb<-wiretagcode  %>% filter(TagCode %notin% cwdbrecovery_unq$TagCode)


#Summary table
explore_summary <- data.frame(Issue_ID=character(), Issue=character(), Count=integer(),
                 Definition=character(), 
                 stringsAsFactors=FALSE)

explore_summary <- explore_summary  %>% 
                   add_row(Issue_ID="1", Issue="Duplicate Tag Codes in WTC", Count=nrow(wire_dupes), Definition="Duplicate wire tag codes in the WireTagCode table") %>% 
                   add_row(Issue_ID="2", Issue="NA Tag Codes in WTC", Count=nrow(wire_tagcode_nas), Definition="NA wire tag codes in the WireTagCode table") %>% 
                   add_row(Issue_ID="3", Issue="Tag Codes nonstandard length in WTC", Count=nrow(wire_tagcode_length), Definition="The wire tag codes in the WireTagCode table that are not 6 characters long (the standard length)") %>% 
                   add_row(Issue_ID="4", Issue="NA Tag Codes in CWD", Count=nrow(cwd_tagcode_nas), Definition="NA wiretagcodes in the CWD Recoveries table") %>% 
                   add_row(Issue_ID="5", Issue="Tag Codes nonstandard length in CWD", Count=nrow(cwd_tagcode_length), Definition="The wire tag codes in the CWD Recoveries table that are not 6 characters long (the standard length)") %>% 
                   add_row(Issue_ID="6", Issue="Duplicate RecoveryID in CWD", Count=nrow(cwd_recid_dupes), Definition="There are Duplicate RecoveryIDs in the CWD Recoveries table") %>% 
                   add_row(Issue_ID="7", Issue="NA RecoveryID in CWD", Count=nrow(cwd_recid_nas), Definition="There are NA RecoveryIDs in the CWD Recoveries table") %>% 
                   add_row(Issue_ID="8", Issue="Tag Codes in CWD not in WTC", Count=nrow(cwd_notin_wire), Definition="Wire Tag Codes in the CWD Recoveries table but not in the WireTagCode table") %>% 
                   add_row(Issue_ID="9", Issue="Tag Codes in WTC not in CWD", Count=nrow(wire_notin_cwb), Definition="Wire Tag Codes in the WireTagCodes table but not in the CWD Recoveries table")

sheet_list<-list(Summary=explore_summary,
                 "1 - Duplicate_TagCode_WTC"=wire_dupes, 
                 "2 - NA_TagCodes_WTC"=wire_tagcode_nas,
                 "3 - Nonstand_TagCodes_WTC" = wire_tagcode_length,
                 "4 - NA_TagCodes_CWD"=cwd_tagcode_nas, 
                 "5 - Nonstand_TagCodes_CWD" = cwd_tagcode_length,
                 "6 - Duplicate_RecoveryID_CWD"=cwd_recid_dupes , 
                 "7 - NA_RecoveryID_CWD "= cwd_recid_nas, 
                 "8 - TagCodes_in_CWD_notin_WTC"=cwd_notin_wire, 
                 "9 - TagCodes_in_WTC_notin_CWD"=wire_notin_cwb)

writexl::write_xlsx(sheet_list, path="CAS_QC.xlsx")



# MRP to CAS comparisons --------------------------------------------------
#Open connection to MRP 
mrp_recoveries<-getDfoTagRecoveries(1950:2022)

#filter MRP database by indicator tags only
mrp_recoveries_ind<-mrp_recoveries %>% filter(tag_code %in% wiretagcode_unq$TagCode)
View(mrp_recoveries_ind)

#isolating just dfo recoveries in CAS
dfo_cwdbrecovery <- cwdbrecovery %>% filter(Agency=="CDFO")


# What's missing ----------------------------------------------------------

#Recovery Id and run years in MRP but not in CAS
mrp_not_CAS_recid<-anti_join(mrp_recoveries_ind, cwdbrecovery, by=c("recovery_id"="RecoveryId", "recovery_year"="RunYear"))

#Where are the mismatches the most common?
mrp_not_CAS_recid_year<- mrp_not_CAS_recid %>% group_by(recovery_year) %>% summarise(n= n()) %>% arrange(desc(n))
mrp_not_CAS_recid_location<- mrp_not_CAS_recid %>% group_by(rec_location_code) %>% summarise(n= n()) %>% arrange(desc(n))
mrp_not_CAS_recid_fishery<- mrp_not_CAS_recid %>% group_by(psc_fishery_id) %>% summarise(n= n()) %>% arrange(desc(n))

#Recovery Id and run years in CAS but not in MRP
CAS_not_MRP_recid<-anti_join(cwdbrecovery, mrp_recoveries_ind, by=c("RecoveryId"="recovery_id","RunYear"= "recovery_year"))
#This returns a lot but I think we need to compare to just DFO recoveries because that's all that's in MRP
dfo_CAS_not_MRP_recid<-anti_join(dfo_cwdbrecovery, mrp_recoveries_ind, by=c("RecoveryId"="recovery_id","RunYear"= "recovery_year"))

#Where are the mismatches the most common?  
dfo_CAS_not_MRP_recid_Fishery<- dfo_CAS_not_MRP_recid %>% group_by(Fishery) %>% summarise(n= n()) %>% arrange(desc(n))
dfo_CAS_not_MRP_recid_RecoverySite<- dfo_CAS_not_MRP_recid %>% group_by(RecoverySite) %>% summarise(n= n())%>% arrange(desc(n))

#Summary table
compare_CAS_MRP_summary <- data.frame(Issue_ID=character(), Issue=character(), Count=integer(),
                              Definition=character(), 
                              stringsAsFactors=FALSE)

compare_CAS_MRP_summary  <- compare_CAS_MRP_summary   %>% 
  add_row(Issue_ID="1", Issue="Recovery id by year in MRP not CAS", Count=nrow(mrp_not_CAS_recid), Definition="Recovery Id and run year combinations that are in MRP database but not in CAS database ") %>% 
  add_row(Issue_ID="1.1", Issue="Recovery id by year in MRP not CAS - year", Count=nrow(mrp_not_CAS_recid_year), Definition="Recovery Id and run year combinations that are in MRP database but not in CAS database, summarized by year") %>% 
  add_row(Issue_ID="1.2", Issue="Recovery id by year in MRP not CAS - location", Count=nrow(mrp_not_CAS_recid_location), Definition="Recovery Id and run year combinations that are in MRP database but not in CAS database, summarized by recovery location") %>% 
  add_row(Issue_ID="1.3", Issue="Recovery id by year in MRP not CAS - fishery", Count=nrow(mrp_not_CAS_recid_fishery), Definition="Recovery Id and run year combinations that are in MRP database but not in CAS database, summarized by psc fishery id") %>% 
  add_row(Issue_ID="2", Issue="Recovery id by year in CAS not MRP", Count=nrow(CAS_not_MRP_recid), Definition="Recovery Id and run year combinations that are in CAS database but not in MRP database ") %>% 
  add_row(Issue_ID="3", Issue="Recovery id by year in dfo_CAS not MRP", Count=nrow(dfo_CAS_not_MRP_recid), Definition="Recovery Id and run year combinations that are in the DFO recoveries in the CAS database but not in MRP database ") %>% 
  add_row(Issue_ID="3.1", Issue="Recovery id by year in dfo_CAS not MRP - fishery", Count=nrow(dfo_CAS_not_MRP_recid_Fishery), Definition="Recovery Id and run year combinations that are in the DFO recoveries in the CAS database but not in MRP database, summarized by Fishery ") %>% 
  add_row(Issue_ID="3.2", Issue="Recovery id by year in dfo_CAS not MRP - recovery site", Count=nrow(dfo_CAS_not_MRP_recid_RecoverySite), Definition="Recovery Id and run year combinations that are in the DFO recoveries in the CAS database but not in MRP database, summarized by recovery site ") 

sheet_list_compare_CAS_MRP<-list(Summary=compare_CAS_MRP_summary,
                 "1 - RecID_MRP_not_CAS"= mrp_not_CAS_recid,
                 "1.1 - year"= mrp_not_CAS_recid_year,
                 "1.2 - location"= mrp_not_CAS_recid_location,
                 "1.3 - fishery"= mrp_not_CAS_recid_fishery,
                 "2 - RecID_CAS_not_MRP"= CAS_not_MRP_recid, 
                 "3 - RecID_dfoCAS_not_MRP"= dfo_CAS_not_MRP_recid, 
                 "3.1 - fishery"= dfo_CAS_not_MRP_recid_Fishery,
                 "3.2 - location"= dfo_CAS_not_MRP_recid_RecoverySite)

writexl::write_xlsx(sheet_list_compare_CAS_MRP, path="MRP_vs_CAS_QC.xlsx")


# What's altered ----------------------------------------------------------
#Work only with the ones in MRP that are in CAS
mrp_yes_CAS_recid<- anti_join(mrp_recoveries_ind, mrp_not_CAS_recid, by=c("recovery_id", "recovery_year") )

#and Work only with the ones in CAS that are in MRP
CAS_yes_MRP_recid<-anti_join(cwdbrecovery, CAS_not_MRP_recid, by=c("RecoveryId","RunYear"))

#which ones in MRP have a different value for the cwt estimate than the CAS database
mrp_CAS_estimate_mismatch<-anti_join(mrp_yes_CAS_recid,CAS_yes_MRP_recid, by=c("recovery_id"="RecoveryId", "recovery_year"="RunYear", "cwt_estimate"="AdjustedEstimatedNumber") )

mrp_CAS_estimate_mismatch_trim<- mrp_CAS_estimate_mismatch %>% select(recovery_id, recovery_year, cwt_estimate)
mrp_CAS_estimate_mismatch_trim_join<-left_join(mrp_CAS_estimate_mismatch_trim, CAS_yes_MRP_recid %>% select (RecoveryId, RunYear, AdjustedEstimatedNumber), by=c("recovery_id"="RecoveryId", "recovery_year"="RunYear"))
mrp_CAS_estimate_mismatch_trim_join_diff<- mrp_CAS_estimate_mismatch_trim_join %>% add_column(diff_estimates= mrp_CAS_estimate_mismatch_trim_join$AdjustedEstimatedNumber - mrp_CAS_estimate_mismatch_trim_join$cwt_estimate) %>% arrange(desc(diff_estimates))

View(mrp_CAS_estimate_mismatch_trim)
View(CAS_yes_MRP_recid)
