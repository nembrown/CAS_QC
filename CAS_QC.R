
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
cwd_recid_dupes<-cwdbrecovery %>% get_dupes(RecoveryId, RunYear, Agency)
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
  add_row(Issue_ID="6", Issue="Duplicate RecoveryID-year-Agency in CWD", Count=nrow(cwd_recid_dupes), Definition="There are Duplicate RecoveryID-year-Agency combinations in the CWD Recoveries table") %>% 
  add_row(Issue_ID="7", Issue="NA RecoveryID in CWD", Count=nrow(cwd_recid_nas), Definition="There are NA RecoveryIDs in the CWD Recoveries table") %>% 
  add_row(Issue_ID="8", Issue="Tag Codes in CWD not in WTC", Count=nrow(cwd_notin_wire), Definition="Wire Tag Codes in the CWD Recoveries table but not in the WireTagCode table") %>% 
  add_row(Issue_ID="9", Issue="Tag Codes in WTC not in CWD", Count=nrow(wire_notin_cwb), Definition="Wire Tag Codes in the WireTagCodes table but not in the CWD Recoveries table")

sheet_list<-list(Summary=explore_summary,
                 "1 - Duplicate_TagCode_WTC"=wire_dupes, 
                 "2 - NA_TagCodes_WTC"=wire_tagcode_nas,
                 "3 - Nonstand_TagCodes_WTC" = wire_tagcode_length,
                 "4 - NA_TagCodes_CWD"=cwd_tagcode_nas, 
                 "5 - Nonstand_TagCodes_CWD" = cwd_tagcode_length,
                 "6 - Duplicate_RecIDyrAg_CWD"=cwd_recid_dupes , 
                 "7 - NA_RecID_CWD "= cwd_recid_nas, 
                 "8 - TagCodes_in_CWD_notin_WTC"=cwd_notin_wire, 
                 "9 - TagCodes_in_WTC_notin_CWD"=wire_notin_cwb)

writexl::write_xlsx(sheet_list, path="CAS_QC.xlsx")



