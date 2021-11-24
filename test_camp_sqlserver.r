library(odbc)

conn_string <- paste0("Driver={ODBC Driver 17 for SQL Server};",
                      "Server=tcp:pscorg.database.windows.net,1433;",
                      "Database=CAMP2022;",
                      "Uid=pscdbman@pscorg;",
                      "Pwd=lAqTn1HM;",
                      "Encrypt=yes;",
                      "TrustServerCertificate=no;",
                      "Connection Timeout=30;")

camp_conn <- dbConnect(odbc::odbc(),
                       .connection_string = conn_string)


unrecovered_tag_sql <- paste0("select rl.Stock, rl.BroodYear, rl.TagCode ",
                              "FROM WireTagCode rl ",
                              "LEFT JOIN (select distinct TagCode from CWDBRecovery) rc ",
                              "on rl.TagCode = rc.TagCode ",
                              "WHERE rc.TagCode is null ",
                              "ORDER BY rl.Stock, rl.BroodYear, rl.TagCode")

unrecoveried_tags <- dbGetQuery(camp_conn, unrecovered_tag_sql)
