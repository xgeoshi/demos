# Renaissance Insurance: case #2.
# Problem type: list of clients suspects in fraud activity.
# Created and tested on Mac (HighSierra OS) / R v.3.4.2
# Date: 13-10-2017
# Georgie Shimanovsky | e-mail: geoshi@me.com

require(data.table) || install.packages("data.table")

# Read CSV file in windows encoding.
csv2.path <- "data.csv" #datafile path
df.test2  <- read.csv2(csv2.path, stringsAsFactors = FALSE,
                       fileEncoding = "windows-1251")[, 1:3]
dt.test2  <- as.data.table(df.test2) #Data Frame to data.table
names(dt.test2) <- c("case_id", "part1", "part2") #Columns rename

# Tidy data: all names in 1 column and it's related descript part(1|2) in another
test2.tidy <- data.table::melt(dt.test2, id = 1)

# Add column: sum of total accidents by unique name
accid.num <- test2.tidy[, .(case_id, cases_ttl = .N), by = value][]

# "cases_ttl" column split into several columns that idicates case's sums.
# each participant belonging is denoted by +1 value instead of listing names.
cast.accid <- dcast(accid.num, case_id ~ cases_ttl, fun.aggregate = length)

# Rank cases_id fraud chance by accidents history of both participants.
# If both participants have only by 1 accident each: fraud risk is lower ("Low")
cast.accid[cast.accid$"1" == 2, "fraud" := "Low"][]

# If one of participants have more than 1 accident: risk is "Medium".
cast.accid[cast.accid$"1" == 1, "fraud" := "Medium"][]

# If both participants within case have more than 1 accident: risk is "High".
cast.accid[cast.accid$"1" == 0, "fraud" := "High"][]

# Merge fraud ranking with with total_cases
dt.full <- merge(accid.num, cast.accid, by = "case_id")

# List of names with high fraud suspicion
dt.suspicion <- dt.full[fraud == "High", .(name = unique(value))]
print(dt.suspicion)