# Renaissance Insurance: case #1.
# Problem type: binary classification.
# Created and tested on Mac (HighSierra OS) / R v.3.4.2
# Total running time ~ 10 minutes on MacBookPro i7, 8Gb
# Date: 13-10-2017
# Georgie Shimanovsky | e-mail: geoshi@me.com

## Load or install packages if it doesn't exist.
require(data.table) || install.packages("data.table") #Fast data read/manipulate
require(h2o) || install.packages("h2o") #for Machine Learning

set.seed(44) #Set seed for reproducability

## Read dataset from current working directory.
wd.path <- getwd() #path to current working directory
data.filename <- "Данные для задачи - Ренессанс.txt" #data filename
dtfile.path <- paste0(c(wd.path, data.filename), collapse = "/") #full filepath
NAs <- c("<Пусто>", "N", "n/d") #values stands for NAs in dataset
dt.full <- fread(dtfile.path, na.strings = NAs) #read csv w/NA val replacement

## Format and Clean data
#add new column: "VEHICLE_LOCAL" (Local Vehicle Manufacturer: Logical, Boolean)
regex.val <- "[а-яА-Я][^Другая марка (Иностранного производства)]|Lada"
dt.full[, VEHICLE_LOCAL := grepl(regex.val, VEHICLE_MAKE)][]

#Removing observations contain NA in "VEHICLE_MAKE" & "VEHICLE_MAKE" columns.
dt.cln <- dt.full[!(VEHICLE_MAKE == "NA" & VEHICLE_MODEL == "NA")]

#Fix "POLICY_MIN_DRIVING_EXPERIENCE" column data.
#Converting calendar year values into the driving experience scale.
dt.cln[, POLICY_MIN_DRIVING_EXPERIENCE := 
               ifelse(nchar(POLICY_MIN_DRIVING_EXPERIENCE) != 4,
                      POLICY_MIN_DRIVING_EXPERIENCE,
                      2015L - POLICY_MIN_DRIVING_EXPERIENCE)][]

#add new column: "VEHICLE_TYPE"
#Type is taken from the "VEHICLE_MODEL" column with "Другая модель..." entries
#for the rest entries (non-other) "Легковое ТС" is assigned.
dt.cln[, VEHICLE_TYPE := ifelse(grepl("Другая модель", VEHICLE_MODEL),
                                gsub("^.*\\(|)$", "", VEHICLE_MODEL),
                                "Легковое ТС")][]

#Replacing all "Другая марка ..." matches both of foreign and local vehicles
#in to single value "OTHER" in "VEHICLE_MAKE" column.
dt.cln[, VEHICLE_MAKE := ifelse(grepl("Другая марка", VEHICLE_MAKE),
                                "OTHER", VEHICLE_MAKE)][]

#Replacing all "Другая марка ..." matches both of foreign and local vehicles
#in to single value "OTHER" in "VEHICLE_MODEL" column.
dt.cln[, VEHICLE_MODEL := ifelse(grepl("Другая модель", VEHICLE_MODEL),
                                "OTHER", VEHICLE_MODEL)][]

#add new column: "CLIENT_REGISTRATION_NEAR"
#for Moscow and St.Petersburg areas - Suburb and City name become as one value.
#for other areas "Прочее" appears.
dt.cln[, CLIENT_REGISTRATION_NEAR := gsub("Московская", "Москва",
                                          CLIENT_REGISTRATION_REGION)][]
dt.cln[, CLIENT_REGISTRATION_NEAR := gsub("Ленинградская", "Санкт-Петербург",
                                          CLIENT_REGISTRATION_NEAR)][]
dt.cln[, CLIENT_REGISTRATION_NEAR :=
               ifelse(CLIENT_REGISTRATION_NEAR %in%
                              c("Москва", "Санкт-Петербург", NA), 
                      CLIENT_REGISTRATION_NEAR, "Прочее")][]

#add new column: "REGISTR_BRANCH_MATCH"
#logical for matches between "CLIENT_REGISTRATION_NEAR" & "POLICY_BRANCH"
dt.cln[, REGISTR_BRANCH_MATCH := CLIENT_REGISTRATION_NEAR == POLICY_BRANCH][]

#Function converts detect certain column classes and conert htem to other class.
class_2_class <- function(dt, detect.fun, convert.fun) {
        detect.cols.lgc <- sapply(dt, detect.fun)
        detect.col.names <- names(dt)[detect.cols.lgc]
        
        if (substitute(detect.fun) == "is.integer" &&
            substitute(convert.fun) == "as.logical") {
                detected.data <- dt.cln[, detect.col.names, with = FALSE]
                intg.data.rng <- lapply(detected.data, range)
                intg.is.lgcl <- sapply(intg.data.rng,
                                       function(x) x[1] == 0 & x[2] == 1)
                intg.is.lgcl <- intg.is.lgcl[!is.na(intg.is.lgcl)]
                detect.col.names <- names(intg.is.lgcl[intg.is.lgcl])
        }
        
        dt[, (detect.col.names) := lapply(.SD, convert.fun),
           .SDcols = detect.col.names][] # Convert upon column names
}

#integer class columns with 0 & 1 values only -> covert to logical.
class_2_class(dt.cln, detect.fun = is.integer, convert.fun = as.logical)

# Split data into Train/Test tables upon values from "DATA_TYPE" column.
split.cln <- split(dt.cln, by = "DATA_TYPE")

#Train set
trainset     <- split.cln[["TRAIN"]]
train.rows   <- trainset[,.N]
train.leng.8 <- ceiling(train.rows * 0.8)
trn.resamp   <- trainset[sample(seq(train.rows))] #shuffle trainset row order
train.8      <- trn.resamp[1:train.leng.8]

#Validation Set: 20% of shuffled train set
valid.2      <- trn.resamp[(train.leng.8 + 1):train.rows]

#Test set
testset      <- split.cln[["TEST"]]

##H2O Cluster Initialization: multithread (all thread - 1) & RAM: 4G.
h2o.init(ip = "localhost", port = 54321, nthreads = -1, max_mem_size = "6g")

#Convert Data.Tables to H2O format.
h2o.no_progress() #Turn off progress bar while converting data process.
h2o.train8     <- as.h2o(train.8)
h2o.trainfull  <- as.h2o(trn.resamp)
h2o.valid      <- as.h2o(valid.2)
h2o.test       <- as.h2o(testset)
h2o.show_progress() #Turn on progress bar.

#Set predictors
y.h2o <- "POLICY_IS_RENEWED"
x.h2o <- setdiff(names(h2o.train8), c(y.h2o, "DATA_TYPE", "POLICY_ID"))

# #Run H2O model
# Machine Learning H2O GBM algorithm w/hyperparameters search with depth = 60.
# Set to 60 (instead of search range) in order to save time on reproduction.

hyper.params <-  list(
        ## restrict the search to the range of max_depth (pre-searched)
        max_depth = 60,
        ## search a large space of row sampling rates per tree
        sample_rate = seq(0.2, 1, 0.01),
        ## search a large space of column sampling rates per split
        col_sample_rate = seq(0.2, 1, 0.01),
        ## search a large space of column sampling rates per tree
        col_sample_rate_per_tree = seq(0.2, 1, 0.01),
        ## search a large space of how column sampling per split should change
        ## as a function of the depth of the split
        col_sample_rate_change_per_level = seq(0.9, 1.1, 0.01),
        ## search a large space of the number of min rows in a terminal node
        min_rows = 2 ^ seq(0, log2(nrow(h2o.train8)) - 1, 1),
        ## search a large space of the number of bins for split-finding
        ## for continuous and integer columns
        nbins = 2 ^ seq(4, 10, 1),
        ## search a large space of the number of bins for split-finding
        ## for categorical columns
        nbins_cats = 2 ^ seq(4, 12, 1),
        ## search a few minimum required relative error improvement thresholds
        ## for a split to happen
        min_split_improvement = c(0, 1e-8, 1e-6, 1e-4),
        ## try all histogram types (QuantilesGlobal and RoundRobin are good
        ## for numeric columns with outliers)
        histogram_type = c("UniformAdaptive", "QuantilesGlobal", "RoundRobin")
)

search.criteria <-  list(
        ## Random grid search
        strategy = "RandomDiscrete",
        ## limit the runtime to:
        max_runtime_secs = 600,
        ## build no more than 100 models
        max_models = 100,
        ## random number generator seed to make sampling of parameter
        ## combinations reproducible
        seed = 44,
        ## early stopping once the leaderboard of the top 5 models is
        ## converged to 0.1% relative difference
        stopping_rounds = 5,
        stopping_metric = "AUC",
        stopping_tolerance = 1e-3
)

grid.pars2 <- list(
        ## hyper parameters
        hyper_params = hyper.params,
        ## hyper-parameter search configuration (see above)
        search_criteria = search.criteria,
        ## which algorithm to run
        algorithm = "gbm",
        ## identifier for the grid, to later retrieve it
        grid_id = "final_grid",
        ## standard model parameters
        x = x.h2o, y = y.h2o, training_frame = h2o.train8,
        validation_frame = h2o.valid,
        ## more trees is better if the learning rate is small enough
        ## use "more than enough" trees - we have early stopping
        ntrees = 1000,
        ## smaller learning rate is better since we have
        ## learning_rate_annealing, we can afford to start
        ## with a bigger learning rate
        learn_rate = 0.01,
        ## learning rate annealing: learning_rate shrinks by 1% after
        ## every tree (use 1.00 to disable, but then lower the
        ## learning_rate)
        learn_rate_annealing = 0.99,
        ## early stopping based on timeout (no model should take
        ## more than 1 hour - modify as needed)
        max_runtime_secs = 600,
        ## early stopping once the validation AUC doesn't improve
        ## by at least 0.01% for 5 consecutive scoring events
        stopping_rounds = 5, stopping_tolerance = 1e-4,
        stopping_metric = "AUC",
        ## score every 10 trees to make early stopping reproducible
        ## (it depends on the scoring interval)
        score_tree_interval = 10,
        ## base random number generator seed for each model
        ## (automatically gets incremented internally for each model)
        seed = 44
)

grid <- do.call(h2o.grid, grid.pars2)

sortedGrid <- h2o.getGrid("final_grid", sort_by = "auc", decreasing = TRUE)
gbm <- h2o.getModel(sortedGrid@model_ids[[1]])

model <- do.call(h2o.gbm,
                 ## update parameters in place
                 {
                         p <- gbm@parameters
                         ## do not overwrite the original grid model
                         p$model_id = NULL
                         ## use the full dataset
                         p$training_frame = h2o.trainfull
                         ## no validation frame
                         p$validation_frame = NULL
                         ## cross-validation
                         p$nfolds = NULL
                         p
                 }
)

h2o.predicted <- h2o.predict(model, h2o.test) #Prediction based on trained model
dt.predicted <- as.data.table(h2o.predicted) #H2O predict to data.table format
h2o.shutdown(prompt = FALSE) #Shuting down H2O cluster
#Merge testset with prediction
dt.output <- cbind(testset[, "POLICY_ID"], dt.predicted[, c(1, 3)])
setnames(dt.output, c("predict", "TRUE."),
         c("POLICY_IS_RENEWED", "POLICY_IS_RENEWED_PROBABILITY")) #Cols rename

fwrite(dt.output, file = "POLICY_IS_RENEWED.csv") #write result to csv file.
dt.output #Print Result in Accordance to required format.
