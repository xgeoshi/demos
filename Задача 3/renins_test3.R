# Задача: проанализировать клиентов в выборке, провести кластеризацию,
# сделать описание сегментов

require(readxl) # Пакет для чтения excel-файлов.
require(data.table) # Пакет для работы с таблицами в R.

# Read excel file and convert to data.table
path <- "Задача.xlsx" # File path.
dt.xls <- as.data.table(readxl::read_xlsx(path))

#Normalizing data features.
data.nrmlzd <- scale(dt.xls[, 2:5])

# Set the row names of data.nrmlzd
rownames(data.nrmlzd) <- dt.xls$Персона

#Determine number of cluster by looping kmeans with cluter setting from 1 to 10.
set.seed(5) #for reproducability
wss.len <- 10L #Set length of the loop.
wss <- integer(wss.len) #Create integer vector (don't grow a vector for mem eff)

for (i in seq(wss.len)) {
        km.i <- kmeans(data.nrmlzd, centers = i, iter.max = 50, nstart = 20)
        # Save total within sum of squares to wss variable
        wss[i] <- km.i$tot.withinss
}

#Scree plot
# plot(x = seq(wss.len), y = wss, type = "b",
#      xlab = "Number of Clusters",
#      ylab = "Within groups sum of squares")

# PCA analysis on normalized data features.
pca.nrmlzd <- prcomp(data.nrmlzd)
# biplot(pca.nrmlzd)

# print(ggbiplot(pca.nrmlzd, obs.scale = 0, var.scale = 0))

#H-clustering of pca data.
pca.hclust <- hclust(dist(pca.nrmlzd$x))

#Cut h-clust tree at 4 clusters.
clust4 <- cutree(pca.hclust, k = 4)

# plot(dt.xls[, c("Уровень заработной платы, руб/год", "Убыточность, %")],
#      # col = km3$cluster, main = "k-means with 3 clusters", xlab = "", ylab = "")
#      col = clust4, main = "k-means with kmeans 4", xlab = "", ylab = "")

report <- cbind(dt.xls, clust4)
report.split <- split(report[, c(-1, -6)], clust4)
report.ranges <- lapply(report.split, apply, 2, range)
res.cols <- c("Мин", "Макс")
ranges.trans <- lapply(report.ranges, t)

for (i in seq_along(ranges.trans)) {
        colnames(ranges.trans[[i]]) <- res.cols
}
names(ranges.trans) <- paste("Кластер", names(ranges.trans))
cat("\014")
ranges.trans
