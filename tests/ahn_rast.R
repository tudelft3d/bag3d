library(ggplot2)
library(tidyr)
diffs <- read.csv("/home/balazs/Data/bag3d_test/quality/ahn_rast.csv", stringsAsFactors = F)
diffs$ahn_version <- as.factor(diffs$ahn_version)
levels(diffs$ahn_version) <- c("AHN2", "AHN3")
names(diffs) <- c("gid", "ahn_version", "tile_id", "00", "10", "25", "50", "75", "90", "95", "99")
clean <- tidyr::gather(diffs, key = "percentile", value = "difference", 4:11,
                       factor_key = TRUE)

ggplot(clean, aes(x=percentile, y=difference, facet)) + 
  stat_summary(fun.data="mean_cl_boot", conf.int=.99, geom = "pointrange", size=1) +
  facet_grid(. ~ ahn_version) + 
  labs(title = "Mean difference between AHN raster and 3dfier output",
       subtitle = paste("AHN 0.5m raster, sample size", nrow(diffs), "bootstrapped mean"),
       x = "Building roof percentile",
       y = "Mean difference [m]") +
  scale_y_continuous(breaks = c(-1, 0, 1, 2, 5, 10))

ggplot(clean, aes(x=percentile, y=difference, facet)) + 
  stat_summary(fun.y="median", geom = "point", size = 6, shape = "—") +
  stat_summary(fun.y="median", geom = "point", size = 2, colour = "red") +
  facet_grid(. ~ ahn_version) + 
  labs(title = "Median difference between AHN raster and 3dfier output",
       subtitle = paste("AHN 0.5m raster, random sample of", nrow(diffs), "buildings"),
       x = "Building height percentile",
       y = "Median difference") +
  scale_y_continuous(breaks = c(-1, 0, 1, 2, 5, 10))

sqrt(sum(diffs$`00`^2, na.rm = TRUE) / nrow(diffs))
