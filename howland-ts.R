library(tidyverse)

source("funs.R")

dat <- read_csv("data/Ho1-flux.csv") %>%
  mutate(
    ts = as.Date(as.character(TIMESTAMP), format = "%Y%m%d"),
    Rsoil = if_else(Rsoil > 0, Rsoil, NA_real_),
    Rh = if_else(Rh > 0, Rh, NA_real_)
  ) %>%
  filter(!is.na(Rsoil)) %>%
  select(ts, NEE, Rsoil, Rh)

# Net ecosystem productivity
## nep <- c(seq(30, 200, length.out = 80),
##          seq(200, 30, length.out = 80))
## nep <- rnorm(length(nep), nep, 5)

# Soil respiration
## r_soil <- c(seq(400, 800, length.out = 80),
##             seq(800, 400, length.out = 80))
## r_soil <- rnorm(length(r_soil), r_soil, 5)

## tseries <- tibble(
##   date = seq_along(nep),
##   nep = nep,
##   r_soil = r_soil
## )

result <- dat %>%
  mutate(gpp_df = map2(-NEE, Rsoil, draw_gpp_r)) %>%
  unnest(gpp_df)

plt <- result %>%
  group_by(ts) %>%
  summarize(
    lo = quantile(gpp, 0.025),
    mid = mean(gpp),
    hi = quantile(gpp, 0.975)
  ) %>%
  complete(ts = full_seq(ts, 1)) %>%
  ggplot() +
  aes(x = ts, y = mid, ymin = lo, ymax = hi) +
  geom_ribbon(fill = "deepskyblue") +
  geom_line(size = 0.1) +
  theme_bw() +
  labs(
    x = "Date",
    y = expression("GPP" ~ (gC ~ m^-2 ~ year^-1))
  )

ggsave("figures/howland-ts.png", plt, width = 5.8, height = 4)
