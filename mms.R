library(tidyverse)
library(fs)

source("funs.R")

dat <- read_csv(path("data", "MMSF", "mmsf_daily.csv")) %>%
  select(doy, r_soil = Flux_gC) %>%
  mutate(
    ts = as.Date("2012-01-01") +
      as.difftime(doy - 1, units = "days")
  ) %>%
  select(ts, r_soil)

## ggplot(dat) +
##   aes(x = ts, y = r_soil) +
##   geom_line()

nirv <- path("data", "MMSF", "mms_daily_flux_nirv.csv") %>%
  read_csv()

# TODO: This is a fake NEE number
both <- dat %>%
  inner_join(nirv, "ts") %>%
  mutate(nee_fake = r_soil * 1.2 + GPP_DT_VUT_REF)

result <- both %>%
  mutate(gpp_df = map2(-NEE_VUT_REF, r_soil, draw_gpp_r)) %>%
  unnest(gpp_df)

plt_data <- result %>%
  group_by(ts) %>%
  summarize(
    lo = quantile(gpp, 0.025),
    mid = mean(gpp),
    hi = quantile(gpp, 0.975),
    gpp = mean(GPP_DT_VUT_REF)
  ) %>%
  complete(ts = full_seq(ts, 1))

plt <- ggplot(plt_data) +
  aes(x = ts, y = mid, ymin = lo, ymax = hi) +
  geom_ribbon(fill = "deepskyblue") +
  geom_line(aes(color = "NEE + Rsoil")) +
  geom_line(aes(y = gpp, color = "Ameriflux")) +
  theme_bw() +
  labs(x = "Date", y = expression(GPP ~ (gC ~ m^-2 ~ day^-1)),
       color = "GPP Method") +
  scale_color_manual(values = c(
    "NEE + Rsoil" = "black",
    "Ameriflux" = "orange"
  )) +
  theme(legend.position = c(0.95, 0.95),
        legend.justification = c(1, 1))
plt
ggsave("figures/mms-ts.png", plt, width = 6, height = 3)

plt2 <- ggplot(plt_data) +
  aes(x = ts, y = mid - gpp, ymin = lo - gpp, ymax = hi - gpp) +
  geom_hline(yintercept = 0, linetype = "dashed") +
  geom_ribbon(fill = "deepskyblue") +
  geom_line(size = 0.2) +
  labs(
    x = "Date",
    y = expression(GPP[finality] - GPP[Ameriflux] ~ (gC ~ m^-2 ~ day^-1))
  ) +
  theme_bw()
print(plt2)
ggsave("figures/mms-ts-error.png", plt2, width = 6, height = 3)
