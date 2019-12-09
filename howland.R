library(tidyverse)
library(cowplot)

source("funs.R")

n <- 10000
nep <- 170
r_soil <- 730

out <- tibble(
  method = "unconstrained",
  Beta = rnorm(n, 0.5, 0.05),
  Rho = rbeta(n, 5, 5),
  gpp = gpp_r(nep, r_soil, Rho, Beta)
)

out2 <- out %>%
  mutate(
    method = "constrained",
    Rho = rnorm(n, 0.56, 0.11),
    gpp = gpp_r(nep, r_soil, Rho, Beta)
  )

# NPP -- 535 (430-560)
# RH -- 365 (300-410)

out3 <- tibble(
  method = "CUE",
  npp = rnorm(n, 535, 30),
  ## rh = runif(n, 300, 410),
  cue = rnorm(n, 0.47, 0.1),
  gpp = gpp_cue(npp, cue)
)

out_all <- bind_rows(out, out2, out3) %>%
  mutate(method = fct_inorder(method))

mainplot <- out_all %>%
  ggplot() +
  aes(x = gpp, color = method) +
  geom_density() +
  geom_vline(xintercept = c(1200, 1450), linetype = "dashed") +
  coord_cartesian(xlim = c(500, 3000)) +
  labs(x = expression("GPP" ~ (gC ~ m^-2 ~ year^-1))) +
  theme_bw()

params <- out_all %>%
  pivot_longer(c(Beta:cue)) %>%
  filter(name %in% c("Beta", "Rho", "npp", "cue"),
         !is.na(value)) %>%
  mutate(name = factor(name, c("Beta", "Rho", "npp", "cue")))

inset <- ggplot(params) +
  aes(x = value, color = method) +
  geom_density() +
  facet_wrap(vars(name), scales = "free") +
  theme_bw() +
  guides(color = FALSE) +
  theme(axis.title = element_blank())

ggdraw() +
  draw_plot(mainplot) +
  draw_plot(inset, x = 0.45, y = 0.6, width = 0.3, height = 0.3,
            scale = 1.3)

ggsave("howland-gpp2.png")
