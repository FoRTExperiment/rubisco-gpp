gpp_r <- function(nee, r_chamber, phi, f) {
  r <- r_chamber * (1 + f / phi - f)
  gpp <- nee + r
  return(gpp)
}

gpp_cue <- function(npp, cue) {
  npp / cue
}

draw_gpp_r <- function(nep, r_soil, n = 2500) {
  tibble(
    phi = rnorm(n, 0.56, 0.11),
    f = rnorm(n, 0.5, 0.125),
    gpp = gpp_r(nep, r_soil, phi, f)
  )
}

# NOTE: If we have Rh and Rsoil measurements, we can solve for "f" analytically.
