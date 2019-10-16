#' Gross primary productivity estimated from tower NEP and chamber soil
#' respiration
#'
#' @param nep Net ecosystem production (positive in growing season). Any units,
#'   but should match `r_chamber`.
#' @param r_chamber Soil chamber respiration. Any units, but should match `nep`.
#' @param phi Belowground fraction of Ra
#' @param f Root fraction of Rsoil
#' @return GPP estimate
#' @author Alexey Shiklomanov
gpp_r <- function(nep, r_chamber, phi, f) {
  r <- r_chamber * (1 + f / phi - f)
  gpp <- nep + r
  return(gpp)
}

#' GPP estimated from net primary productivity and carbon use efficiency
#'
#' @param npp Net primary productivity (any units)
#' @param cue Carbon use efficiency (0-1). Fraction of GPP not lost to
#'   respiration.
#' @return GPP estimate
#' @author Alexey Shiklomanov
gpp_cue <- function(npp, cue) {
  npp / cue
}

#' Draw GPP values for a given NEP and Rsoil observation
#'
#' @param nep Net ecosystem productivity
#' @param r_soil Soil respiration
#' @param n Number of draws
draw_gpp_r <- function(nep, r_soil, n = 2500) {
  tibble(
    # Phi and f distributions
    phi = rnorm(n, 0.56, 0.11),
    f = rnorm(n, 0.5, 0.125),
    gpp = gpp_r(nep, r_soil, phi, f)
  )
}

# NOTE: If we have Rh and Rsoil measurements, we can solve for "f" analytically.
