message(sprintf("using .Rprofile in %s", getwd()))

get_git_branch <- function() {
  system("git rev-parse --abbrev-ref HEAD", intern = TRUE)
}

set_config <- function() {

  if (get_git_branch() == "main") {
    source("R/config_prd.R")
  } else {
    source("R/config_dev.R")
  }
}

check_git_branch <- function() {

  if (get_git_branch() == "main") {
    message(">>>\n>>>    WARNING: you are on the MAIN BRANCH, do not manually edit anything.\n>>>\n")
  }
}

set_config()
check_git_branch()
