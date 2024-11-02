message(sprintf("using .Rprofile in %s", getwd()))

get_git_branch <- function() {
  system("git rev-parse --abbrev-ref HEAD", intern = TRUE)
}

set_config <- function() {

  if (get_git_branch() == "main") {
    source("src/config_prd.R")
  } else {
    source("src/config_dev.R")
  }
}
