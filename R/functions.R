
# TO VALIDATE CONN:
# sqlstmt <- "show variables like 'character_set_client'"
# result <- dbGetQuery(conn = wp_conn, statement = sqlstmt)
get_wp_conn <- function() {


  db_port <- 3306
  # flog.appender(appender.file("/Users/nipper/Logs/nipper.log"), name = "nipperlog")

  grh_conn <- tryCatch(
    {
      dbConnect(drv = MySQL(), user = db_user, password = db_password,
                dbname = db_name, host = db_host, port = db_port)
    },
    error = function(cond) {
      return("connection-error")
    }
  )

  return(grh_conn)
}

salsa_git_version <- function(qfn_repo) {

  repo <- git2r::repository(qfn_repo)
  branch <- git2r::repository_head(repo)$name
  latest_commit <- git2r::commits(repo, n = 1)[[1]]
  commit_author <- latest_commit$author$name
  commit_date <- latest_commit$author$when
  fmt_commit_date <- format(lubridate::with_tz(commit_date, tzone = "Europe/Amsterdam"), "%a %Y-%m-%d, %H:%M")

  return(list(git_branch = branch, ts = fmt_commit_date, by = commit_author, path = repo$path))
}

delete_remote_file <- function(file) {
  command <- sprintf("rm -f '%s'", file)
  ssh_exec_wait(gh_sess, command)
  flog.info(sprintf("Deleted: %s", path_file(file)), name = config$log_slug)
}
