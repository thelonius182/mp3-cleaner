
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

log_count <- function(tib, cmt) {
  count_fmt <- sprintf("%6s", format(nrow(tib), big.mark = ".", decimal.mark = ",", scientific = FALSE))
  flog.info(sprintf("%s %s", count_fmt, cmt), name = config$log_slug)
}

delete_mp3s_uzm <- function(uzm_dir) {
  browser()
  flog.info(sprintf("deleting from %s", uzm_dir), name = config$log_slug)

  uzm_tib <- dir_ls(uzm_dir, recurse = F, regexp = "\\.mp3$") |> as_tibble() |> rename(mp3_qfn = value)
  log_count(tib = uzm_tib, cmt = "mp3's total")

  # prepare for matching to WP-posts
  uzm_tib_a <- uzm_tib |> mutate(fn = path_file(mp3_qfn),
                                 mp3_ts = str_extract(mp3_qfn, "\\d{8}[_-]\\d{4}"),
                                 mp3_ts_a = round_date(ymd_hm(mp3_ts), unit = "15 minutes"),
                                 mp3_ts_fmt = bc_fmt(mp3_ts_a)) |> select(mp3_qfn, mp3_ts_fmt) |> distinct()

  uzm_tib_rmv <- uzm_tib_a |> inner_join(wordpress_replay_dates_cz,
                                         by = join_by(mp3_ts_fmt == wordpress_replay_ts_fmt))
  log_count(tib = uzm_tib_rmv, cmt = "replay-mp3's found. Trying to delete ...")

  uzm_tib_rmv_result <- uzm_tib_rmv |> mutate(deletion_status = map(mp3_qfn, safe_delete),
                                              delete_failed = map_lgl(deletion_status, is.null))
  log_count(tib = uzm_tib_rmv_result |> filter(delete_failed), cmt = "failed")

}
