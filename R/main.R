# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Remove .mp3 replay files from UZM/RoD and greenhost. HiJack unnecessarily records replays.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

conflicts_prefer(dplyr::lag, dplyr::lead, dplyr::filter, lubridate::minutes, .quiet = T)

# enter main control loop
repeat {

  # connect to wordpress-DB
  wp_conn <- get_wp_conn()

  if (typeof(wp_conn) != "S4") {
    flog.error(sprintf("connecting to wordpress-DB (%s) failed", config$wpdb_env), name = config$log_slug)
    break
  }

  # get WordPress replay post dates
  qry <- "select distinct po.post_date, po.post_type
          from wp_posts po join wp_postmeta pm
                             on pm.post_id = po.ID
          where po.post_type like 'programma%'
            and pm.meta_key = 'pr_metadata_orig'
            and pm.meta_value != '';"
  wordpress_replay_dates <- dbGetQuery(wp_conn, qry)
  dbDisconnect(wp_conn)

  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  # clean WordPress replay post dates (bc: broadcast) and prepare for matching
  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  bc_fmt <- stamp("20241227_2300", orders = "%Y%Om%d_%H%M", quiet = T)

  # - for CZ
  wordpress_replay_dates_cz <- wordpress_replay_dates |> filter(post_type == "programma") |>
    mutate(wordpress_replay_ts = round_date(ymd_hms(post_date), unit = "15 minutes"),
           wordpress_replay_ts_fmt = bc_fmt(wordpress_replay_ts))
  log_count(tib = wordpress_replay_dates_cz, cmt = "WordPress replays CZ.")

  # - for WoJ
  wordpress_replay_dates_woj <- wordpress_replay_dates |> filter(post_type == "programma_woj") |>
    mutate(wordpress_replay_ts = round_date(ymd_hms(post_date), unit = "15 minutes"),
           wordpress_replay_ts_fmt = bc_fmt(wordpress_replay_ts))
  log_count(tib = wordpress_replay_dates_woj, cmt = "WordPress replays WoJ.")

  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  # RoD-split folders are fed from HiJack. RoD folder is fed from RoD-split folders by FolderSync.
  # To prevent mp3's from re-appearing in RoD, they first need to be deleted from RoD-split.
  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

  # define function needed to check deletion result
  safe_delete <- possibly(file_delete, otherwise = NULL)

  delete_mp3s_uzm("//UITZENDMAC-2/Avonden/RoD split/RoD split ana")
  delete_mp3s_uzm("//UITZENDMAC-2/Avonden/RoD split/RoD split dig")
  delete_mp3s_uzm("//UITZENDMAC-2/Avonden/RoD")
  delete_mp3s_uzm("//UITZENDMAC-2/Avonden/RoD/woj")

  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  # get all mp3's from Greenhost + clean
  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  gh_sess <- ssh_connect("cz@streams.greenhost.nl")

  gh_list_cz  <- ssh_exec_internal(session = gh_sess, "ls -la /srv/audio/cz_rod/")
  gh_list_woj <- ssh_exec_internal(session = gh_sess, "ls -la /srv/audio/cz_rod/woj/")

  output_text_cz  <- rawToChar(gh_list_cz$stdout)  |> str_split_1("\n") |> as_tibble()
  output_text_woj <- rawToChar(gh_list_woj$stdout) |> str_split_1("\n") |> as_tibble()

  gh_mp3s_cz_a <- output_text_cz |> as_tibble() |> rename(gh_line = value) |>
    filter(!str_detect(gh_line, "total")) |>
    mutate(n_bytes = parse_integer(str_extract(gh_line, "\\b[0-9]{5,}\\b")),
           mp3_name = str_extract(gh_line, "20[0-9]{6}[-_][0-9]{4}\\.mp3$")) |>
    filter(!is.na(mp3_name) & !str_detect(mp3_name, "mp3\\.mp3")) |> select(-gh_line)
  log_count(tib = gh_mp3s_cz_a, cmt = "mp3's total in RoD on Greenhost.")

  gh_mp3s_woj_a <- output_text_woj |> as_tibble() |> rename(gh_line = value) |>
    filter(!str_detect(gh_line, "total")) |>
    mutate(n_bytes = parse_integer(str_extract(gh_line, "\\b[0-9]{5,}\\b")),
           mp3_name = str_extract(gh_line, "20[0-9]{6}[-_][0-9]{4}\\.mp3$")) |>
    filter(!is.na(mp3_name) & !str_detect(mp3_name, "mp3\\.mp3")) |> select(-gh_line)
  log_count(tib = gh_mp3s_woj_a, cmt = "mp3's total in RoD/woj on Greenhost.")

  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  # prepare Greenhost mp3's for matching to WP-posts
  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  gh_mp3s_cz_b <- gh_mp3s_cz_a |> mutate(bc_time = str_extract(mp3_name, "([0-9]{4})\\.mp3$", group = 1),
                                         bc_date = str_extract(mp3_name, "([0-9]{8})[-_][0-9]{4}\\.mp3$", group = 1),
                                         bc_ts_chr = paste0(bc_date, " ", bc_time),
                                         bc_ts = round_date(ymd_hm(bc_ts_chr), unit = "hour"),
                                         bc_ts_fmt = bc_fmt(bc_ts),
                                         mp3_name = path("/srv/audio/cz_rod", mp3_name))

  gh_mp3s_woj_b <- gh_mp3s_woj_a |> mutate(bc_time = str_extract(mp3_name, "([0-9]{4})\\.mp3$", group = 1),
                                           bc_date = str_extract(mp3_name, "([0-9]{8})[-_][0-9]{4}\\.mp3$", group = 1),
                                           bc_ts_chr = paste0(bc_date, " ", bc_time),
                                           bc_ts = round_date(ymd_hm(bc_ts_chr), unit = "hour"),
                                           bc_ts_fmt = bc_fmt(bc_ts),
                                           mp3_name = path("/srv/audio/cz_rod/woj", mp3_name))

  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  # match and delete Greenhost replay-mp3's
  # - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
  # - for CZ
  gh_mp3s_cz_rmv <- gh_mp3s_cz_b |>
    inner_join(wordpress_replay_dates_cz, by = join_by(bc_ts_fmt == wordpress_replay_ts_fmt))

  if (nrow(gh_mp3s_cz_rmv) > 0) {

    flog.info("start deleting replay-mp3's from RoD on Greenhost", name = config$log_slug)
    gh_mp3s_cz_rmv |> pull(mp3_name) |> walk(delete_remote_file)
    log_count(tib = gh_mp3s_cz_rmv, cmt = "replay-mp3's deleted from RoD on Greenhost.")

  } else {

    flog.info("no replay-mp3's found for RoD on Greenhost", name = config$log_slug)
  }

  # - for WoJ
  gh_mp3s_woj_rmv <- gh_mp3s_woj_b |>
    inner_join(wordpress_replay_dates_woj, by = join_by(bc_ts_fmt == wordpress_replay_ts_fmt))

  if (nrow(gh_mp3s_woj_rmv) > 0) {

    flog.info("start deleting replay-mp3's from RoD/Woj on Greenhost", name = config$log_slug)
    gh_mp3s_woj_rmv |>  pull(mp3_name) |> walk(delete_remote_file)
    log_count(tib = gh_mp3s_woj_rmv, cmt = "replay-mp3's deleted from RoD/woj on Greenhost.")

  } else {

    flog.info("no replay-mp3's found for RoD/woj on Greenhost", name = config$log_slug)
  }

  ssh_disconnect(gh_sess)

  n_gibi_bytes_cz <- sum(gh_mp3s_cz_rmv$n_bytes, na.rm = T) / 1024 / 1024 / 1024
  flog.info(sprintf("deleted %s GB from greenhost/rod", round(n_gibi_bytes_cz, 1)), name = config$log_slug)

  n_gibi_bytes_woj <- sum(gh_mp3s_woj_rmv$n_bytes, na.rm = T) / 1024 / 1024 / 1024
  flog.info(sprintf("deleted %s GB from greenhost/rod/woj", round(n_gibi_bytes_woj, 1)), name = config$log_slug)

  flog.info("mp3 clean-up finished", name = config$log_slug)

  # exit main control loop
  break
}
