# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -
# Remove .mp3 replay files from UZM/RoD and greenhost. HiJack unnecessarily records replays.
# - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - - -

conflicts_prefer(dplyr::lag, dplyr::lead, dplyr::filter, lubridate::minutes, .quiet = T)

# enter main control loop
repeat {

  # connect to wordpress-DB ----
  wp_conn <- get_wp_conn()

  if (typeof(wp_conn) != "S4") {
    flog.error(sprintf("connecting to wordpress-DB (%s) failed", config$wpdb_env), name = config$log_slug)
    break
  }

  # get replay post dates ----
  qry <- "select distinct po.post_date, po.post_type
          from wp_posts po join wp_postmeta pm
                             on pm.post_id = po.ID
          where po.post_type like 'programma%'
            and pm.meta_key = 'pr_metadata_orig'
            and pm.meta_value != '';"
  wordpress_replay_dates <- dbGetQuery(wp_conn, qry)
  dbDisconnect(wp_conn)

  # clean replay post dates
  bc_fmt <- stamp("20241227_2300", orders = "%Y%Om%d_%H%M", quiet = T)
  wordpress_replay_dates_cz <- wordpress_replay_dates |> filter(post_type == "programma") |>
    mutate(wordpress_replay_ts = round_date(ymd_hms(post_date), unit = "15 minutes"),
           wordpress_replay_ts_fmt = bc_fmt(wordpress_replay_ts))

  wordpress_replay_dates_woj <- wordpress_replay_dates |> filter(post_type == "programma_woj") |>
    mutate(wordpress_replay_ts = round_date(ymd_hms(post_date), unit = "15 minutes"),
           wordpress_replay_ts_fmt = bc_fmt(wordpress_replay_ts))

  # get replay mp3's from UZM
  uzm_replays_cz <- dir_ls("//UITZENDMAC-2/Avonden/RoD", recurse = F, regexp = "\\.mp3$") |>
    as_tibble() |> rename(mp3_qfn = value)

  uzm_replays_woj <- dir_ls("//UITZENDMAC-2/Avonden/RoD/woj/", recurse = F, regexp = "\\.mp3$") |>
    as_tibble() |> rename(mp3_qfn = value)

  # clean replay mp3's from UZM
  uzm_replays_cz_a <- uzm_replays_cz |> mutate(fn = path_file(mp3_qfn),
                                         mp3_ts = str_extract(mp3_qfn, "\\d{8}[_-]\\d{4}"),
                                         mp3_ts_a = round_date(ymd_hm(mp3_ts), unit = "15 minutes"),
                                         mp3_ts_fmt = bc_fmt(mp3_ts_a)) |> select(mp3_qfn, mp3_ts_fmt) |> distinct()

  uzm_replays_woj_a <- uzm_replays_woj |> mutate(fn = path_file(mp3_qfn),
                                         mp3_ts = str_extract(mp3_qfn, "\\d{8}[_-]\\d{4}"),
                                         mp3_ts_a = round_date(ymd_hm(mp3_ts), unit = "15 minutes"),
                                         mp3_ts_fmt = bc_fmt(mp3_ts_a)) |> select(mp3_qfn, mp3_ts_fmt) |> distinct()

  # delete replay mp3's
  uzm_replays_rmv_cz <- uzm_replays_cz_a |>
    inner_join(wordpress_replay_dates_cz, by = join_by(mp3_ts_fmt == wordpress_replay_ts_fmt))

  file_delete(uzm_replays_rmv_cz$mp3_qfn)

  uzm_replays_rmv_woj <- uzm_replays_woj_a |>
    inner_join(wordpress_replay_dates_woj, by = join_by(mp3_ts_fmt == wordpress_replay_ts_fmt))

  file_delete(uzm_replays_rmv_cz$mp3_qfn)

  # get replay mp3's from Greenhost
  gh_sess <- ssh_connect("cz@streams.greenhost.nl")
  gh_list_cz <- ssh_exec_internal(session = gh_sess, "ls -la /srv/audio/cz_rod/")
  gh_list_woj <- ssh_exec_internal(session = gh_sess, "ls -la /srv/audio/cz_rod/woj/")
  output_text_cz <- rawToChar(gh_list_cz$stdout) |> str_split_1("\n") |> as_tibble()
  output_text_woj <- rawToChar(gh_list_woj$stdout) |> str_split_1("\n") |> as_tibble()

  gh_mp3s_cz_a <- output_text_cz |> as_tibble() |> rename(gh_line = value) |>
    filter(!str_detect(gh_line, "total")) |>
    mutate(n_bytes = parse_integer(str_extract(gh_line, "\\b[0-9]{5,}\\b")),
           mp3_name = str_extract(gh_line, "20[0-9]{6}[-_][0-9]{4}\\.mp3$")) |>
    filter(!is.na(mp3_name) & !str_detect(mp3_name, "mp3\\.mp3")) |> select(-gh_line)

  gh_mp3s_woj_a <- output_text_woj |> as_tibble() |> rename(gh_line = value) |>
    filter(!str_detect(gh_line, "total")) |>
    mutate(n_bytes = parse_integer(str_extract(gh_line, "\\b[0-9]{5,}\\b")),
           mp3_name = str_extract(gh_line, "20[0-9]{6}[-_][0-9]{4}\\.mp3$")) |>
    filter(!is.na(mp3_name) & !str_detect(mp3_name, "mp3\\.mp3")) |> select(-gh_line)

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

  gh_mp3s_cz_rmv <- gh_mp3s_cz_b |>
    inner_join(wordpress_replay_dates_cz, by = join_by(bc_ts_fmt == wordpress_replay_ts_fmt))

  gh_mp3s_cz_rmv_tmp <- gh_mp3s_cz_rmv |> head(3)
  gh_mp3s_cz_rmv_tmp |>  pull(mp3_name) |> walk(delete_remote_file)

  gh_mp3s_woj_rmv <- gh_mp3s_woj_b |>
    inner_join(wordpress_replay_dates_woj, by = join_by(bc_ts_fmt == wordpress_replay_ts_fmt))

  gh_mp3s_woj_rmv |>  pull(mp3_name) |> walk(delete_remote_file)

  ssh_disconnect(gh_sess)

  n_gibi_bytes_cz <- sum(gh_mp3s_cz_c$n_bytes, na.rm = T) / 1024 / 1024 / 1024
  sprintf("deleted %s GB from greenhost/rod", round(n_gibi_bytes_cz, 1))
  n_gibi_bytes_woj <- sum(gh_mp3s_woj_c$n_bytes, na.rm = T) / 1024 / 1024 / 1024
  sprintf("deleted %s GB from greenhost/rod/woj", round(n_gibi_bytes_woj, 1))

  # exit main control loop
  break
}
