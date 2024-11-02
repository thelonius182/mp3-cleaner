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
  qry <- "select distinct po.post_date
          from wp_posts po join wp_postmeta pm
                             on pm.post_id = po.ID
          where po.post_type like 'programma%'
            and pm.meta_key = 'pr_metadata_orig'
            and pm.meta_value != ''
          order by 1;"
  wordpress_replay_dates <- dbGetQuery(wp_conn, qry)
  dbDisconnect(wp_conn)

  # clean replay post dates
  bc_fmt <- stamp("20241227_2300", orders = "%Y%Om%d_%H%M", quiet = T)
  wordpress_replay_dates_a <- wordpress_replay_dates |>
    mutate(wordpress_replay_ts = round_date(ymd_hms(post_date), unit = "15 minutes"),
           wordpress_replay_ts_fmt = bc_fmt(wordpress_replay_ts))

  # get replay mp3's from UZM
  uzm_replays <- dir_ls("//UITZENDMAC-2/Avonden/RoD", regexp = "\\.mp3$") |> as_tibble() |> rename(mp3_qfn = value)
  uzm_replays_a <- uzm_replays |> mutate(fn = path_file(mp3_qfn),
                                         mp3_ts = str_extract(mp3_qfn, "\\d{8}_\\d{4}"),
                                         mp3_ts_a = round_date(ymd_hm(mp3_ts), unit = "15 minutes"),
                                         mp3_ts_fmt = bc_fmt(mp3_ts_a))
  # replay mp3's to be deleted
  uzm_replays_rmv <- uzm_replays_a |>
    left_join(wordpress_replay_dates_a, by = join_by(mp3_ts_fmt == wordpress_replay_ts_fmt))

  gh_sess <- ssh_connect("cz@streams.greenhost.nl")
  gh_list_cz <- ssh_exec_internal(session = gh_sess, "ls -la /srv/audio/cz_rod/")
  gh_list_woj <- ssh_exec_internal(session = gh_sess, "ls -la /srv/audio/cz_rod/woj/")
  output_text_cz <- rawToChar(gh_list_cz$stdout) |> str_split_1("\n") |> as_tibble()
  output_text_woj <- rawToChar(gh_list_woj$stdout) |> str_split_1("\n") |> as_tibble()

  gh_mp3s_cz_a <- output_text_cz |> as_tibble() |> rename(gh_line = value) |>
    filter(!str_detect(gh_line, "total")) |>
    mutate(n_bytes = parse_integer(str_extract(gh_line, "\\b[0-9]{5,}\\b")),
           mp3_name = str_extract(gh_line, "20[0-9]{6}_[0-9]{4}\\.mp3$")) |>
    filter(!is.na(mp3_name) & !str_detect(mp3_name, "mp3\\.mp3")) |> select(-gh_line)

  gh_mp3s_woj_a <- output_text_woj |> mutate(n_bytes = parse_integer(str_extract(value, "\\b[0-9]{5,}\\b")),
                                     mp3_name = str_extract(value, "20[0-9]{6}_[0-9]{4}\\.mp3$")) |>
    filter(!is.na(mp3_name) & !str_detect(mp3_name, "mp3\\.mp3")) |> select(-value)

  gh_mp3s_b <- gh_mp3s_a |> mutate(bc_time = str_extract(mp3_name, "([0-9]{4})\\.mp3$", group = 1),
                                   bc_date = str_extract(mp3_name, "([0-9]{8})_[0-9]{4}\\.mp3$", group = 1),
                                   bc_ts_chr = paste0(bc_date, " ", bc_time),
                                   bc_ts = round_date(ymd_hm(bc_ts_chr), unit = "hour"),
                                   bc_ts_fmt = bc_fmt(bc_ts))

  gh_mp3s_c <- gh_mp3s_b |> inner_join(wp_replays, by = join_by(bc_ts_fmt == replay_ts_fmt))
  gh_mp3s_c_tmp <- gh_mp3s_c |> head(3)
  gh_mp3s_c_tmp |>  pull(mp3_name) |> walk(delete_remote_file)

  ssh_disconnect(gh_sess)

  n_gibi_bytes <- sum(gh_mp3s_c$n_bytes, na.rm = T) / 1024 / 1024 / 1024

  # exit main control loop
  break
}
