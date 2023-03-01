
# packages ----------------------------------------------------------------

pacman::p_load(tidyverse, arrow, here, install = FALSE)



# read data ---------------------------------------------------------------

urgencias_2019 <- read_parquet(here("datos_input", "au_2019.parquet"))

urgencias_2018 <- read_parquet(here("datos_input", "au_2018.parquet"))



# transform data ----------------------------------------------------------

urgencias_2019 <- urgencias_2019 |> 
  janitor::clean_names() |> 
  rename(total_atenciones = col01,
         a_menor_1 = col02,
         a_1_4 = col03,
         a_5_14 = col04,
         a_15_64 = col05,
         a_mas_65 = col06,
         n_semana = semana) |> 
  mutate(fecha = as.Date(x = fecha, tryFormats = "%d/%m/%Y"),
         semana = lubridate::floor_date(fecha, unit = "week", week_start = getOption("lubridate.week.start", 1))) |> 
  relocate(semana, .after = n_semana)


urgencias_2018 <- urgencias_2018 |> 
  janitor::clean_names() |> 
  rename(total_atenciones = col01,
         a_menor_1 = col02,
         a_1_4 = col03,
         a_5_14 = col04,
         a_15_64 = col05,
         a_mas_65 = col06,
         n_semana = semana) |> 
  mutate(fecha = as.Date(x = fecha, tryFormats = "%d/%m/%Y"),
         semana = lubridate::floor_date(fecha, unit = "week", week_start = getOption("lubridate.week.start", 1))) |> 
  relocate(semana, .after = n_semana)


base_2019 <-  expand.grid(
  "n_semana" = 
    urgencias_2019 |> 
      count(n_semana) |> 
      pull(n_semana),
  "glosacausa" =
    urgencias_2019 |> 
      filter(glosacausa %in% c("TOTAL CAUSAS SISTEMA CIRCULATORIO",
                               "Neumonía (J12-J18)",
                               "Otra causa respiratoria (J22, J30-J39, J47, J60-J98)")) |> 
      count(glosacausa) |> 
      pull(glosacausa),
  stringsAsFactors = FALSE,
  KEEP.OUT.ATTRS = FALSE
    ) |> 
  tibble() |> 
  arrange(n_semana, glosacausa)


base_2018 <-  expand.grid(
  "n_semana" = 
    urgencias_2018 |> 
      count(n_semana) |> 
      pull(n_semana),
  "glosacausa" =
    urgencias_2018 |> 
      filter(glosacausa %in% c("TOTAL CAUSAS SISTEMA CIRCULATORIO",
                               "Neumonía (J12-J18)",
                               "Otra causa respiratoria (J22, J30-J39, J47, J60-J98)")) |> 
      count(glosacausa) |> 
      pull(glosacausa),
  stringsAsFactors = FALSE,
  KEEP.OUT.ATTRS = FALSE
    ) |> 
  tibble() |> 
  arrange(n_semana, glosacausa)

# filters -----------------------------------------------------------------
# (servicio salud antofagasta + enfermedades)

urgencias_2019 <- urgencias_2019 |> 
  mutate(
    reg =
      case_when(
        str_detect(idestablecimiento, "-") &
          str_sub(idestablecimiento, end = 2) == "03" ~ 1,
        TRUE ~ 0)
    ) |> 
  filter(reg == 1) |> 
  filter(glosacausa %in% c("TOTAL CAUSAS SISTEMA CIRCULATORIO",
                           "Neumonía (J12-J18)",
                           "Otra causa respiratoria (J22, J30-J39, J47, J60-J98)"))


urgencias_2018 <- urgencias_2018 |> 
  mutate(
    reg =
      case_when(
        str_detect(idestablecimiento, "-") &
          str_sub(idestablecimiento, end = 2) == "03" ~ 1,
        TRUE ~ 0)
    ) |> 
  filter(reg == 1) |> 
  filter(glosacausa %in% c("TOTAL CAUSAS SISTEMA CIRCULATORIO",
                           "Neumonía (J12-J18)",
                           "Otra causa respiratoria (J22, J30-J39, J47, J60-J98)"))

## 2019 (17) tiene más establecimientos que 2018 (13)




# group data --------------------------------------------------------------

datos_semana_2019 <- urgencias_2019 |> 
  group_by(n_semana, glosacausa) |> 
  summarise(n_total = sum(total_atenciones, na.rm = TRUE),
            n_15_64 = sum(a_15_64, na.rm = TRUE)) |> 
  ungroup()


datos_semana_2018 <- urgencias_2018 |> 
  group_by(n_semana, glosacausa) |> 
  summarise(n_total = sum(total_atenciones, na.rm = TRUE),
            n_15_64 = sum(a_15_64, na.rm = TRUE)) |> 
  ungroup()


# join back ---------------------------------------------------------------


serie_tiempo_2019 <- base_2019 |> 
  full_join(datos_semana_2019,
            by = c("n_semana", "glosacausa")) |> 
  mutate(across(.cols = c(n_total, n_15_64),
                .fns = ~ replace_na(data = .x, replace = 0)))
  

serie_tiempo_2018 <- base_2018 |> 
  full_join(datos_semana_2018,
            by = c("n_semana", "glosacausa")) |> 
  mutate(across(.cols = c(n_total, n_15_64),
                .fns = ~ replace_na(data = .x, replace = 0)))



# plots -------------------------------------------------------------------


# 2019
ggplot(serie_tiempo_2019) +
  geom_point(aes(x = n_semana,
                 y = n_15_64,
                 colour = glosacausa)) +
  geom_line(aes(x = n_semana,
                y = n_15_64,
                colour = glosacausa)) +
  facet_wrap(~ glosacausa) +
  theme(legend.position = "bottom")



# 2018
ggplot(serie_tiempo_2018) +
  geom_point(aes(x = n_semana,
                 y = n_15_64,
                 colour = glosacausa)) +
  geom_line(aes(x = n_semana,
                y = n_15_64,
                colour = glosacausa)) +
  facet_wrap(~ glosacausa) +
  theme(legend.position = "bottom")
