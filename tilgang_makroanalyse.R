
library(DBI)
library(odbc)
#con <- DBI::dbConnect(odbc::odbc(), "Oracle DB")

con <- dbConnect(odbc::odbc(), 
                 DBQ = "dm09-scan.adeo.no:1521/dwh_ha",
                 Driver = "{/usr/lib/oracle/19.9/client64/lib/libsqora.so.19.1}",
                 UID = "W158886",
                 PWD = rstudioapi::askForPassword("Passord:"))

#Stilling makro
library(dplyr)
library(data.table)
library(ggplot2)
library(plotly)


query_new = "select sum(tilgang_still_denne_mnd) tilgang, stilling_kilde_id, bransje_grovgruppetekst,bransje_fingruppetekst,
fylkesnavn, aarmnd 
from DATAMART2.arb_agg_stilling_mnd_ainfo stilling
    left join (select distinct styrk_id, bransje_grovgruppetekst, bransje_fingruppetekst, yrkeskode
    from dimensjoner2.dim_styrk where isco_versjon = 'ISCO-08') yrkes_klass
    on stilling.stilling_styrk_id = yrkes_klass.styrk_id
    left join (select geografi_id,fylkesnavn from dimensjoner2.dim_geografi) geo_klass
    on stilling.arbeidssted_geografi_id = geo_klass.geografi_id
where aarmnd >= 202005
group by stilling_kilde_id, bransje_grovgruppetekst,bransje_fingruppetekst, fylkesnavn, aarmnd
order by aarmnd desc"

df_new =  setDT(DBI::dbGetQuery(con, query_new))

query_old = "select sum(tilgang_still_denne_mnd) tilgang, stilling_kilde_id, bransje_grovgruppetekst,bransje_fingruppetekst,
fylkesnavn, aarmnd 
from DATAMART2.agg_fak_stilling_mnd_actuate stilling
    left join (select distinct styrk_id, bransje_grovgruppetekst, bransje_fingruppetekst, yrkeskode
    from dimensjoner2.dim_styrk where isco_versjon = 'ISCO-08') yrkes_klass
    on stilling.stilling_styrk_id = yrkes_klass.styrk_id
    left join (select geografi_id,fylkesnavn from dimensjoner2.dim_geografi) geo_klass
    on stilling.arbeidssted_geografi_id = geo_klass.geografi_id
where aarmnd >= 202005
group by stilling_kilde_id, bransje_grovgruppetekst,bransje_fingruppetekst, fylkesnavn, aarmnd
order by aarmnd desc"
df_old = setDT(DBI::dbGetQuery(con, query_old))


setnames(df_new,tolower(names(df_new)))
setnames(df_old,tolower(names(df_old)))

#litt generelle tall

plyr::count(df_new[tilgang>0, fylkesnavn])
plyr::count(df_old[tilgang>0, fylkesnavn])

df_new[,versjon := "kafka"]
df_old[,versjon := "ainfo"]

tilgang_stilling <- rbind(df_new, df_old)
setnames(tilgang_stilling, c('bransje_grovgruppetekst'), c('bransje_grov'))

tilgang_stilling[,fylkesnavn := stringi::stri_replace_all_regex(fylkesnavn,"\\?", "ø")]

saveRDS(tilgang_stilling, file = "tilgang_stilling.rds")
#---------------------------------------------------------------------------------------------------
# start her om ikke dataene er endret!
#---------------------------------------------------------------------------------------------------

tilgang_stilling = readRDS("tilgang_stilling.rds")

#test <- tilgang_stilling[, sum(tilgang), by = .(aarmnd, versjon)]

tilgang_mnd <-  tilgang_stilling %>% group_by(aarmnd,versjon) %>% summarise(tilgang_sum = sum(tilgang)) %>% setDT()
mnd_plot <- ggplot(tilgang_mnd, aes(x = aarmnd, y = tilgang_sum, fill = versjon)) + 
  geom_bar(stat = "identity", position = "dodge")
ggplotly(mnd_plot)

tilgang_bransje <-  tilgang_stilling %>% group_by(bransje_grov,versjon) %>% summarise(tilgang_sum = sum(tilgang)) %>% setDT()
bransje_plot <- ggplot(tilgang_bransje, aes(x = bransje_grov, y = tilgang_sum, fill = versjon)) + geom_bar(stat = "identity", position = "dodge")
ggplotly(bransje_plot)

tilgang_fylke <-  tilgang_stilling %>% group_by(fylkesnavn,versjon) %>% summarise(tilgang_sum = sum(tilgang)) %>% setDT()
tilgang_fylke <- tilgang_fylke[tilgang_sum != 0]
fylke_plot <- ggplot(tilgang_fylke, aes(x = fylkesnavn, y = tilgang_sum, fill = versjon)) + geom_bar(stat = "identity", position = "dodge")
ggplotly(fylke_plot)

tilgang_kilde <-  tilgang_stilling %>% group_by(stilling_kilde_id,versjon) %>% summarise(tilgang_sum = sum(tilgang)) %>% setDT()
kilde_plot <- ggplot(tilgang_kilde, aes(x = stilling_kilde_id, y = tilgang_sum, fill = versjon)) + geom_bar(stat = "identity", position = "dodge")
ggplotly(kilde_plot)

tilgang_mnd_bransje <-  tilgang_stilling %>% group_by(aarmnd,bransje_grov,versjon) %>% summarise(tilgang_sum = sum(tilgang)) %>% setDT()
bransje_mnd_plot <- ggplot(tilgang_mnd_bransje, aes(x = bransje_grov, y = tilgang_sum, fill = versjon)) + 
  geom_bar(stat = "identity", position = "dodge") +
  facet_wrap(vars(aarmnd))
ggplotly(bransje_mnd_plot)


tilgang_stilling %>% filter(bransje_grovgruppetekst == "Ingeni?r- og ikt-fag" & aarmnd == 202009) %>%
  group_by(versjon) %>% summarise(sum(tilgang))

# #setDT(df_new)
# setnames(df_new,tolower(names(df_new)) )
# df_new[, tilgang_new_month := sum(tilgang), by = aarmnd]
# df_new[, tilgang_new_yrke := sum(tilgang), by = bransje_grovgruppetekst]
# df_new[, tilgang_new_fylke := sum(tilgang), by = fylkesnavn]
# head(df_new)
# 
# setDT(df_old)
# 
# df_old[, tilgang_old_month := sum(tilgang), by = aarmnd]
# df_old[, tilgang_old_yrke := sum(tilgang), by = bransje_grovgruppetekst]
# df_old[, tilgang_old_fylke := sum(tilgang), by = fylkesnavn]
# head(df_old)
# 
# test <- merge(df_old, df_new, by = c("aarmnd", "fylkesnavn", "bransje_grovgruppetekst"), all = TRUE)

ggplot(test, aes(x = aarmnd, y = ))
#df_mnd_ny <- df_new %>% group_by(AARMND) %>% summarise(tilgang_new = sum(TILGANG)) %>% setDT()

# 
# df_mnd_old = df_old.groupby(['AARMND'])['TILGANG'].sum().reset_index(name='tilgang_old')
# df_yrke_old = df_old.groupby(['BRANSJE_GROVGRUPPETEKST'])['TILGANG'].sum().reset_index(name='tilgang_old')
# df_fylk_old = df_old.groupby(['FYLKESNAVN'])['TILGANG'].sum().reset_index(name='tilgang_old')
# 
# df_mnd_ny1 = df_new1.groupby(['AARMND'])['TILGANG'].sum().reset_index(name='tilgang_new1')
# df_yrke_ny1 = df_new1.groupby(['BRANSJE_GROVGRUPPETEKST'])['TILGANG'].sum().reset_index(name='tilgang_new1')
# df_fylk_ny1 = df_new1.groupby(['FYLKESNAVN'])['TILGANG'].sum().reset_index(name='tilgang_new1')

df_mnd = df_mnd_old.merge(df_mnd_ny1, on = 'AARMND')
df_yrke = df_yrke_old.merge(df_yrke_ny1, on = 'BRANSJE_GROVGRUPPETEKST')
df_fylk = df_fylk_old.merge(df_fylk_ny1, on = 'FYLKESNAVN')
df_fylk