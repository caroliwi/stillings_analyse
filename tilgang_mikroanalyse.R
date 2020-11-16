# def setAccessDB(con, schema = 'DATAMART2', role = 'RO', justification = 'Analyse', ndays = 7):
#     db_command = f"""begin
#       ssr_mtn.set_access_role_for_me( p_access_role   => 'DVH_SCH_{schema}_{role}_ROLE'
#                                 , p_justification => '{justification}'
#                                 , p_valid_from_date    => sysdate
#                                 , p_valid_through_date => sysdate + {ndays});
#     end;
#     
#     """
#     #print(db_command)
#     cur = con.cursor()
#     cur.execute(db_command)
#     cur.execute('set role all')

library(DBI)
library(odbc)
library(data.table)

con <- dbConnect(odbc::odbc(), 
                 DBQ = "dm09-scan.adeo.no:1521/dwh_ha",
                 Driver = "{/usr/lib/oracle/19.9/client64/lib/libsqora.so.19.1}",
                 UID = "W158886",
                 PWD = rstudioapi::askForPassword("Passord:"))


query_new = "select still_mnd.aarmnd, still_mnd.stilling_id as ainfo_stilling_id, still_mnd.tilgang_still_denne_mnd as ainfo_tilgang,  
    id_lookup.uuid,
    still_fak.arbsted_geografi_id as ainfo_geo_id, geo_klass.fylkesnavn as ainfo_fylke,
    still_fak.styrk_id as ainfo_styrk_id, yrkesklass.bransje_grovgruppetekst as ainfo_bransje_grov, yrkesklass.bransje_fingruppetekst as ainfo_branje_fin,
    still_fak.stilling_kilde as ainfo_kilde
from forkammer2.fak_stilling_mnd still_mnd
    left join forkammer2.fak_stilling still_fak
    on still_mnd.stilling_id = still_fak.stilling_id
    left join dimensjoner2.dim_styrk yrkesklass
    on still_fak.styrk_id = yrkesklass.styrk_id
    left join dimensjoner2.dim_geografi geo_klass
    on still_fak.arbsted_geografi_id = geo_klass.geografi_id
    left join (SELECT kafka.uuid, arena.stilling_id as arena_stilling_id, arena.mod_dato as arena_mod_dato FROM 
        FORKAMMER2.stilling_id_kafka_drp202012 kafka, FORKAMMER2.pam_stilling_id_drp202012 arena
        where kafka.stilling_id = arena.pam_stilling_id) id_lookup
    on still_mnd.stilling_id = id_lookup.arena_stilling_id
where aarmnd = 202009"
df_ainfo = setDT(DBI::dbGetQuery(con, query_new))
setnames(df_ainfo,tolower(names(df_ainfo)))
df_ainfo <- unique(df_ainfo)

query_s = "select s_data.uuid, s_data.status, s_data.stillinger, s_data.stillinger_stat, s_data.kafka_timestamp, s_data.expires, s_data.updated, s_data.created, s_data.published_by_admin, s_data.published,
    s_data.arbeidssted_geografi_id as kafka_geo_id, geo_klass.fylkesnavn as kafka_fylke,
    s_data.stilling_styrk_id as kafka_styrk_id, yrkesklass.bransje_grovgruppetekst as kafka_bransje_grov, yrkesklass.bransje_fingruppetekst as kafka_branje_fin,
    s_data.stilling_kilde_id as kafka_kilde
from datamart2.arb_still_testtabell_drp202012 s_data
     left join dimensjoner2.dim_styrk yrkesklass
     on s_data.stilling_styrk_id = yrkesklass.styrk_id
     left join dimensjoner2.dim_geografi geo_klass
     on s_data.arbeidssted_geografi_id = geo_klass.geografi_id"
df_kafka = setDT(DBI::dbGetQuery(con, query_s))
setnames(df_kafka, tolower(names(df_kafka)))
df_kafka <- unique(df_kafka)

#dato må være typen timestamp
statmnd = setDT(dbGetQuery(con, "select aarmnd, 
                          STATMND_STARTDATO as m_start,
                          STATMND_SLUTTDATO+1 as m_slutt, 
                          STATMND_SLUTTDATO-91 as inaktiv_dato 
                          from dimensjoner2.dim_tid_mnd"))
setnames(statmnd,tolower(names(statmnd)))
armnd <- 202009
statmnd = statmnd[aarmnd == armnd]
statmnd$m_slutt

#df_kakfa_sept <- df_kafka[(kafka_timestamp >= statmnd$m_start & kafka_timestamp < statmnd$m_slutt) & (expires >= statmnd$m_start)] #& (expires > updated), (created >= statmnd$m_start) 
df_kakfa_sept <- 
    df_kafka[(kafka_timestamp >= statmnd$m_start & kafka_timestamp < statmnd$m_slutt) 
             & (expires >= statmnd$m_start) & (published_by_admin >= statmnd$m_start)]
setorder(df_kakfa_sept, uuid, updated) 
df_kakfa_sept <- df_kakfa_sept[status =='ACTIVE']
df_kakfa_sept[stillinger == 0,stillinger_stat := 1]
df_kakfa_sept <- df_kakfa_sept[!duplicated(uuid)]
sum(df_kakfa_sept$stillinger_stat) #37673



sept_compare <- merge(df_kakfa_sept, df_ainfo[,.(uuid, ainfo_tilgang, ainfo_bransje_grov, ainfo_fylke)], by = 'uuid', all.x = TRUE, all.y = TRUE)




sept_compare[,merge_indikator := '']
sept_compare[stillinger_stat == ainfo_tilgang, merge_indikator := 'teller likt']
sept_compare[stillinger_stat != ainfo_tilgang, merge_indikator := 'teller forskjellig']
sept_compare[is.na(kafka_fylke),merge_indikator := 'kun_ainfo']
sept_compare[is.na(ainfo_fylke), merge_indikator := 'kun_kafka']

table(sept_compare$merge_indikator)

v <- sept_compare[,.(uuid,merge_indikator,stillinger, stillinger_stat, ainfo_tilgang,kafka_bransje_grov, ainfo_bransje_grov, kafka_fylke, ainfo_fylke)]
View(v[merge_indikator == 'kun_ainfo'])

sum(sept_compare[])
sum(is.na(df_ainfo$uuid))
nomatch_ainfo <- df_ainfo[is.na(uuid)] #93 rader i ainfo som ikke finnes i topic

#alternativ telling! Vil ha 38000 for sept
df_kakfa_sept <- 
    df_kafka[(kafka_timestamp >= statmnd$m_start & kafka_timestamp < statmnd$m_slutt) 
             & (expires >= statmnd$m_start)
             & (published_by_admin >= statmnd$m_start)] #& (expires > updated), (created >= statmnd$m_start) 
df_kakfa_sept[stillinger == 0,stillinger_stat := 1]

setorder(df_kakfa_sept, uuid, -stillinger_stat) 


#df_kakfa_sept[,status_num := ifelse(status == 'ACTIVE', 1, 0)]
#df_kakfa_sept[,diff := diff(status_num), by = uuid]

#(f['diff'].isnull()) & (f['status'] == 'ACTIVE') & (f['publishedByAdmin'] >= s_m['m_start'])

df_kakfa_sept <- df_kakfa_sept[!duplicated(uuid)]
sum(df_kakfa_sept$stillinger_stat) #35722



ikt_sept <- df_kakfa_sept[status=='ACTIVE' & kafka_bransje_grov == 'Ingeni?r- og ikt-fag']
df_kakfa_sept[,sum(stillinger_stat), by = .(kafka_bransje_grov)]

head(df_kakfa_sept)

sum(df_kakfa_sept$stillinger_stat)

