--full query of the useful information gathered
--from the indices
select u.id, url,
digest as cc_index_digest,
f.fetched_digest,
u.status as http_status,
m.name as mime,
dm.name as detected_mime,
t.name as truncated,
w.name as warc_file_name,
warc_offset, warc_length,
l.name as languages,
f.fetched_length,
s.status as fetched_status
from cc_urls u
join cc_warc_file_name w on u.warc_file_name = w.id
join cc_mimes m on u.mime = m.id
join cc_detected_mimes dm on u.detected_mime=dm.id
join cc_truncated t on u.truncated = t.id
join cc_languages l on u.languages = l.id
left join cc_fetch f on f.id=u.id
left join cc_fetch_status s on f.status_id=s.id
where u.status = 200 and length(t.name) = 0
order by w.name, warc_offset