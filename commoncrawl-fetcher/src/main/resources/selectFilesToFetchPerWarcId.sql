select u.id,
digest as cc_index_digest,
w.name as warc_file_name,
warc_offset, warc_length,
t.name as cc_truncated
from cc_urls u
join cc_warc_file_name w on u.warc_file_name = w.id
join cc_truncated t on u.truncated = t.id
left join cc_fetch f on f.id = u.id
where f.id is null and u.status = 200
order by w.id, warc_offset