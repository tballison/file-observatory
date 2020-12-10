--limited query used to pull enough info back to
--extract the right files from common crawl's warc files
select u.id,
digest as cc_index_digest,
w.name as warc_file_name,
warc_offset, warc_length
from cc_urls u
join cc_warc_file_name w on u.warc_file_name = w.id
join cc_truncated t on u.truncated = t.id
left join cc_fetch f on f.id = u.id
where f.id is null and u.status = 200 and length(t.name) = 0
order by w.name, warc_offset