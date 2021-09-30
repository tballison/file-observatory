select u.id,
u.url as url,
's3://safedocs-cc-202109/'||p.path as fname,
p.path as relpath,
fetched_digest as shasum_256,
'CC-MAIN-2021-31' as collection,
fetched_length as size,
case
	when latitude is null
	then ''
	else latitude||','||longitude
end as host_location,
h.tld, h.country,
pinfo.stderr pinfo_stderr,
pinfo.stdout pinfo_stdout,
pinfo.exit_value pinfo_exit,
case
    when pinfo.stderr like 'Command Line Error: Incorrect password%' then 'encrypted'
    when pinfo.path is null then 'missing'
	when pinfo.timeout=true then 'timeout'
	when pinfo.exit_value <> 0 then 'crash'
	when length(pinfo.stderr) > 5 then 'warn'
	else 'success'
end as pinfo_status,
q.stderr q_stderr,
q.exit_value q_exit,
case
    when t.stderr like 'org.apache.tika.exception.EncryptedDocumentException%' then 'encrypted'
    when t.path is null then 'missing'
	when t.timeout=true then 'timeout'
	when t.exit_value <> 0 then 'crash'
	when length(t.stderr) > 5 then 'warn'
	else 'success'
end as tk_status,
t.exit_value tk_exit,
t.stderr tk_stderr,
case
    when q.path is null then 'missing'
	when q.timeout=true then 'timeout'
	when q.exit_value <> 0 then 'crash'
	when length(q.stderr) > 5 then 'warn'
	else 'success'
end as q_status,
ptt.stderr ptt_stderr,
ptt.exit_value ptt_exit,
case
    when ptt.stderr like 'Incorrect password%' then 'encrypted'
    when ptt.path is null then 'missing'
	when ptt.timeout=true then 'timeout'
	when ptt.exit_value <> 0 then 'crash'
	when length(ptt.stderr) > 5 then 'warn'
	else 'success'
end as ptt_status,
mt.stderr mt_warn,
mt.stdout mt_stdout,
mt.exit_value mt_exit,
case
    when mt.stderr like 'error: cannot authenticate password%' then 'encrypted'
    when mt.path is null then 'missing'
	when mt.timeout=true then 'timeout'
	when mt.exit_value <> 0 then 'crash'
	when length(mt.stderr) > 5 then 'warn'
	else 'success'
end as mt_status,
mcomp.num_tokens_0,
mcomp.oov_0,
mcomp.detected_lang_0,
mcomp.num_tokens_1,
mcomp.oov_1,
mcomp.detected_lang_1,
mcomp.num_tokens_2,
mcomp.oov_2,
mcomp.detected_lang_2,
mcomp.overlap_tool_0_v_1,
mcomp.overlap_tool_0_v_2,
mcomp.overlap_tool_1_v_2
from profiles p
join cc_fetch f on p.path = f.path
join cc_fetch_status s on f.status_id=s.id
join cc_urls u on f.id=u.id
join cc_hosts h on u.host=h.id
join pdfinfo pinfo on pinfo.path=p.path
join qpdf q on q.path = p.path
left join tika t on t.path = p.path
join pdftotext ptt on ptt.path = p.path
join mutooltext mt on mt.path = p.path
left join multi_comparisons mcomp on mcomp.path = p.path

--where fetched_length is not null and latitude is not null
limit 100000