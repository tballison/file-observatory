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
    when q.path is null then 'missing'
	when q.timeout=true then 'timeout'
	when q.exit_value <> 0 then 'crash'
	when length(q.stderr) > 5 then 'warn'
	else 'success'
end as q_status
from profiles p
join cc_fetch f on p.path = f.path
join cc_fetch_status s on f.status_id=s.id
join cc_urls u on f.id=u.id
join cc_hosts h on u.host=h.id
join pdfinfo pinfo on pinfo.path=p.path
join qpdf q on q.path = p.path
limit 10