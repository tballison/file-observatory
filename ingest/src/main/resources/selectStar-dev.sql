select u.id,
's3://safedocs-cc-202109/'||path as fname,
path as relpath,
fetched_digest as shasum_256,
'CC-MAIN-2021-31' as collection,
fetched_length as size,
latitude||','||longitude as host_location
from cc_fetch f
join cc_fetch_status s on f.status_id=s.id
join cc_urls u on f.id=u.id
join cc_hosts h on u.host=h.id
where fetched_length is not null and latitude is not null

