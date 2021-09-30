select u.id as id,
'CC-MAIN-2021-31-sample' as collection,
case
    when m.name is null or length(m.name) = 0
        then 'UNKNOWN'
    else m.name
end as detected_mime,
case
   when latitude is not null
	then latitude||','||longitude
	else ''
end as host_location,
h.tld,
case
	when h.country is not null
	then h.country
	else 'UNKNOWN'
end as country
from sample.cc_urls u
join sample.cc_hosts h on u.host=h.id
join sample.cc_detected_mimes m on u.detected_mime=m.id
