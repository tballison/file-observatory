select f.id, f.fetched_digest, f.fetched_length, s.status
from cc_fetch f
join cc_fetch_status s on f.status_id=s.id