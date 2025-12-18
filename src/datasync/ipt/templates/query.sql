select *, {{ core }}.{{ core.id }} as fid, coalesce({% if 'footprintWKT' in columns %}ST_GeomFromText(footprintWKT), {% endif %}ST_POINT(decimalLongitude, decimalLatitude)) as geom, 4326 as srid
from read_csv('{{core.path}}') as {{ core }}
{% for ext in extensions -%}
    join read_csv('{{ext.path}}', sample_size=-1) as {{ ext }} on {{ ext }}.{{ ext.id }}={{ core }}.{{ core.id }}
{% endfor %}
