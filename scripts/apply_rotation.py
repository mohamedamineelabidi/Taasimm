import re
import os
import math # make it easy
config_path = os.path.join('producers', 'config.py')
with open(config_path, 'r', encoding='utf-8') as f:
    content = f.read()

content = content.replace('import numpy as np', 'import numpy as np\nimport math')

new_transform = '''def transform_to_casablanca(lat, lon):
    """Transform a Porto GPS coordinate to Casablanca space + noise."""
    casa_lat = transform_coord(lat, PORTO_LAT_MIN, PORTO_LAT_MAX,
                               CASA_LAT_MIN, CASA_LAT_MAX)
    casa_lon = transform_coord(lon, PORTO_LON_MIN, PORTO_LON_MAX,
                               CASA_LON_MIN, CASA_LON_MAX)
    
    # 2. Add -45 deg clockwise rotation around center
    angle = math.radians(-45.0)
    cos_t, sin_t = math.cos(angle), math.sin(angle)
    cx = (CASA_LAT_MIN + CASA_LAT_MAX) / 2.0
    cy = (CASA_LON_MIN + CASA_LON_MAX) / 2.0

    rot_lat = cos_t * (casa_lat - cx) - sin_t * (casa_lon - cy) + cx
    rot_lon = sin_t * (casa_lat - cx) + cos_t * (casa_lon - cy) + cy
    
    # 3. Add noise
    rot_lat += np.random.normal(0, NOISE_SIGMA)
    rot_lon += np.random.normal(0, NOISE_SIGMA)
    
    return float(rot_lat), float(rot_lon)'''

# Regex sub
content = re.sub(r'def transform_to_casablanca\(lat, lon\):.*?(?=\ndef is_in_porto_metro)', new_transform + '\n\n', content, flags=re.DOTALL)

with open(config_path, 'w', encoding='utf-8') as f:
    f.write(content)
