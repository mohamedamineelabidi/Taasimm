"""Generate clean Grafana dashboard JSON."""
import json

ds = {"type": "hadesarchitect-cassandra-datasource", "uid": "cassandra-ds"}

def zone_targets(query_tpl, limit):
    refs = "ABCDEFGHIJKLMNOP"
    targets = []
    for i in range(16):
        targets.append({
            "refId": refs[i],
            "datasource": ds,
            "queryType": "query",
            "rawQuery": True,
            "target": query_tpl.format(zone_id=i+1, limit=limit)
        })
    return targets

dashboard = {
    "title": "TaaSim - Vehicle Tracking",
    "uid": "taasim-vehicles",
    "timezone": "utc",
    "refresh": "10s",
    "time": {"from": "now-1h", "to": "now"},
    "panels": [
        # Panel 1: Vehicle Positions Map
        {
            "id": 1,
            "title": "Vehicle Positions (Live)",
            "type": "geomap",
            "gridPos": {"h": 16, "w": 16, "x": 0, "y": 0},
            "datasource": ds,
            "targets": zone_targets(
                "SELECT taxi_id, lat, lon, zone_id, speed, status, event_time "
                "FROM taasim.vehicle_positions "
                "WHERE city='casablanca' AND zone_id={zone_id} LIMIT {limit}", 15),
            "transformations": [{"id": "merge", "options": {}}],
            "options": {
                "view": {"id": "coords", "lat": 33.57, "lon": -7.59, "zoom": 12},
                "controls": {"showZoom": True, "mouseWheelZoom": True, "showAttribution": True},
                "basemap": {"type": "default", "name": "OpenStreetMap"},
                "layers": [{
                    "type": "markers", "name": "Vehicles",
                    "config": {
                        "showLegend": True,
                        "style": {
                            "size": {"fixed": 10},
                            "color": {"field": "zone_id", "fixed": "green"},
                            "symbol": {"fixed": "circle"},
                            "opacity": 0.85,
                            "text": {"field": "taxi_id", "fixed": ""},
                            "textConfig": {"fontSize": 8, "offsetX": 0, "offsetY": -12}
                        }
                    },
                    "location": {"mode": "coords", "latitude": "lat", "longitude": "lon"}
                }]
            },
            "fieldConfig": {"defaults": {}, "overrides": []}
        },
        # Panel 2: Vehicles per Zone bar chart
        {
            "id": 2,
            "title": "Vehicles per Zone",
            "type": "barchart",
            "gridPos": {"h": 16, "w": 8, "x": 16, "y": 0},
            "datasource": ds,
            "targets": zone_targets(
                "SELECT taxi_id, zone_id FROM taasim.vehicle_positions "
                "WHERE city='casablanca' AND zone_id={zone_id} LIMIT {limit}", 20),
            "fieldConfig": {"defaults": {"color": {"mode": "palette-classic"}}, "overrides": []},
            "options": {
                "orientation": "horizontal",
                "showValue": "auto",
                "stacking": "none",
                "groupWidth": 0.7,
                "barWidth": 0.97
            },
            "transformations": [
                {"id": "merge", "options": {}},
                {"id": "groupBy", "options": {"fields": {
                    "zone_id": {"operation": "groupby", "aggregations": []},
                    "taxi_id": {"operation": "aggregate", "aggregations": ["count"]}
                }}},
                {"id": "organize", "options": {"renameByName": {"taxi_id (count)": "vehicle_count"}}}
            ]
        },
        # Panel 8: Trip Requests Map (origins red, destinations blue)
        {
            "id": 8,
            "title": "Trip Requests \u2014 Origins (red) & Destinations (blue)",
            "type": "geomap",
            "gridPos": {"h": 16, "w": 24, "x": 0, "y": 16},
            "datasource": ds,
            "targets": [{
                "refId": "A", "datasource": ds, "queryType": "query", "rawQuery": True,
                "target": (
                    "SELECT trip_id, rider_id, origin_lat, origin_lon, dest_lat, dest_lon, "
                    "origin_zone, dest_zone, status, created_at "
                    "FROM taasim.trips WHERE city='casablanca' AND date_bucket='2026-04-19' LIMIT 100"
                )
            }],
            "options": {
                "view": {"id": "coords", "lat": 33.57, "lon": -7.59, "zoom": 12},
                "controls": {"showZoom": True, "mouseWheelZoom": True, "showAttribution": True},
                "basemap": {"type": "default", "name": "OpenStreetMap"},
                "layers": [
                    {
                        "type": "markers", "name": "Pickup (origin)",
                        "config": {
                            "showLegend": True,
                            "style": {
                                "size": {"fixed": 8},
                                "color": {"fixed": "red"},
                                "symbol": {"fixed": "circle"},
                                "opacity": 0.8,
                                "text": {"field": "origin_zone", "fixed": ""},
                                "textConfig": {"fontSize": 9, "offsetX": 0, "offsetY": -10}
                            }
                        },
                        "location": {"mode": "coords", "latitude": "origin_lat", "longitude": "origin_lon"}
                    },
                    {
                        "type": "markers", "name": "Dropoff (destination)",
                        "config": {
                            "showLegend": True,
                            "style": {
                                "size": {"fixed": 7},
                                "color": {"fixed": "blue"},
                                "symbol": {"fixed": "triangle"},
                                "opacity": 0.7
                            }
                        },
                        "location": {"mode": "coords", "latitude": "dest_lat", "longitude": "dest_lon"}
                    }
                ]
            },
            "fieldConfig": {"defaults": {}, "overrides": []}
        },
        # Panel 3: Recent GPS Events
        {
            "id": 3,
            "title": "Recent GPS Events",
            "type": "table",
            "gridPos": {"h": 10, "w": 24, "x": 0, "y": 32},
            "datasource": ds,
            "targets": zone_targets(
                "SELECT taxi_id, zone_id, lat, lon, speed, status, event_time "
                "FROM taasim.vehicle_positions "
                "WHERE city='casablanca' AND zone_id={zone_id} LIMIT {limit}", 5),
            "transformations": [
                {"id": "merge", "options": {}},
                {"id": "sortBy", "options": {"fields": {}, "sort": [{"field": "event_time", "desc": True}]}}
            ],
            "fieldConfig": {"defaults": {}, "overrides": []},
            "options": {"showHeader": True, "footer": {"show": True, "countRows": True, "reducer": ["count"]}}
        },
        # Panel 4: Demand vs Supply bar chart
        {
            "id": 4,
            "title": "Demand vs Supply by Zone",
            "type": "barchart",
            "gridPos": {"h": 10, "w": 12, "x": 0, "y": 42},
            "datasource": ds,
            "targets": zone_targets(
                "SELECT zone_id, active_vehicles, pending_requests "
                "FROM taasim.demand_zones "
                "WHERE city='casablanca' AND zone_id={zone_id} LIMIT {limit}", 1),
            "fieldConfig": {
                "defaults": {"color": {"mode": "palette-classic"}},
                "overrides": [
                    {"matcher": {"id": "byName", "options": "active_vehicles"},
                     "properties": [
                         {"id": "color", "value": {"fixedColor": "green", "mode": "fixed"}},
                         {"id": "displayName", "value": "Supply (vehicles)"}
                     ]},
                    {"matcher": {"id": "byName", "options": "pending_requests"},
                     "properties": [
                         {"id": "color", "value": {"fixedColor": "red", "mode": "fixed"}},
                         {"id": "displayName", "value": "Demand (requests)"}
                     ]}
                ]
            },
            "options": {
                "orientation": "horizontal",
                "groupWidth": 0.7,
                "barWidth": 0.8,
                "showValue": "auto",
                "stacking": "none",
                "legend": {"displayMode": "list", "placement": "bottom"}
            },
            "transformations": [
                {"id": "merge", "options": {}},
                {"id": "groupBy", "options": {"fields": {
                    "zone_id": {"operation": "groupby", "aggregations": []},
                    "active_vehicles": {"operation": "aggregate", "aggregations": ["sum"]},
                    "pending_requests": {"operation": "aggregate", "aggregations": ["sum"]}
                }}},
                {"id": "organize", "options": {"renameByName": {
                    "active_vehicles (sum)": "active_vehicles",
                    "pending_requests (sum)": "pending_requests"
                }}}
            ]
        },
        # Panel 5: Demand Zone Details
        {
            "id": 5,
            "title": "Demand Zone Details",
            "type": "table",
            "gridPos": {"h": 10, "w": 12, "x": 12, "y": 42},
            "datasource": ds,
            "targets": zone_targets(
                "SELECT zone_id, active_vehicles, pending_requests, ratio, window_start "
                "FROM taasim.demand_zones "
                "WHERE city='casablanca' AND zone_id={zone_id} LIMIT {limit}", 5),
            "transformations": [
                {"id": "merge", "options": {}},
                {"id": "sortBy", "options": {"fields": {}, "sort": [{"field": "window_start", "desc": True}]}}
            ],
            "fieldConfig": {"defaults": {}, "overrides": []},
            "options": {"showHeader": True, "footer": {"show": True, "countRows": True, "reducer": ["count"]}}
        },
        # Panel 6: Trip Matching Results
        {
            "id": 6,
            "title": "Trip Matching Results",
            "type": "table",
            "gridPos": {"h": 10, "w": 24, "x": 0, "y": 52},
            "datasource": ds,
            "targets": [
                {"refId": "A", "datasource": ds, "queryType": "query", "rawQuery": True,
                 "target": "SELECT trip_id, rider_id, taxi_id, status, origin_zone, dest_zone, "
                           "eta_seconds, fare, created_at "
                           "FROM taasim.trips WHERE city='casablanca' AND date_bucket='2026-04-19' LIMIT 50"},
                {"refId": "B", "datasource": ds, "queryType": "query", "rawQuery": True,
                 "target": "SELECT trip_id, rider_id, taxi_id, status, origin_zone, dest_zone, "
                           "eta_seconds, fare, created_at "
                           "FROM taasim.trips WHERE city='casablanca' AND date_bucket='2026-04-18' LIMIT 50"}
            ],
            "transformations": [
                {"id": "merge", "options": {}},
                {"id": "sortBy", "options": {"fields": {}, "sort": [{"field": "created_at", "desc": True}]}}
            ],
            "fieldConfig": {"defaults": {}, "overrides": [
                {"matcher": {"id": "byName", "options": "status"}, "properties": [
                    {"id": "mappings", "value": [
                        {"type": "value", "options": {
                            "matched": {"text": "MATCHED", "color": "green"},
                            "no_vehicle": {"text": "NO VEHICLE", "color": "red"}
                        }}
                    ]}
                ]}
            ]},
            "options": {"showHeader": True, "footer": {"show": True, "countRows": True, "reducer": ["count"]}}
        }
    ]
}

with open("config/grafana-dashboard.json", "w") as f:
    json.dump(dashboard, f, indent=2)
print(f"OK - {len(dashboard['panels'])} panels written")
