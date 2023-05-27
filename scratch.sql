SELECT t.trip_id 
FROM trip_t
INNER JOIN breadcrumb b
WHERE b.latitude > 45.506022
  AND b.latitude < 45.516636
  AND b.longitude > -122.711662
  AND b.longitude < -122.700316