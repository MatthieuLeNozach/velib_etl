
---
### **Existing Tables**

**Locations (idx=stationcode, static table):**
- stationcode VARCHAR(255) PRIMARY KEY,
- name VARCHAR(255),
- latitude FLOAT,
- longitude FLOAT,
- nom_arrondissement_communes VARCHAR(255)

**Station (main table):**
- id SERIAL PRIMARY KEY,
- record_timestamp VARCHAR(255),
- stationcode VARCHAR(255),
- ebike INTEGER,
- mechanical INTEGER,
- duedate VARCHAR(255),
- numbikesavailable INTEGER,
- numdocksavailable INTEGER,
- capacity INTEGER,
- is_renting VARCHAR(255),
- is_installed VARCHAR(255),
- is_returning VARCHAR(255),
- FOREIGN KEY (stationcode) REFERENCES locations(stationcode)

---

### **External Tables**
**Weather (idx=timestamp):**
- stationcode
- temperature
- factual weather
- rain(mm)
- pollution score

### **Enrichment Tables**

**Grid zoning (static table):**
- stationcode
- x coordinates: cols ABCDEDF
- y coordinates: rows 12456 
- etc


### **Views:**

**useful_facts**:
- Northernmost station
- Southernmost station
- Easternmost station
- Westernmost station
- Biggest number of docks per station
- Smallest number of docks per station
- Shortest distance from the shortest station
- Longest distance from the shortest station

**timestamps**: 
- total of timestamps
- mean timedelta between timestamps
- how many ts per hour / day / week etc



**Cities (idx=arrondissement_...)**:
- number of stations
- number of docks
- mean distance between shortest stations


**Paris vs banlieue (idx=timestamp)**:
- paris
- banlieue

**station_traffic (idx=timestamp)**:
- stationcode
- total of available docks per timestamp
- variation of it 1h / 3h / 6h / 12h / 24h (absolute)
- variation of it 1h / 3h / 6h / 12h / 24h (share % over total num of docks)


- total of ebike per timestamp
- variation of it 1h / 3h / 6h / 12h / 24h (share % over total num of docks)



**Station records idx=stationcode)**:
- biggest difference of docks available over 2mn(1 timestamp) / 5mn / 10 mn / 30mn etc
- longest period of time in bike shortage
- longest period of time in available dock shortage



**Global traffic (idx=timestamp)**
- total of available docks per timestamp
- variation of it 1h / 3h / 6h / 12h / 24h

- total of ebike per timestamp
- variation of it 1h / 3h / 6h / 12h / 24h

minutely:
temp
wind speed
weather main, 
weather description
humidity
uvi
rain